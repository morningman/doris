// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/connector_credential_cache.h"

#include <fmt/format.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/Status_types.h>

#include "common/config.h"
#include "common/logging.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "util/client_cache.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"

namespace doris {

std::string CachedCredential::redacted_summary() const {
    return fmt::format(
            "CachedCredential{{secret=***REDACTED***, expires_at_ms={}, refresh_hint={}}}",
            expires_at_ms, refresh_hint.empty() ? "<none>" : "***REDACTED***");
}

namespace {

Status default_rpc(const TRefreshCredentialRequest& req, TRefreshCredentialResult* out) {
    auto* cluster_info = ExecEnv::GetInstance()->cluster_info();
    if (cluster_info == nullptr) {
        return Status::InternalError("cluster_info not initialized");
    }
    auto master_addr = cluster_info->master_fe_addr;
    return ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port, [&req, out](FrontendServiceConnection& client) {
                client->refreshCredential(*out, req);
            });
}

} // namespace

std::unique_ptr<ConnectorCredentialCache> ConnectorCredentialCache::create_default(
        std::string requestor_id) {
    return std::make_unique<ConnectorCredentialCache>(default_rpc, std::move(requestor_id));
}

ConnectorCredentialCache::ConnectorCredentialCache(RpcFn rpc_fn, std::string requestor_id)
        : _rpc_fn(std::move(rpc_fn)), _requestor_id(std::move(requestor_id)) {
    DORIS_CHECK(_rpc_fn != nullptr);
}

int64_t ConnectorCredentialCache::_now_ms() const {
    return _clock_ms ? _clock_ms() : UnixMillis();
}

Status ConnectorCredentialCache::get_or_refresh(const std::string& scheme, const std::string& ref,
                                                const std::string& scope, CachedCredential* out) {
    DORIS_CHECK(out != nullptr);
    Key key {scheme, ref, scope};

    {
        std::lock_guard<std::mutex> lk(_entries_mu);
        auto it = _entries.find(key);
        if (it != _entries.end() && !it->second.is_expired(_now_ms())) {
            *out = it->second;
            return Status::OK();
        }
    }

    // Single-flight: get or create a per-key slot, then serialize on its mutex.
    std::shared_ptr<InflightSlot> slot;
    {
        std::lock_guard<std::mutex> lk(_inflight_mu);
        auto it = _inflight.find(key);
        if (it == _inflight.end()) {
            slot = std::make_shared<InflightSlot>();
            _inflight.emplace(key, slot);
        } else {
            slot = it->second;
        }
    }

    std::lock_guard<bthread::Mutex> rlk(slot->mu);

    // Re-check after acquiring the slot lock: a previous waiter may have
    // already populated the cache.
    {
        std::lock_guard<std::mutex> lk(_entries_mu);
        auto it = _entries.find(key);
        if (it != _entries.end() && !it->second.is_expired(_now_ms())) {
            *out = it->second;
            // Cleanup inflight slot if we are the last holder.
            std::lock_guard<std::mutex> ilk(_inflight_mu);
            auto sit = _inflight.find(key);
            if (sit != _inflight.end() && sit->second.get() == slot.get()) {
                _inflight.erase(sit);
            }
            return Status::OK();
        }
    }

    Status st = _do_refresh(key, out);

    {
        std::lock_guard<std::mutex> ilk(_inflight_mu);
        auto sit = _inflight.find(key);
        if (sit != _inflight.end() && sit->second.get() == slot.get()) {
            _inflight.erase(sit);
        }
    }
    return st;
}

Status ConnectorCredentialCache::_do_refresh(const Key& key, CachedCredential* out) {
    TRefreshCredentialRequest req;
    req.__set_scheme(key.scheme);
    req.__set_ref(key.ref);
    req.__set_scope(key.scope);
    if (!_requestor_id.empty()) {
        req.__set_requestor_id(_requestor_id);
    }

    TRefreshCredentialResult resp;
    Status rpc_st = _rpc_fn(req, &resp);
    if (!rpc_st.ok()) {
        LOG(WARNING) << "refreshCredential rpc failed: scheme=" << key.scheme
                     << ", scope=" << key.scope << ", err=" << rpc_st.to_string();
        return rpc_st;
    }
    if (resp.status.status_code != TStatusCode::OK) {
        std::string msg =
                !resp.status.error_msgs.empty() ? resp.status.error_msgs[0] : "unknown FE error";
        LOG(WARNING) << "refreshCredential FE returned error: scheme=" << key.scheme
                     << ", scope=" << key.scope << ", msg=" << msg;
        return Status::InternalError("refreshCredential failed: {}", msg);
    }
    if (!resp.__isset.credential) {
        return Status::InternalError("refreshCredential ok but no credential");
    }

    CachedCredential cc;
    cc.secret = resp.credential.__isset.secret ? resp.credential.secret : std::string();
    cc.expires_at_ms = resp.credential.__isset.expires_at_ms ? resp.credential.expires_at_ms : 0;
    cc.refresh_hint =
            resp.credential.__isset.refresh_hint ? resp.credential.refresh_hint : std::string();

    {
        std::lock_guard<std::mutex> lk(_entries_mu);
        _entries[key] = cc;
    }
    *out = cc;

    // SECURITY: never log raw secret; only summary metadata.
    DORIS_CHECK(cc.redacted_summary().find(cc.secret) == std::string::npos || cc.secret.empty());
    VLOG_DEBUG << "refreshCredential ok: scheme=" << key.scheme << ", scope=" << key.scope << ", "
               << cc.redacted_summary();
    return Status::OK();
}

void ConnectorCredentialCache::invalidate(const std::string& scheme, const std::string& ref,
                                          const std::string& scope) {
    Key k {scheme, ref, scope};
    std::lock_guard<std::mutex> lk(_entries_mu);
    _entries.erase(k);
}

size_t ConnectorCredentialCache::size_for_test() const {
    std::lock_guard<std::mutex> lk(_entries_mu);
    return _entries.size();
}

} // namespace doris
