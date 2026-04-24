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

#pragma once

#include <bthread/mutex.h>
#include <gen_cpp/FrontendService_types.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"

namespace doris {

// Credential payload returned by the FE refreshCredential RPC, cached on the
// BE for reuse by scan operators (M1-02).
//
// SECURITY: callers must NEVER log `secret`. Use `redacted_summary()` instead.
struct CachedCredential {
    std::string secret;
    int64_t expires_at_ms = 0; // 0 = no expiry
    std::string refresh_hint;

    bool is_expired(int64_t now_unix_ms) const {
        return expires_at_ms > 0 && now_unix_ms >= expires_at_ms;
    }

    // Redacted, log-safe summary. Exposes only metadata, never the secret.
    std::string redacted_summary() const;
};

// BE-side credential cache for scan/load operators.
//
// `get_or_refresh` returns a cached non-expired credential or, on miss /
// expiry, calls back to FE via `refreshCredential` RPC. Concurrent callers
// asking for the same key share a single in-flight RPC (single-flight) keyed
// on (scheme, ref, scope).
//
// The RPC client is injected as a `std::function` so unit tests can supply a
// fake without touching the network. The default constructor wires the real
// FrontendServiceClient via ExecEnv::frontend_client_cache.
class ConnectorCredentialCache {
public:
    using RpcFn = std::function<Status(const TRefreshCredentialRequest&,
                                       TRefreshCredentialResult* /*out*/)>;

    // Cache key: (scheme, ref, scope).
    struct Key {
        std::string scheme;
        std::string ref;
        std::string scope;

        bool operator==(const Key& other) const {
            return scheme == other.scheme && ref == other.ref && scope == other.scope;
        }
    };

    struct KeyHash {
        size_t operator()(const Key& k) const noexcept {
            // FNV-1a 64-bit, simple combine.
            auto h = std::hash<std::string> {};
            size_t r = h(k.scheme);
            r ^= h(k.ref) + 0x9e3779b97f4a7c15ULL + (r << 6) + (r >> 2);
            r ^= h(k.scope) + 0x9e3779b97f4a7c15ULL + (r << 6) + (r >> 2);
            return r;
        }
    };

    // Construct with the production RPC implementation (uses
    // ExecEnv::frontend_client_cache). The optional `requestor_id` is sent in
    // each request for FE-side audit logging.
    static std::unique_ptr<ConnectorCredentialCache> create_default(std::string requestor_id);

    // Construct with an injected RPC function. Used by tests and by
    // create_default.
    ConnectorCredentialCache(RpcFn rpc_fn, std::string requestor_id);

    ConnectorCredentialCache(const ConnectorCredentialCache&) = delete;
    ConnectorCredentialCache& operator=(const ConnectorCredentialCache&) = delete;

    // Cache hit (non-expired) -> copy into *out and return OK.
    // Cache miss / expired   -> single-flight RPC to FE, update cache, copy into *out.
    Status get_or_refresh(const std::string& scheme, const std::string& ref,
                          const std::string& scope, CachedCredential* out);

    // Drop the cached entry for (scheme, ref, scope). Used by tests and to
    // proactively invalidate after the FE signals revocation.
    void invalidate(const std::string& scheme, const std::string& ref, const std::string& scope);

    // Test helper: returns the current number of cached entries.
    size_t size_for_test() const;

    // Override the monotonic-time source for expiry tests. Production callers
    // should never invoke this.
    void set_clock_for_test(std::function<int64_t()> clock_ms) { _clock_ms = std::move(clock_ms); }

private:
    struct InflightSlot {
        bthread::Mutex mu;
    };

    Status _do_refresh(const Key& key, CachedCredential* out);

    int64_t _now_ms() const;

    RpcFn _rpc_fn;
    std::string _requestor_id;
    std::function<int64_t()> _clock_ms; // null -> use UnixMillis()

    mutable std::mutex _entries_mu;
    std::unordered_map<Key, CachedCredential, KeyHash> _entries;

    // Per-key in-flight slot for single-flight. The slot itself owns a mutex
    // that the resolving thread holds while talking to FE; concurrent callers
    // for the same key serialize on that mutex and then re-check the cache.
    std::mutex _inflight_mu;
    std::unordered_map<Key, std::shared_ptr<InflightSlot>, KeyHash> _inflight;
};

} // namespace doris
