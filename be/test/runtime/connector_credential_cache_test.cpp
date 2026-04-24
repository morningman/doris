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

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Status_types.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "common/status.h"

namespace doris {

namespace {

ConnectorCredentialCache::RpcFn make_ok_rpc(std::atomic<int>* call_count, std::string secret = "S",
                                            int64_t expires_at_ms = 0,
                                            std::chrono::milliseconds delay = {}) {
    return [call_count, secret = std::move(secret), expires_at_ms, delay](
                   const TRefreshCredentialRequest& req, TRefreshCredentialResult* out) -> Status {
        call_count->fetch_add(1);
        if (delay.count() > 0) {
            std::this_thread::sleep_for(delay);
        }
        TStatus st;
        st.status_code = TStatusCode::OK;
        out->__set_status(st);
        TConnectorCredential c;
        c.__set_scheme(req.scheme);
        c.__set_ref(req.ref);
        c.__set_secret(secret);
        if (expires_at_ms > 0) {
            c.__set_expires_at_ms(expires_at_ms);
        }
        out->__set_credential(c);
        return Status::OK();
    };
}

} // namespace

TEST(ConnectorCredentialCacheTest, MissThenHit) {
    std::atomic<int> calls {0};
    ConnectorCredentialCache cache(make_ok_rpc(&calls), "be-1");

    CachedCredential out;
    EXPECT_TRUE(cache.get_or_refresh("env", "env://A", "CATALOG", &out).ok());
    EXPECT_EQ("S", out.secret);
    EXPECT_EQ(1, calls.load());
    EXPECT_EQ(1U, cache.size_for_test());

    // Second call hits the cache: RPC must not fire.
    EXPECT_TRUE(cache.get_or_refresh("env", "env://A", "CATALOG", &out).ok());
    EXPECT_EQ("S", out.secret);
    EXPECT_EQ(1, calls.load());
}

TEST(ConnectorCredentialCacheTest, ExpiryTriggersRefresh) {
    std::atomic<int> calls {0};
    int64_t fake_now = 1000;
    ConnectorCredentialCache cache(make_ok_rpc(&calls, "S", /*expires_at_ms=*/2000), "be-1");
    cache.set_clock_for_test([&fake_now] { return fake_now; });

    CachedCredential out;
    EXPECT_TRUE(cache.get_or_refresh("env", "env://X", "TABLE", &out).ok());
    EXPECT_EQ(1, calls.load());

    fake_now = 1500; // not expired
    EXPECT_TRUE(cache.get_or_refresh("env", "env://X", "TABLE", &out).ok());
    EXPECT_EQ(1, calls.load());

    fake_now = 2000; // expired (>=)
    EXPECT_TRUE(cache.get_or_refresh("env", "env://X", "TABLE", &out).ok());
    EXPECT_EQ(2, calls.load());
}

TEST(ConnectorCredentialCacheTest, RpcFailurePropagates) {
    auto failing = [](const TRefreshCredentialRequest&, TRefreshCredentialResult*) -> Status {
        return Status::InternalError("boom");
    };
    ConnectorCredentialCache cache(failing, "be-1");

    CachedCredential out;
    Status st = cache.get_or_refresh("env", "env://Z", "CATALOG", &out);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(0U, cache.size_for_test());
}

TEST(ConnectorCredentialCacheTest, FeNonOkStatusFails) {
    auto fe_err = [](const TRefreshCredentialRequest&, TRefreshCredentialResult* out) -> Status {
        TStatus st;
        st.status_code = TStatusCode::INTERNAL_ERROR;
        st.error_msgs.push_back("fe error");
        out->__set_status(st);
        return Status::OK();
    };
    ConnectorCredentialCache cache(fe_err, "be-1");

    CachedCredential out;
    Status st = cache.get_or_refresh("env", "env://Z", "CATALOG", &out);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(0U, cache.size_for_test());
}

TEST(ConnectorCredentialCacheTest, SingleFlightUnderConcurrency) {
    std::atomic<int> calls {0};
    ConnectorCredentialCache cache(
            make_ok_rpc(&calls, "S", /*exp=*/0, std::chrono::milliseconds(50)), "be-1");

    constexpr int N = 8;
    std::vector<std::thread> ts;
    ts.reserve(N);
    std::atomic<int> ok_count {0};
    for (int i = 0; i < N; ++i) {
        ts.emplace_back([&] {
            CachedCredential out;
            Status st = cache.get_or_refresh("env", "env://Y", "CATALOG", &out);
            if (st.ok() && out.secret == "S") {
                ok_count.fetch_add(1);
            }
        });
    }
    for (auto& t : ts) {
        t.join();
    }
    EXPECT_EQ(N, ok_count.load());
    EXPECT_EQ(1, calls.load());
}

TEST(ConnectorCredentialCacheTest, InvalidateForcesRefresh) {
    std::atomic<int> calls {0};
    ConnectorCredentialCache cache(make_ok_rpc(&calls), "be-1");

    CachedCredential out;
    ASSERT_TRUE(cache.get_or_refresh("env", "env://A", "CATALOG", &out).ok());
    EXPECT_EQ(1, calls.load());

    cache.invalidate("env", "env://A", "CATALOG");
    ASSERT_TRUE(cache.get_or_refresh("env", "env://A", "CATALOG", &out).ok());
    EXPECT_EQ(2, calls.load());
}

TEST(ConnectorCredentialCacheTest, RedactedSummaryNeverContainsSecret) {
    CachedCredential c;
    c.secret = "TOP_SECRET_VALUE_XYZ";
    c.expires_at_ms = 12345;
    c.refresh_hint = "https://fe/refresh";
    std::string s = c.redacted_summary();
    EXPECT_EQ(std::string::npos, s.find("TOP_SECRET_VALUE_XYZ"));
    EXPECT_EQ(std::string::npos, s.find("https://fe/refresh"));
    EXPECT_NE(std::string::npos, s.find("REDACTED"));
}

} // namespace doris
