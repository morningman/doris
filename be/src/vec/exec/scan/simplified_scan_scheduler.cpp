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

#include <memory>

#include "scanner_scheduler.h"
#include "vec/exec/scan/scanner_context.h"

namespace doris::vectorized {
class ScannerDelegate;
class ScanTask;

Status TaskExecutorSimplifiedScanScheduler::schedule_scan_task(
        std::shared_ptr<ScannerContext> scanner_ctx, std::shared_ptr<ScanTask> current_scan_task,
        std::unique_lock<std::mutex>& transfer_lock,
                                      RuntimeProfile::Counter* s1,
                                      RuntimeProfile::Counter* s2,
                                      RuntimeProfile::Counter* s3,
                                      RuntimeProfile::Counter* s4,
                                      RuntimeProfile::Counter* s5) {
    // SCOPED_TIMER(s1);
    MonotonicStopWatch watch;
    watch.start();
    std::unique_lock<std::shared_mutex> wl(_lock);
    LOG(INFO) << "yy debug lock time: " << watch.elapsed_time_microseconds();
    SCOPED_TIMER(s2);
    return scanner_ctx->schedule_scan_task(current_scan_task, transfer_lock, wl,s1,s2,s3,s4,s5);
}

Status ThreadPoolSimplifiedScanScheduler::schedule_scan_task(
        std::shared_ptr<ScannerContext> scanner_ctx, std::shared_ptr<ScanTask> current_scan_task,
        std::unique_lock<std::mutex>& transfer_lock,
                                      RuntimeProfile::Counter* s1,
                                      RuntimeProfile::Counter* s2,
                                      RuntimeProfile::Counter* s3,
                                      RuntimeProfile::Counter* s4,
                                      RuntimeProfile::Counter* s5) {
    // SCOPED_TIMER(s1);
    MonotonicStopWatch watch;
    watch.start();
    std::unique_lock<std::shared_mutex> wl(_lock);
    LOG(INFO) << "yy debug 2 lock time: " << watch.elapsed_time_microseconds();
    SCOPED_TIMER(s2);
    return scanner_ctx->schedule_scan_task(current_scan_task, transfer_lock, wl,s1,s2,s3,s4,s5);
}
} // namespace doris::vectorized
