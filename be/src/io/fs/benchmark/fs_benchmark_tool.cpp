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

#include <gflags/gflags.h>

#include <fstream>

#include "common/config.h"
#include "common/logging.h"
#include "gtest/gtest_pred_impl.h"
#include "http/ev_http_server.h"
#include "io/fs/benchmark/benchmark_factory.hpp"
#include "io/fs/s3_file_bufferpool.h"
#include "olap/page_cache.h"
#include "olap/segment_loader.h"
#include "olap/tablet_schema_cache.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "service/http_service.h"
#include "testutil/http_utils.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/mem_info.h"
#include "util/threadpool.h"

DEFINE_string(fs_type, "hdfs", "Supported File System: s3, hdfs");
DEFINE_string(operation, "create_write",
              "Supported Operations: create_write, open_read, open, rename, delete, exists");
DEFINE_string(threads, "1", "Number of threads");
DEFINE_string(iterations, "1", "Number of runs of each thread");
DEFINE_string(repetitions, "1", "Number of iterations");
DEFINE_string(file_size, "0", "File size for read/write opertions");
DEFINE_string(conf, "", "config file");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris BE benchmark tool for testing file system.\n";

    ss << "Usage:\n";
    ss << progname
       << " --fs_type=[fs_type] --operation=[op_type] --threads=[num] --iterations=[num] "
          "--repetitions=[num] "
          "--file_size=[num]\n";
    ss << "\nfs_type:\n";
    ss << "     hdfs\n";
    ss << "     s3\n";
    ss << "\nop_type:\n";
    ss << "     read\n";
    ss << "     write\n";
    ss << "\nthreads:\n";
    ss << "     num of threads\n";
    ss << "\niterations:\n";
    ss << "     Number of runs of each thread\n";
    ss << "\nrepetitions:\n";
    ss << "     Number of iterations\n";
    ss << "\nfile_size:\n";
    ss << "     File size for read/write opertions\n";
    ss << "\nExample:\n";
    ss << progname
       << " --conf my.conf --fs_type=hdfs --operation=create_write --threads=2 --iterations=100 "
          "--file_size=1048576\n";
    return ss.str();
}

int read_conf(const std::string& conf, std::map<std::string, std::string>* conf_map) {
    bool ok = true;
    std::ifstream fin(conf);
    if (fin.is_open()) {
        std::string line;
        while (getline(fin, line)) {
            if (line.empty() || line.rfind("#", 0) == 0) {
                // skip empty line and line starts with #
                continue;
            }
            size_t pos = line.find('=');
            if (pos != std::string::npos) {
                std::string key = line.substr(0, pos);
                std::string val = line.substr(pos + 1);
                (*conf_map)[key] = val;
            } else {
                std::cerr << "invalid config item: " << line << std::endl;
                ok = false;
                break;
            }
        }
        fin.close();

        std::cout << "read config from file \"" << conf << "\":\n";
        for (auto it = conf_map->begin(); it != conf_map->end(); it++) {
            std::cout << it->first << " = " << it->second << std::endl;
        }
    } else {
        std::cerr << "failed to open conf file: " << conf << std::endl;
        return 1;
    }
    return ok ? 0 : 1;
}

int main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    std::string conf_file = FLAGS_conf;
    std::map<std::string, std::string> conf_map;
    int res = read_conf(conf_file, &conf_map);
    if (res != 0) {
        std::cerr << "failed to read conf from file \"conf_file\"" << std::endl;
        return 1;
    }

    std::cout << "yy debug 1" << std::endl;
    doris::ThreadLocalHandle::create_thread_local_if_not_exits();
    std::cout << "yy debug 2" << std::endl;
    doris::ExecEnv::GetInstance()->init_mem_tracker();
    std::cout << "yy debug 3" << std::endl;
    doris::thread_context()->thread_mem_tracker_mgr->init();
    std::cout << "yy debug 4" << std::endl;
    std::shared_ptr<doris::MemTrackerLimiter> test_tracker =
            doris::MemTrackerLimiter::create_shared(doris::MemTrackerLimiter::Type::GLOBAL,
                                                    "BE-UT");
    std::cout << "yy debug 5" << std::endl;
    doris::thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(test_tracker);
    // doris::ExecEnv::GetInstance()->set_cache_manager(doris::CacheManager::create_global_instance());
    // doris::ExecEnv::GetInstance()->set_dummy_lru_cache(std::make_shared<doris::DummyLRUCache>());
    // doris::ExecEnv::GetInstance()->set_storage_page_cache(
    //         doris::StoragePageCache::create_global_cache(1 << 30, 10, 0));
    // doris::ExecEnv::GetInstance()->set_segment_loader(new doris::SegmentLoader(1000));
    std::cout << "yy debug 6" << std::endl;
    std::string beconf = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    std::cout << "yy debug 7" << std::endl;
    auto st = doris::config::init(beconf.c_str(), false);
    // doris::ExecEnv::GetInstance()->set_tablet_schema_cache(
    //         doris::TabletSchemaCache::create_global_schema_cache(
    //                 doris::config::tablet_schema_cache_capacity));
    LOG(INFO) << "init config " << st;

    std::cout << "yy debug 2" << std::endl;
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    doris::DiskInfo::init();
    doris::MemInfo::init();
    doris::BackendOptions::init();

    // auto service = std::make_unique<doris::HttpService>(doris::ExecEnv::GetInstance(), 0, 1);
    // service->register_debug_point_handler();
    // service->_ev_http_server->start();
    // doris::global_test_http_host = "http://127.0.0.1:" + std::to_string(service->get_real_port());

    // init s3 write buffer pool
    std::cout << "yy debug 3" << std::endl;
    int num_cores = doris::CpuInfo::num_cores();
    std::unique_ptr<doris::ThreadPool> s3_file_upload_thread_pool;
    doris::Status status = doris::ThreadPoolBuilder("S3FileUploadThreadPool")
                               .set_min_threads(num_cores)
                               .set_max_threads(num_cores)
                               .build(&s3_file_upload_thread_pool);
    if (!status.ok()) {
        std::cerr << "init s3 write buffer pool failed" << std::endl;
        return 1;
    }

    try {
        doris::io::MultiBenchmark multi_bm(FLAGS_fs_type, FLAGS_operation, std::stoi(FLAGS_threads),
                                           std::stoi(FLAGS_iterations), std::stol(FLAGS_file_size),
                                           conf_map);
        doris::Status st = multi_bm.init_env();
        if (!st) {
            std::cerr << "init env failed: " << st << std::endl;
            return 1;
        }
        st = multi_bm.init_bms();
        if (!st) {
            std::cerr << "init bms failed: " << st << std::endl;
            return 1;
        }

        benchmark::Initialize(&argc, argv);
        benchmark::RunSpecifiedBenchmarks();
        benchmark::Shutdown();

    } catch (std::invalid_argument const& ex) {
        std::cerr << "std::invalid_argument::what(): " << ex.what() << std::endl;
        return 1;
    } catch (std::out_of_range const& ex) {
        std::cerr << "std::out_of_range::what(): " << ex.what() << std::endl;
        return 1;
    }
    return 0;
}
