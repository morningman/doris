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

#include "exec/olap_utils.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/function_filter.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {

class OlapScanRange;

namespace vectorized {

class NewOlapScanNode;

class NewOlapScanner : public VScanner {
public:
    NewOlapScanner(RuntimeState* state, NewOlapScanNode* parent, bool aggregation,
                   bool need_agg_finalize, const TPaloScanRange& scan_range, MemTracker* tracker);

    Status open(RuntimeState* state) override;

    Status get_block(RuntimeState* state, Block* block, bool* eos) override;

    Status close(RuntimeState* state) override;

public:
    Status prepare(const TPaloScanRange& scan_range, const std::vector<OlapScanRange*>& key_ranges,
                   const std::vector<TCondition>& filters,
                   const std::vector<std::pair<string, std::shared_ptr<IBloomFilterFuncBase>>>&
                           bloom_filters,
                   const std::vector<FunctionFilter>& function_filters);
};
} // namespace vectorized
} // namespace doris
