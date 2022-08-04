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

#include "vec/exec/scan/vscanner.h"

#include "vec/exec/scan/vscan_node.h"

namespace doris::vectorized {

VScanner::VScanner(RuntimeState* state, VScanNode* parent) : _state(state), _parent(parent) {
    _total_rf_num = _parent->runtime_filter_num();
}

Status VScanner::try_append_late_arrival_runtime_filter() {
    if (_applied_rf_num == _total_rf_num) {
        return Status::OK();
    }
    DCHECK(_applied_rf_num < _total_rf_num);

    int arrived_rf_num = 0;
    RETURN_IF_ERROR(_parent->try_append_late_arrival_runtime_filter(&arrived_rf_num));

    if (arrived_rf_num == _applied_rf_num) {
        // No newly arrived runtime filters, just return;
        return Status::OK();
    }

    // There are newly arrived runtime filters,
    // renew the vconjunct_ctx_ptr
    if (_vconjunct_ctx) {
        _discard_conjuncts();
    }
    // Notice that the number of runtiem filters may be larger than _applied_rf_num.
    // But it is ok because it will be updated at next time.
    RETURN_IF_ERROR(_parent->clone_vconjunct_ctx(&_vconjunct_ctx));
    _applied_rf_num = arrived_rf_num;
    return Status::OK();
}

} // namespace doris::vectorized
