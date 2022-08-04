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

#include "vec/exec/scan/vscan_node.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {

Status VScanNode::init(const TPlanNode &tnode, RuntimeState *state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    _direct_conjunct_size = _conjunct_ctxs.size();
    _runtime_state = state;

    const TQueryOptions& query_options = state->query_options();
    if (query_options.__isset.max_scan_key_num) {
        _max_scan_key_num = query_options.max_scan_key_num;
    } else {
        _max_scan_key_num = config::doris_max_scan_key_num;
    }
    if (query_options.__isset.max_pushdown_conditions_per_column) {
        _max_pushdown_conditions_per_column = query_options.max_pushdown_conditions_per_column;
    } else {
        _max_pushdown_conditions_per_column = config::max_pushdown_conditions_per_column;
    }

    _max_scanner_queue_size_bytes = query_options.mem_limit / 20;

    RETURN_IF_ERROR(_register_runtime_filter());

    return Status::OK();
}

Status VScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    RETURN_IF_ERROR(_init_profile());
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    DCHECK(_tuple_desc != nullptr);

    // init profile for runtime filter
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        IRuntimeFilter* runtime_filter = nullptr;
        state->runtime_filter_mgr()->get_consume_filter(_runtime_filter_descs[i].filter_id,
                                                        &runtime_filter);
        DCHECK(runtime_filter != nullptr);
        runtime_filter->init_profile(_runtime_profile.get());
    }
    return Status::OK();
}

Status VScanNode::open(RuntimeState* state) {
    START_AND_SCOPE_SPAN(state->get_tracer(), span, "VScanNode::open");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    RETURN_IF_ERROR(_acquire_runtime_filter());

    RETURN_IF_ERROR(_process_conjuncts());
    RETURN_IF_ERROR(_init_scanners());
    RETURN_IF_ERROR(_start_scanners());
    return Status::OK();
}

Status VScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    return Status::OK();
}

Status VScanNode::_init_scanners() {

    return Status::OK();
}

Status VScanNode::_start_scanners() {
    return Status::OK();
}

Status VScanNode::_register_runtime_filter() {
    int filter_size = _runtime_filter_descs.size();
    _runtime_filter_ctxs.resize(filter_size);
    _runtime_filter_ready_flag.resize(filter_size);
    for (int i = 0; i < filter_size; ++i) {
        IRuntimeFilter* runtime_filter = nullptr;
        const auto& filter_desc = _runtime_filter_descs[i];
        RETURN_IF_ERROR(_runtime_state->runtime_filter_mgr()->regist_filter(
                RuntimeFilterRole::CONSUMER, filter_desc, _runtime_state->query_options(), id()));
        RETURN_IF_ERROR(_runtime_state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id,
                                                                        &runtime_filter));
        _runtime_filter_ctxs[i].runtime_filter = runtime_filter;
        _runtime_filter_ready_flag[i] = false;
        _rf_locks.push_back(std::make_unique<std::mutex>());
    }
    return Status::OK();
}

Status VScanNode::_acquire_runtime_filter() {
    _runtime_filter_ctxs.resize(_runtime_filter_descs.size());
    for (size_t i = 0; i < _runtime_filter_descs.size(); ++i) {
        auto& filter_desc = _runtime_filter_descs[i];
        IRuntimeFilter* runtime_filter = nullptr;
        _runtime_state->runtime_filter_mgr()->get_consume_filter(filter_desc.filter_id, &runtime_filter);
        DCHECK(runtime_filter != nullptr);
        if (runtime_filter == nullptr) {
            continue;
        }
        bool ready = runtime_filter->is_ready();
        if (!ready) {
            ready = runtime_filter->await();
        }
        if (ready) {
            std::list<ExprContext*> expr_context;
            RETURN_IF_ERROR(runtime_filter->get_push_expr_ctxs(&expr_context));
            _runtime_filter_ctxs[i].apply_mark = true;
            _runtime_filter_ctxs[i].runtimefilter = runtime_filter;
            for (auto ctx : expr_context) {
                ctx->prepare(_runtime_state, row_desc());
                ctx->open(_runtime_state);
                int index = _conjunct_ctxs.size();
                _conjunct_ctxs.push_back(ctx);
                _conjunctid_to_runtime_filter_ctxs[index] = &_runtime_filter_ctxs[i];
            }
        }
    }

    return Status::OK();
}

Status VScanNode::_process_conjuncts() {
    return Status::OK();
}

Status VScanNode::_init_scanners() {
    return Status::OK();
}

Status VScanNode::_start_scanners() {
    return Status::OK();
}

}
