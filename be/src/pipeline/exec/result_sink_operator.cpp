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

#include "vec/sink/vresult_sink.h"
#include "result_sink_operator.h"
#include "runtime/buffer_control_block.h"

namespace doris::pipeline {
ResultSinkOperator::ResultSinkOperator(OperatorTemplate *operator_template, vectorized::VResultSink* sink) :
                                       Operator(operator_template), _sink(sink) {}

Status ResultSinkOperator::init(const TDataSink &tsink) {
    return _sink->init(tsink);
}

Status ResultSinkOperator::prepare(RuntimeState *state) {
    return _sink->prepare(state);
}

Status ResultSinkOperator::open(RuntimeState *state) {
    return _sink->open(state);
}

bool ResultSinkOperator::can_write() {
    return _sink->_sender->can_sink();
}

Status ResultSinkOperator::sink(RuntimeState *state, vectorized::Block *block, bool eos) {
    if (!block) {
        LOG(INFO) << "block is null, eos should invoke in finalize.";
        DCHECK(eos);
        return Status::OK();
    }
    return _sink->send(state, block);
}

Status  ResultSinkOperator::finalize(RuntimeState* state) {
    _finalized = true;
    return _sink->close(state, Status::OK());
}

Status  ResultSinkOperator::close(RuntimeState* state) {
    if (!_finalized) {
        RETURN_IF_ERROR(_sink->close(state, Status::InternalError("Not finalized")));
    }
    return Status::OK();
}
}