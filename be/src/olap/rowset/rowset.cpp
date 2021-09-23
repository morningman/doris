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

#include "olap/rowset/rowset.h"

#include "olap/rowset_cache.h"
#include "util/time.h"
#include "util/stack_util.h"

namespace doris {

Rowset::Rowset(const TabletSchema* schema, std::string rowset_path, RowsetMetaSharedPtr rowset_meta)
        : _schema(schema),
          _rowset_path(std::move(rowset_path)),
          _rowset_meta(std::move(rowset_meta)),
          _rowset_state_machine(RowsetStateMachine()) {
    _is_pending = !_rowset_meta->has_version();
    if (_is_pending) {
        _is_cumulative = false;
    } else {
        Version version = _rowset_meta->version();
        _is_cumulative = version.first != version.second;
    }
}

OLAPStatus Rowset::load(bool use_cache, std::shared_ptr<MemTracker> parent) {
    // if the state is ROWSET_UNLOADING it means close() is called
    // and the rowset is already loaded, and the resource is not closed yet.
    if (_rowset_state_machine.rowset_state() == ROWSET_LOADED) {
        return OLAP_SUCCESS;
    }
    {
        // before lock, if rowset state is ROWSET_UNLOADING, maybe it is doing do_close in release
        std::lock_guard<std::mutex> load_lock(_lock);
        // after lock, if rowset state is ROWSET_UNLOADING, it is ok to return
        if (_rowset_state_machine.rowset_state() == ROWSET_UNLOADED) {
            // first do load, then change the state
            RETURN_NOT_OK(do_load(use_cache, parent));
            RETURN_NOT_OK(_rowset_state_machine.on_load());
        } else if (_rowset_state_machine.rowset_state() == ROWSET_CLOSED) {
            return OLAP_ERR_ROWSET_ALREADY_CLOSED;
        }
    }
    // load is done
    VLOG_CRITICAL << "rowset is loaded. " << rowset_id() << ", rowset version:" << rowset_meta()->version()
              << ", state from ROWSET_UNLOADED to ROWSET_LOADED. tablet id:"
              << _rowset_meta->tablet_id() << ",  " << get_stack_trace();
    return OLAP_SUCCESS;
}

OLAPStatus Rowset::unload() {
    if (_rowset_state_machine.rowset_state() == ROWSET_UNLOADED) {
        return OLAP_SUCCESS;
    }
    {
        std::lock_guard<std::mutex> load_lock(_lock);
        // after lock, if rowset state is ROWSET_UNLOADING, it is ok to return
        if (_rowset_state_machine.rowset_state() == ROWSET_LOADED) {
            // first do load, then change the state
            RETURN_NOT_OK(do_unload());
            RETURN_NOT_OK(_rowset_state_machine.on_unload());
        } else if (_rowset_state_machine.rowset_state() == ROWSET_CLOSED) {
            // just return ok, resource has already been release when close()
            return OLAP_SUCCESS;
        }
    }

    // unload is done
    VLOG_CRITICAL << "rowset is unloaded. " << rowset_id() << ", rowset version:" << rowset_meta()->version()
        << ", state from ROWSET_LOADED to ROWSET_UNLOADED. tablet id:"
        << _rowset_meta->tablet_id() << ", " << get_stack_trace();
    return OLAP_SUCCESS;
}

void Rowset::make_visible(Version version, VersionHash version_hash) {
    _is_pending = false;
    _rowset_meta->set_version(version);
    _rowset_meta->set_version_hash(version_hash);
    _rowset_meta->set_rowset_state(VISIBLE);
    // update create time to the visible time,
    // it's used to skip recently published version during compaction
    _rowset_meta->set_creation_time(UnixSeconds());

    if (_rowset_meta->has_delete_predicate()) {
        _rowset_meta->mutable_delete_predicate()->set_version(version.first);
        return;
    }
    make_visible_extra(version, version_hash);
}

} // namespace doris
