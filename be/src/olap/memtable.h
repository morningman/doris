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

#include <stddef.h>
#include <stdint.h>

#include <cstring>
#include <functional>
#include <memory>
#include <vector>

#include "common/status.h"
#include "olap/partial_update_info.h"
#include "olap/tablet_schema.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/arena.h"
#include "vec/common/custom_allocator.h"
#include "vec/core/block.h"

namespace doris {

class Schema;
class SlotDescriptor;
class TabletSchema;
class TupleDescriptor;
enum KeysType : int;

// Active: the memtable is currently used by writer to insert into blocks
// Write_finished: the memtable finished write blocks and in the queue waiting for flush
// FLUSH: the memtable is under flushing, write segment to disk.
enum MemType { ACTIVE = 0, WRITE_FINISHED = 1, FLUSH = 2 };

// row pos in _input_mutable_block
struct RowInBlock {
    size_t _row_pos;
    char* _agg_mem = nullptr;
    size_t* _agg_state_offset = nullptr;
    bool _has_init_agg;

    RowInBlock(size_t row) : _row_pos(row), _has_init_agg(false) {}

    void init_agg_places(char* agg_mem, size_t* agg_state_offset) {
        _has_init_agg = true;
        _agg_mem = agg_mem;
        _agg_state_offset = agg_state_offset;
    }

    char* agg_places(size_t offset) const { return _agg_mem + _agg_state_offset[offset]; }

    inline bool has_init_agg() const { return _has_init_agg; }

    inline void remove_init_agg() { _has_init_agg = false; }
};

class Tie {
public:
    class Iter {
    public:
        Iter(Tie& tie) : _tie(tie), _next(tie._begin + 1) {}
        size_t left() const { return _left; }
        size_t right() const { return _right; }

        // return false means no more ranges
        bool next() {
            if (_next >= _tie._end) {
                return false;
            }
            _next = _find(1, _next);
            if (_next >= _tie._end) {
                return false;
            }
            _left = _next - 1;
            _next = _find(0, _next);
            _right = _next;
            return true;
        }

    private:
        size_t _find(uint8_t value, size_t start) {
            if (start >= _tie._end) {
                return start;
            }
            size_t offset = start - _tie._begin;
            size_t size = _tie._end - start;
            void* p = std::memchr(_tie._bits.data() + offset, value, size);
            if (p == nullptr) {
                return _tie._end;
            }
            return static_cast<uint8_t*>(p) - _tie._bits.data() + _tie._begin;
        }

    private:
        Tie& _tie;
        size_t _left;
        size_t _right;
        size_t _next;
    };

public:
    Tie(size_t begin, size_t end) : _begin(begin), _end(end) {
        _bits = std::vector<uint8_t>(_end - _begin, 1);
    }
    uint8_t operator[](size_t i) const { return _bits[i - _begin]; }
    uint8_t& operator[](size_t i) { return _bits[i - _begin]; }
    Iter iter() { return Iter(*this); }

private:
    const size_t _begin;
    const size_t _end;
    std::vector<uint8_t> _bits;
};

class RowInBlockComparator {
public:
    RowInBlockComparator(std::shared_ptr<TabletSchema> tablet_schema)
            : _tablet_schema(tablet_schema) {}
    // call set_block before operator().
    // only first time insert block to create _input_mutable_block,
    // so can not Comparator of construct to set pblock
    void set_block(vectorized::MutableBlock* pblock) { _pblock = pblock; }
    int operator()(const RowInBlock* left, const RowInBlock* right) const;

private:
    std::shared_ptr<TabletSchema> _tablet_schema;
    vectorized::MutableBlock* _pblock = nullptr; //  corresponds to Memtable::_input_mutable_block
};

class MemTableStat {
public:
    MemTableStat& operator+=(const MemTableStat& stat) {
        raw_rows += stat.raw_rows;
        merged_rows += stat.merged_rows;
        sort_ns += stat.sort_ns;
        agg_ns += stat.agg_ns;
        put_into_output_ns += stat.put_into_output_ns;
        duration_ns += stat.duration_ns;
        sort_times += stat.sort_times;
        agg_times += stat.agg_times;

        return *this;
    }

    std::atomic<int64_t> raw_rows = 0;
    std::atomic<int64_t> merged_rows = 0;
    int64_t sort_ns = 0;
    int64_t agg_ns = 0;
    int64_t put_into_output_ns = 0;
    int64_t duration_ns = 0;
    std::atomic<int64_t> sort_times = 0;
    std::atomic<int64_t> agg_times = 0;
};

class MemTable {
public:
    MemTable(int64_t tablet_id, std::shared_ptr<TabletSchema> tablet_schema,
             const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
             bool enable_unique_key_mow, PartialUpdateInfo* partial_update_info,
             const std::shared_ptr<ResourceContext>& resource_ctx);
    ~MemTable();

    int64_t tablet_id() const { return _tablet_id; }
    size_t memory_usage() const { return _mem_tracker->consumption(); }
    size_t get_flush_reserve_memory_size() const;
    // insert tuple from (row_pos) to (row_pos+num_rows)
    Status insert(const vectorized::Block* block, const DorisVector<uint32_t>& row_idxs);

    void shrink_memtable_by_agg();

    bool need_flush() const;

    bool need_agg() const;

    Status to_block(std::unique_ptr<vectorized::Block>* res);

    bool empty() const { return _input_mutable_block.rows() == 0; }

    const MemTableStat& stat() { return _stat; }

    std::shared_ptr<ResourceContext> resource_ctx() { return _resource_ctx; }

    std::shared_ptr<MemTracker> mem_tracker() { return _mem_tracker; }

    void set_flush_success() { _is_flush_success = true; }

    MemType get_mem_type() { return _mem_type; }

    void update_mem_type(MemType memtype) { _mem_type = memtype; }

private:
    // for vectorized
    template <bool has_skip_bitmap_col>
    void _aggregate_two_row_in_block(vectorized::MutableBlock& mutable_block, RowInBlock* new_row,
                                     RowInBlock* row_in_skiplist);

    // Used to wrapped by to_block to do exception handle logic
    Status _to_block(std::unique_ptr<vectorized::Block>* res);

private:
    std::atomic<MemType> _mem_type;
    int64_t _tablet_id;
    bool _enable_unique_key_mow = false;
    bool _is_flush_success = false;
    UniqueKeyUpdateModePB _partial_update_mode {UniqueKeyUpdateModePB::UPSERT};
    const KeysType _keys_type;
    std::shared_ptr<TabletSchema> _tablet_schema;

    std::shared_ptr<RowInBlockComparator> _vec_row_comparator;

    std::shared_ptr<ResourceContext> _resource_ctx;

    std::shared_ptr<MemTracker> _mem_tracker;
    // Only the rows will be inserted into block can allocate memory from _arena.
    // In this way, we can make MemTable::memory_usage() to be more accurate, and eventually
    // reduce the number of segment files that are generated by current load
    std::unique_ptr<vectorized::Arena> _arena;

    void _init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                            const TupleDescriptor* tuple_desc);
    std::vector<int> _column_offset;

    // Number of rows inserted to this memtable.
    // This is not the rows in this memtable, because rows may be merged
    // in unique or aggregate key model.
    MemTableStat _stat;

    //for vectorized
    vectorized::MutableBlock _input_mutable_block;
    vectorized::MutableBlock _output_mutable_block;
    size_t _last_sorted_pos = 0;

    //return number of same keys
    size_t _sort();
    Status _sort_by_cluster_keys();
    void _sort_one_column(DorisVector<std::shared_ptr<RowInBlock>>& row_in_blocks, Tie& tie,
                          std::function<int(RowInBlock*, RowInBlock*)> cmp);
    template <bool is_final>
    void _finalize_one_row(RowInBlock* row, const vectorized::ColumnsWithTypeAndName& block_data,
                           int row_pos);
    template <bool is_final, bool has_skip_bitmap_col = false>
    void _aggregate();
    Status _put_into_output(vectorized::Block& in_block);
    bool _is_first_insertion;

    void _init_agg_functions(const vectorized::Block* block);
    std::vector<vectorized::AggregateFunctionPtr> _agg_functions;
    std::vector<size_t> _offsets_of_aggregate_states;
    size_t _total_size_of_aggregate_states;
    std::unique_ptr<DorisVector<std::shared_ptr<RowInBlock>>> _row_in_blocks;

    size_t _num_columns;
    int32_t _seq_col_idx_in_block = -1;
    int32_t _skip_bitmap_col_idx {-1};
    int32_t _seq_col_unique_id {-1};

    bool _is_partial_update_and_auto_inc = false;
}; // class MemTable

} // namespace doris
