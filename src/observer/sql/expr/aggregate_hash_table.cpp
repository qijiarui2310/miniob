/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/expr/aggregate_hash_table.h"

// ----------------------------------StandardAggregateHashTable------------------

RC StandardAggregateHashTable::add_chunk(Chunk &groups_chunk, Chunk &aggrs_chunk)
{
  /** 1 2 3
    * 0 3 4
    * 1 3 5
   * group by c1
   *1
   *0
   *1
   * aggrs sum c2
   * 2
   * 3
   * 该函数需要根据group_chunk和aggr_chunk的值，将group_by_values和aggr_values聚合到hash_table中，主要是根据group_chunk的group的列，
   * 计算相应聚合列的聚合值，并将聚合结果存在hash_table的aggr_values中.
   */
  for (size_t i = 0; i < groups_chunk.rows(); i++) {
    //用于存储每一行group和aggregate值，即一行一行元素去执行aggregate操作
    //注意区分aggregate_val和aggr_values_,aggregate_val为chunk中的内容，而aggr_values_为hash_table中聚合后的值，这里只考虑sum的情况
    std::vector<Value> group_val, aggregate_val;
    for(size_t col_id = 0; col_id < groups_chunk.column_num(); col_id++){
      group_val.push_back(groups_chunk.get_value(col_id, i));
    }
    for(size_t col_id = 0; col_id < aggrs_chunk.column_num(); col_id++){
      aggregate_val.push_back(aggrs_chunk.get_value(col_id, i));
    }
    //下面执行聚合操作
    if(aggr_values_.count(group_val)!=0){
      for(size_t agg_id = 0; agg_id<aggrs_chunk.column_num(); agg_id++){
        if (aggr_values_[group_val].at(agg_id).attr_type() == AttrType::INTS) {
          auto old_value = aggr_values_[group_val].at(agg_id).get_int();
          aggr_values_[group_val].at(agg_id).set_int(old_value + aggregate_val.at(agg_id).get_int());
        } else if (aggr_values_[group_val].at(agg_id).attr_type() == AttrType::FLOATS) {
          auto old_value = aggr_values_[group_val].at(agg_id).get_float();
          aggr_values_[group_val].at(agg_id).set_float(old_value + aggregate_val.at(agg_id).get_float());
        } else {
          ASSERT(false, "not supported value type");
        }
      }
    }else{
      aggr_values_[group_val] = aggregate_val;
    }
  }

  return RC::SUCCESS;
}

void StandardAggregateHashTable::Scanner::open_scan()
{
  it_  = static_cast<StandardAggregateHashTable *>(hash_table_)->begin();
  end_ = static_cast<StandardAggregateHashTable *>(hash_table_)->end();
}

RC StandardAggregateHashTable::Scanner::next(Chunk &output_chunk)
{
  if (it_ == end_) {
    return RC::RECORD_EOF;
  }
  while (it_ != end_ && output_chunk.rows() <= output_chunk.capacity()) {
    auto &group_by_values = it_->first;
    auto &aggrs           = it_->second;
    for (int i = 0; i < output_chunk.column_num(); i++) {
      auto col_idx = output_chunk.column_ids(i);
      if (col_idx >= static_cast<int>(group_by_values.size())) {
        output_chunk.column(i).append_one((char *)aggrs[col_idx - group_by_values.size()].data());
      } else {
        output_chunk.column(i).append_one((char *)group_by_values[col_idx].data());
      }
    }
    it_++;
  }
  if (it_ == end_) {
    return RC::SUCCESS;
  }

  return RC::SUCCESS;
}

size_t StandardAggregateHashTable::VectorHash::operator()(const vector<Value> &vec) const
{
  size_t hash = 0;
  for (const auto &elem : vec) {
    hash ^= std::hash<string>()(elem.to_string());
  }
  return hash;
}

bool StandardAggregateHashTable::VectorEqual::operator()(const vector<Value> &lhs, const vector<Value> &rhs) const
{
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    if (rhs[i].compare(lhs[i]) != 0) {
      return false;
    }
  }
  return true;
}

// ----------------------------------LinearProbingAggregateHashTable------------------
#ifdef USE_SIMD
template <typename V>
RC LinearProbingAggregateHashTable<V>::add_chunk(Chunk &group_chunk, Chunk &aggr_chunk)
{
  if (group_chunk.column_num() != 1 || aggr_chunk.column_num() != 1) {
    LOG_WARN("group_chunk and aggr_chunk size must be 1.");
    return RC::INVALID_ARGUMENT;
  }
  if (group_chunk.rows() != aggr_chunk.rows()) {
    LOG_WARN("group_chunk and aggr _chunk rows must be equal.");
    return RC::INVALID_ARGUMENT;
  }
  add_batch((int *)group_chunk.column(0).data(), (V *)aggr_chunk.column(0).data(), group_chunk.rows());
  return RC::SUCCESS;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::Scanner::open_scan()
{
  capacity_   = static_cast<LinearProbingAggregateHashTable *>(hash_table_)->capacity();
  size_       = static_cast<LinearProbingAggregateHashTable *>(hash_table_)->size();
  scan_pos_   = 0;
  scan_count_ = 0;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::Scanner::next(Chunk &output_chunk)
{
  if (scan_pos_ >= capacity_ || scan_count_ >= size_) {
    return RC::RECORD_EOF;
  }
  auto linear_probing_hash_table = static_cast<LinearProbingAggregateHashTable *>(hash_table_);
  while (scan_pos_ < capacity_ && scan_count_ < size_ && output_chunk.rows() <= output_chunk.capacity()) {
    int key;
    V   value;
    RC  rc = linear_probing_hash_table->iter_get(scan_pos_, key, value);
    if (rc == RC::SUCCESS) {
      output_chunk.column(0).append_one((char *)&key);
      output_chunk.column(1).append_one((char *)&value);
      scan_count_++;
    }
    scan_pos_++;
  }
  return RC::SUCCESS;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::Scanner::close_scan()
{
  capacity_   = -1;
  size_       = -1;
  scan_pos_   = -1;
  scan_count_ = 0;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::get(int key, V &value)
{
  RC  rc          = RC::SUCCESS;
  int index       = (key % capacity_ + capacity_) % capacity_;
  int iterate_cnt = 0;
  while (true) {
    if (keys_[index] == EMPTY_KEY) {
      rc = RC::NOT_EXIST;
      break;
    } else if (keys_[index] == key) {
      value = values_[index];
      break;
    } else {
      index += 1;
      index %= capacity_;
      iterate_cnt++;
      if (iterate_cnt > capacity_) {
        rc = RC::NOT_EXIST;
        break;
      }
    }
  }
  return rc;
}

template <typename V>
RC LinearProbingAggregateHashTable<V>::iter_get(int pos, int &key, V &value)
{
  RC rc = RC::SUCCESS;
  if (keys_[pos] == LinearProbingAggregateHashTable<V>::EMPTY_KEY) {
    rc = RC::NOT_EXIST;
  } else {
    key   = keys_[pos];
    value = values_[pos];
  }
  return rc;
}

template <typename V>
void LinearProbingAggregateHashTable<V>::aggregate(V *value, V value_to_aggregate)
{
  if (aggregate_type_ == AggregateExpr::Type::SUM) {
    *value += value_to_aggregate;
  } else {
    ASSERT(false, "unsupported aggregate type");
  }
}

template <typename V>
void LinearProbingAggregateHashTable<V>::resize()
{
  capacity_ *= 2;
  std::vector<int> new_keys(capacity_);
  std::vector<V>   new_values(capacity_);

  for (size_t i = 0; i < keys_.size(); i++) {
    auto &key   = keys_[i];
    auto &value = values_[i];
    if (key != EMPTY_KEY) {
      int index = (key % capacity_ + capacity_) % capacity_;
      while (new_keys[index] != EMPTY_KEY) {
        index = (index + 1) % capacity_;
      }
      new_keys[index]   = key;
      new_values[index] = value;
    }
  }

  keys_   = std::move(new_keys);
  values_ = std::move(new_values);
}

template <typename V>
void LinearProbingAggregateHashTable<V>::resize_if_need()
{
  if (size_ >= capacity_ / 2) {
    resize();
  }
}
template <typename V>
void LinearProbingAggregateHashTable<V>::selective_load_slow_key(int* memory,int offset,int* val,int* inv)
{
  for (int i = 0; i < SIMD_WIDTH; i++) {
    if (inv[i] == -1) {
      val[i] = memory[offset++];
    }
  }
}

template <typename V>
void LinearProbingAggregateHashTable<V>::selective_load_slow_value(V* memory,int offset,V* val,int* inv)
{
  for (int i = 0; i < SIMD_WIDTH; i++) {
    if (inv[i] == -1) {
      val[i] = memory[offset++];
    }
  }
}

template <typename V>
vector<int> LinearProbingAggregateHashTable<V>::get_hash(vector<int> keys)
{
  vector<int> hash_value;
  for(int i=0;i<8;i++)
  {
    hash_value.push_back((keys[i]% capacity_ + capacity_) % capacity_);
  }
  return hash_value;
}
template <typename V>
void LinearProbingAggregateHashTable<V>::deal_lost(int key,V val)
{
  int index = (key % capacity_ + capacity_) % capacity_;
  // Linear probe
  while (keys_[index]!=EMPTY_KEY&&keys_[index]!=key)
  {
    index++;        /* code */
  }
  // Insert or update
  if (keys_[index] == key) {
    size_++;
    values_[index] += val;
  } else {
    keys_[index] = key;
    values_[index] = val;
  }
}
template <typename V>
void LinearProbingAggregateHashTable<V>::add_batch(int *input_keys, V *input_values, int len)
{
  // your code here
  // inv (invalid) 表示是否有效，inv[i] = -1 表示有效，inv[i] = 0 表示无效。
  // key[SIMD_WIDTH],value[SIMD_WIDTH] 表示当前循环中处理的键值对。
  // off (offset) 表示线性探测冲突时的偏移量，key[i] 每次遇到冲突键，则off[i]++，如果key[i] 已经完成聚合，则off[i] = 0，
  // i = 0 表示selective load 的起始位置。
  // inv 全部初始化为 -1
  // off 全部初始化为 0

  // for (; i + SIMD_WIDTH <= len;) {
    // 1: 根据 `inv` 变量的值，从 `input_keys` 中 `selective load` `SIMD_WIDTH` 个不同的输入键值对。
    // 2. 计算 i += |inv|, `|inv|` 表示 `inv` 中有效的个数 
    // 3. 计算 hash 值，
    // 4. 根据聚合类型（目前只支持 sum），在哈希表中更新聚合结果。如果本次循环，没有找到key[i] 在哈希表中的位置，则不更新聚合结果。
    // 5. gather 操作，根据 hash 值将 keys_ 的 gather 结果写入 table_key 中。
    // 6. 更新 inv 和 off。如果本次循环key[i] 聚合完成，则inv[i]=-1，表示该位置在下次循环中读取新的键值对。
    // 如果本次循环 key[i] 未在哈希表中聚合完成（table_key[i] != key[i]），则inv[i] = 0，表示该位置在下次循环中不需要读取新的键值对。
    // 如果本次循环中，key[i]聚合完成，则off[i] 更新为 0，表示线性探测偏移量为 0，key[i] 未完成聚合，则off[i]++,表示线性探测偏移量加 1。
  // }
  //7. 通过标量线性探测，处理剩余键值对

  // resize_if_need();
  int inv[8] = {-1,-1,-1,-1,-1,-1,-1,-1};
  int off[8] = {0,0,0,0,0,0,0,0};
  vector<int> key(SIMD_WIDTH,0);
  vector<V> value(SIMD_WIDTH,0);

  int i =0;
  while(i+SIMD_WIDTH<=len)
  {
    // 1: 根据 `inv` 变量的值，从 `input_keys` 中 `selective load` `SIMD_WIDTH` 个不同的输入键值对。
    selective_load_slow_key(input_keys,i,key.data(),inv);
    selective_load_slow_value(input_values,i,value.data(),inv);

    // 2. 计算 i += |inv|, `|inv|` 表示 `inv` 中有效的个数
    for (int j = 0; j < 8; j++)
    {
      i -= inv[j];
    }

    // 3. 计算 hash 值，
    vector<int> hash_value = get_hash(key);
    // 4. 根据聚合类型（目前只支持 sum），在哈希表中更新聚合结果。如果本次循环，没有找到key[i] 在哈希表中的位置，则不更新聚合结果。
    for(int j=0;j<8;j++)
    {
      // 6. 更新 inv 和 off。如果本次循环key[i] 聚合完成，则inv[i]=-1，表示该位置在下次循环中读取新的键值对。
      int index = hash_value[j];
      index += off[j];
      if(keys_[index] == EMPTY_KEY)
      {
        size_++;
        keys_[index] = key[j];
        values_[index] = value[j];
        off[j] = 0;
        inv[j] = -1;//需要更新
      }
      else if (keys_[index] != key[j])
      {
        off[j]++;
        inv[j] = 0;//不能更新
      }
      else
      {
        //进行聚合
        off[j] = 0;
        inv[j] = -1;
        values_[index] += value[j];
      }
    }
  }
  //7. 通过标量线性探测，处理剩余键值对
  //处理遗留在key和value中的键值对
  for(int j=0;j<8;j++)
  {
    if(inv[j]==0)
    {
      deal_lost(key[j],value[j]);
    }
  }


  for (; i < len; i++) {
    int key_it = input_keys[i];
    V value_it = input_values[i];
    int index = (key_it % capacity_ + capacity_) % capacity_;

    // Linear probe
    while (keys_[index]!=EMPTY_KEY&&keys_[index]!=key_it)
    {
      index++;        /* code */
    }
    // Insert or update
    if (keys_[index] == key_it) {
      values_[index] += value_it;
    } else {
      size_++;
      keys_[index] = key_it;
      values_[index] = value_it;
    }
  }
  resize_if_need();
}

template <typename V>
const int LinearProbingAggregateHashTable<V>::EMPTY_KEY = 0xffffffff;
template <typename V>
const int LinearProbingAggregateHashTable<V>::DEFAULT_CAPACITY = 16384;

template class LinearProbingAggregateHashTable<int>;
template class LinearProbingAggregateHashTable<float>;
#endif