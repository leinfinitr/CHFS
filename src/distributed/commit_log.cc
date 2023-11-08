#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs
{
  /**
   * `CommitLog` part
   */
  // {Your code here}
  CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                       bool is_checkpoint_enabled)
      : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm)
  {
  }

  CommitLog::~CommitLog() {}

  // {Your code here}
  auto CommitLog::get_log_entry_num() -> usize
  {
    return bm_->log_map.size();
  }

  // {Your code here}
  auto CommitLog::append_log(txn_id_t txn_id,
                             std::vector<std::shared_ptr<BlockOperation>> ops)
      -> void
  {
    // log 的格式为 txn_id + ops.size() + ops
    auto log_size = sizeof(txn_id_t) + sizeof(usize) +
                    ops.size() * (sizeof(block_id_t) + DiskBlockSize);
    std::vector<u8> log_data(log_size);

    // 写入 txn_id 和 op_num
    usize offset = sizeof(txn_id_t);
    usize op_num = ops.size();
    memcpy(log_data.data(), &txn_id, sizeof(txn_id_t));
    memcpy(log_data.data() + offset, &op_num, sizeof(usize));

    // 写入每个 op
    offset += sizeof(usize);
    for (auto &op : ops)
    {
      memcpy(log_data.data() + offset, &op->block_id_, sizeof(block_id_t));
      offset += sizeof(block_id_t);
      memcpy(log_data.data() + offset, op->new_block_state_.data(),
             DiskBlockSize);
      offset += DiskBlockSize;
    }

    // 将 log 写入 log block
    auto log_block_num = log_size / DiskBlockSize;
    for (usize i = 0; i < log_block_num; i++)
    {
      memcpy(bm_->unsafe_get_block_ptr() + bm_->log_block_id * bm_->block_size() + DiskBlockSize * i, log_data.data() + DiskBlockSize * i, DiskBlockSize);
      bm_->sync(bm_->log_block_id + i);
    }
    if (log_size % DiskBlockSize != 0)
    {
      memcpy(bm_->unsafe_get_block_ptr() + bm_->log_block_id * bm_->block_size() + DiskBlockSize * log_block_num, log_data.data() + DiskBlockSize * log_block_num, log_size % DiskBlockSize);
      bm_->sync(bm_->log_block_id + log_block_num);
    }

    commit_log(txn_id);
  }

  // {Your code here}
  auto CommitLog::commit_log(txn_id_t txn_id) -> void
  {
    last_txn_id_ = txn_id;
  }

  // {Your code here}
  auto CommitLog::checkpoint() -> void
  {
    bm_->flush();
    // 清空 log
    bm_->log_map.clear();
  }

  // {Your code here}
  auto CommitLog::recover() -> void
  {
    // 读取所有的 log
    std::vector<u8> logs(bm_->log_block_cnt * DiskBlockSize);
    for (usize i = 0; i < bm_->log_block_cnt; i++)
    {
      memcpy(logs.data() + i * DiskBlockSize, bm_->unsafe_get_block_ptr() + bm_->log_block_id * bm_->block_size() + i * DiskBlockSize, DiskBlockSize);
    }

    // 逐个 log 进行恢复
    usize offset = 0;
    while (offset < logs.size())
    {
      // 读取 txn_id 和 op_num
      txn_id_t txn_id;
      usize op_num;
      memcpy(&txn_id, logs.data() + offset, sizeof(txn_id_t));
      offset += sizeof(txn_id_t);
      memcpy(&op_num, logs.data() + offset, sizeof(usize));
      offset += sizeof(usize);

      // 读取每个 op
      for (usize i = 0; i < op_num; i++)
      {
        block_id_t block_id;
        std::vector<u8> new_block_state(DiskBlockSize);
        memcpy(&block_id, logs.data() + offset, sizeof(block_id_t));
        offset += sizeof(block_id_t);
        memcpy(new_block_state.data(), logs.data() + offset, DiskBlockSize);
        offset += DiskBlockSize;

        // 恢复
        memcpy(bm_->unsafe_get_block_ptr() + block_id * bm_->block_size(), new_block_state.data(), DiskBlockSize);
        bm_->sync(block_id);
      }
    }

    // 恢复 log_map 中的 log
    for (auto &log : bm_->log_map)
    {
      memcpy(bm_->unsafe_get_block_ptr() + log.first * bm_->block_size(), log.second.data(), DiskBlockSize);
      bm_->sync(log.first);
    }
  }
}; // namespace chfs