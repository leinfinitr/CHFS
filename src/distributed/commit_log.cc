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
    log_entry_num = 0;
    last_txn_id_ = 0;
  }

  CommitLog::~CommitLog() {}

  // {Your code here}
  auto CommitLog::get_log_entry_num() -> usize
  {
    return log_entry_num;
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
    std::cout << "log size: " << log_size << std::endl;
    std::cout << "bm_->log_block_start: " << bm_->log_block_start << std::endl;
    std::cout << "bm_->block_ptr: " << bm_->unsafe_get_block_ptr() << std::endl;
    std::cout << "ops :" << ops.size() << std::endl;
    for(auto &op : ops)
    {
      std::cout << "op->block_id_: " << op->block_id_ << std::endl;
      std::cout << "op->new_block_state_.data(): " << op->new_block_state_.data() << std::endl;
    }
    std::cout << "log_data: " << log_data.data() << std::endl;

    auto log_block_num = log_size / DiskBlockSize;
    for (usize i = 0; i < log_block_num; i++)
    {
      memcpy(bm_->unsafe_get_block_ptr() + bm_->log_block_start * bm_->block_size() + DiskBlockSize * i, log_data.data() + DiskBlockSize * i, DiskBlockSize);
    }
    if (log_size % DiskBlockSize != 0)
    {
      memcpy(bm_->unsafe_get_block_ptr() + bm_->log_block_start * bm_->block_size() + DiskBlockSize * log_block_num, log_data.data() + DiskBlockSize * log_block_num, log_size % DiskBlockSize);
    }

    commit_log(txn_id);
  }

  // {Your code here}
  auto CommitLog::commit_log(txn_id_t txn_id) -> void
  {
    last_txn_id_ = txn_id;
    log_entry_num++;
    bm_->flush();
  }

  // {Your code here}
  auto CommitLog::checkpoint() -> void
  {
    bm_->flush();
    // 重置 log
    bm_->log_block_cnt = 0;
    bm_->last_txn_id = 0;
    bm_->log_buffer.clear();
    for(usize i = 0; i < bm_->log_block_num; i++)
    {
      bm_->zero_block(bm_->log_block_start + i);
    }
  }

  // {Your code here}
  auto CommitLog::recover() -> void
  {
    // 读取所有的 log
    std::vector<u8> logs(bm_->log_block_cnt * DiskBlockSize);
    for (usize i = 0; i < bm_->log_block_cnt; i++)
    {
      memcpy(logs.data() + i * DiskBlockSize, bm_->unsafe_get_block_ptr() + bm_->log_block_start * bm_->block_size() + i * DiskBlockSize, DiskBlockSize);
    }

    // 逐个 log 进行恢复
    usize offset = 0;
    for(usize log_cnt = 0; log_cnt < log_entry_num; log_cnt++)
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
  }
}; // namespace chfs