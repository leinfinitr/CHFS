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
  CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                       bool is_checkpoint_enabled)
      : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm)
  {
    log_entry_num = 0;
    last_txn_id_ = 0;
  }

  CommitLog::~CommitLog() {}

  auto CommitLog::get_log_entry_num() -> usize
  {
    return log_entry_num;
  }

  auto CommitLog::append_log(txn_id_t txn_id,
                             std::vector<std::shared_ptr<BlockOperation>> ops)
      -> void
  {
    if (ops.size() == 0)
    {
      return;
    }

    // log 的格式为 txn_id + op_num + ops
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
      std::cout << "offset 1: " << offset << std::endl;
      memcpy(log_data.data() + offset, &op->block_id_, sizeof(block_id_t));
      offset += sizeof(block_id_t);
      std::cout << "offset 2: " << offset << std::endl;
      memcpy(log_data.data() + offset, op->new_block_state_.data(),
             DiskBlockSize);
      offset += DiskBlockSize;
      std::cout << "offset 3: " << offset << std::endl;
    }

    std::cout << "log size: " << log_size << "tnx_id: " << txn_id << std::endl;
    std::cout << "bm_->log_block_start: " << bm_->log_block_start << " bm_->log_block_offset: " << bm_->log_block_offset << std::endl;
    std::cout << "ops :" << ops.size() << std::endl;
    // for (auto &op : ops)
    // {
    //   std::cout << "op->block_id_: " << op->block_id_ << " " << op->new_block_state_.size() << std::endl;
    // }

    // 将 log 写入 log block
    auto log_block_num = log_size / DiskBlockSize;
    for (usize i = 0; i < log_block_num; i++)
    {
      bm_->write_log_block(log_data.data() + DiskBlockSize * i, DiskBlockSize);
    }
    if (log_size % DiskBlockSize != 0)
    {
      bm_->write_log_block(log_data.data() + DiskBlockSize * log_block_num, log_size % DiskBlockSize);
    }
    // bm_->write_log_block(log_data.data(), log_size);
    std::cout << "log_block_cnt: " << bm_->log_block_cnt << std::endl;

    commit_log(txn_id);
  }

  auto CommitLog::commit_log(txn_id_t txn_id) -> void
  {
    last_txn_id_ = txn_id;
    log_entry_num++;
    // 每 50 个 log 就进行一次 checkpoint
    if (log_entry_num % 50 == 0)
    {
      checkpoint();
    }
  }

  auto CommitLog::checkpoint() -> void
  {
    if (!is_checkpoint_enabled_)
    {
      return;
    }

    std::cout << "\ncheckpoint ..." << std::endl;
    // 读取所有的 log
    std::cout << "log_block_cnt: " << bm_->log_block_cnt << std::endl;
    std::vector<u8> logs(bm_->log_block_cnt * DiskBlockSize + bm_->log_block_offset);
    usize i = 0;
    for (; i < bm_->log_block_cnt; i++)
    {
      bm_->read_block(bm_->log_block_start + i, logs.data() + i * DiskBlockSize);
    }
    bm_->read_block(bm_->log_block_start + i, logs.data() + bm_->log_block_offset);

    // 对于每个 log，如果 txn_id 小于 last_txn_id_，则将其中所有 block 的更改落盘，而后删除该 log
    // 否则将该 log 保存之后重新写入 log block
    std::vector<u8> new_logs;
    usize new_log_entry_num = 0;
    usize offset = 0;
    std::cout << "log_entry_num: " << log_entry_num << std::endl;
    for (usize log_cnt = 0; log_cnt < log_entry_num; log_cnt++)
    {
      // 读取 txn_id 和 op_num
      txn_id_t txn_id;
      usize op_num;
      memcpy(&txn_id, logs.data() + offset, sizeof(txn_id_t));
      offset += sizeof(txn_id_t);
      memcpy(&op_num, logs.data() + offset, sizeof(usize));
      offset += sizeof(usize);
      std::cout << "tnx_id: " << txn_id << " op_num: " << op_num << std::endl;

      // 落盘后删除该 log 或者将该 log 保存至 new_logs
      if (txn_id < last_txn_id_)
      {
        // 读取每个 block_id
        for (usize i = 0; i < op_num; i++)
        {
          block_id_t block_id;
          memcpy(&block_id, logs.data() + offset, sizeof(block_id_t));
          offset += sizeof(block_id_t);
          offset += DiskBlockSize;
          bm_->sync(block_id);
        }
      }
      else
      {
        new_logs.insert(new_logs.end(), logs.begin() + offset - sizeof(txn_id_t) - sizeof(usize), logs.begin() + offset);
        new_log_entry_num++;
        for(usize i = 0; i < op_num; i++)
        {
          new_logs.insert(new_logs.end(), logs.begin() + offset, logs.begin() + offset + sizeof(block_id_t) + DiskBlockSize);
          offset += sizeof(block_id_t) + DiskBlockSize;
        }
      }
    }

    // 清空 log block
    bm_->clear_log_blocks();

    // 将 new_logs 写入 log block
    auto log_size = new_logs.size();
    auto log_block_num = log_size / DiskBlockSize;
    for (usize i = 0; i < log_block_num; i++)
    {
      bm_->write_log_block(new_logs.data() + DiskBlockSize * i, DiskBlockSize);
    }
    if (log_size % DiskBlockSize != 0)
    {
      bm_->write_log_block(new_logs.data() + DiskBlockSize * log_block_num, log_size % DiskBlockSize);
    }

    // 更新 log_entry_num
    log_entry_num = new_log_entry_num;

    std::cout << "checkpoint finish: new_log_entry_num: " << new_log_entry_num << std::endl;
  }

  auto CommitLog::recover() -> void
  {
    std::cout << "\nrecovering ..." << std::endl;
    // 读取所有的 log
    std::cout << "log_block_cnt: " << bm_->log_block_cnt << std::endl;
    std::vector<u8> logs(bm_->log_block_cnt * DiskBlockSize + bm_->log_block_offset);
    usize i = 0;
    for (; i < bm_->log_block_cnt; i++)
    {
      bm_->read_block(bm_->log_block_start + i, logs.data() + i * DiskBlockSize);
    }
    bm_->read_block(bm_->log_block_start + i, logs.data() + bm_->log_block_offset);

    // 逐个 log 进行恢复
    usize offset = 0;
    std::cout << "log_entry_num: " << log_entry_num << std::endl;
    for (usize log_cnt = 0; log_cnt < log_entry_num; log_cnt++)
    {
      // 读取 txn_id 和 op_num
      txn_id_t txn_id;
      usize op_num;
      memcpy(&txn_id, logs.data() + offset, sizeof(txn_id_t));
      offset += sizeof(txn_id_t);
      memcpy(&op_num, logs.data() + offset, sizeof(usize));
      offset += sizeof(usize);

      // 读取每个 op
      std::cout << "tnx_id: " << txn_id << " op_num: " << op_num << std::endl;
      for (usize i = 0; i < op_num; i++)
      {
        block_id_t block_id;
        std::vector<u8> new_block_state(DiskBlockSize);
        std::cout << "offset 1: " << offset << std::endl;
        memcpy(&block_id, logs.data() + offset, sizeof(block_id_t));
        offset += sizeof(block_id_t);
        std::cout << "offset 2: " << offset << std::endl;
        memcpy(new_block_state.data(), logs.data() + offset, DiskBlockSize);
        offset += DiskBlockSize;
        std::cout << "offset 3: " << offset << std::endl;

        std::cout << "block_id: " << block_id << std::endl;
        // 恢复
        bm_->write_block_direct(block_id, new_block_state.data());
        std::cout << "recover finish: " << block_id << std::endl;
      }

      // 每读完一个 tnx，就将 offset 补足为 DiskBlockSize 的倍数
      // offset = (offset + DiskBlockSize - 1) / DiskBlockSize * DiskBlockSize;
    }

    std::cout << "recover finish" << std::endl;
  }
}; // namespace chfs