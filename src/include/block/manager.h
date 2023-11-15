//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// manager.h
//
// Identification: src/include/block/manager.h
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "common/result.h"
#include "librpc/client.h"

// 在此处不能 inclued distributed/commit_log.h，因为会造成循环依赖？
// 反正会编译不通过

namespace chfs
{
  // TODO

  class BlockIterator;

  /**
   * BlockManager implements a block device to read/write block devices
   * Note that the block manager is **not** thread-safe.
   */
  class BlockManager
  {
    friend class BlockIterator;
    friend class CommitLog;
    friend class MetadataServer;

  protected:
    const usize block_sz = 4096;

    std::string file_name_; // the file name of the block device
    int fd;                 // the file descriptor of the block device
    u8 *block_data;         // the block data pointer
    usize block_cnt;        // the number of blocks in the device
    bool in_memory;         // whether we use in-memory to emulate the block manager
    bool maybe_failed;
    usize write_fail_cnt;
    bool is_log_enabled;
    usize log_block_num;                              // the number of log blocks
    usize log_block_start;                            // the start block id of the log block
    usize log_block_cnt;                              // the number of writen log blocks
    txn_id_t last_txn_id;                             // the last txn id
    std::map<block_id_t, std::vector<u8>> log_buffer; // the log buffer

  public:
    /**
     * Creates a new block manager that writes to a file-backed block device.
     * @param block_file the file name of the  file to write to
     */
    explicit BlockManager(const std::string &file);

    /**
     * Creates a new block manager that writes to a file-backed block device.
     * @param block_file the file name of the  file to write to
     * @param block_cnt the number of expected blocks in the device. If the
     * device's blocks are more or less than it, the manager should adjust the
     * actual block cnt.
     */
    BlockManager(const std::string &file, usize block_cnt);

    /**
     * Creates a memory-backed block manager that writes to a memory block device.
     * Note that this is commonly used for testing.
     * Maybe it can be used for non-volatile memory, but who knows.
     *
     * @param block_count the number of blocks in the device
     * @param block_size the size of each block
     */
    BlockManager(usize block_count, usize block_size);

    /**
     * Creates a new block manager that writes to a file-backed block device.
     * It reserves some blocks for recording logs.
     *
     * @param block_file the file name of the  file to write to
     * @param block_cnt the number of blocks in the device
     * @param is_log_enabled whether to enable log
     */
    BlockManager(const std::string &file, usize block_cnt, bool is_log_enabled);

    virtual ~BlockManager();

    /**
     * Write a log block to the internal block device.
     * @param log_block_id id of the log block
     * @param log_block_data raw log block data
     */
    virtual auto write_log_block(const u8 *log_block_data) -> ChfsNullResult;

    /**
     * Write a partial log block to the internal block device.
     * @param log_block_id id of the log block
     * @param log_block_data raw log block data
     * @param offset the offset to write
     * @param len the length to write
     */
    virtual auto write_partial_log_block(const u8 *log_block_data, usize offset, usize len) -> ChfsNullResult;

    /**
     * Write a block to the internal block device.  This is a write-through one,
     * i.e., no cache.
     * @param block_id id of the block
     * @param block_data raw block data
     */
    virtual auto write_block(block_id_t block_id, const u8 *block_data)
        -> ChfsNullResult;

    /**
     * Write a partial block to the internal block device.
     */
    virtual auto write_partial_block(block_id_t block_id, const u8 *block_data,
                                     usize offset, usize len) -> ChfsNullResult;

    /**
     * Read a block to the internal block device.
     * @param block_id id of the block
     * @param block_data raw block data buffer to store the result
     */
    virtual auto read_block(block_id_t block_id, u8 *block_data)
        -> ChfsNullResult;

    /**
     * Clear the content of a block
     * @param block_id id of the block
     */
    virtual auto zero_block(block_id_t block_id) -> ChfsNullResult;

    auto total_storage_sz() const -> usize
    {
      return this->block_cnt * this->block_sz;
    }

    /**
     * Get the total number of blocks in the block manager
     */
    auto total_blocks() const -> usize { return this->block_cnt; }

    /**
     * Get the block size of the device managed by the manager
     */
    auto block_size() const -> usize { return this->block_sz; }

    /**
     * Get the block data pointer of the manager
     */
    auto unsafe_get_block_ptr() const -> u8 * { return this->block_data; }

    /**
     * flush the data of a block into disk
     */
    auto sync(block_id_t block_id) -> ChfsNullResult;

    /**
     * Flush the page cache
     */
    auto flush() -> ChfsNullResult;

    /**
     * Mark the block manager as may fail state
     */
    auto set_may_fail(bool may_fail) -> void
    {
      this->maybe_failed = may_fail;
    }
  };

  /**
   * A class to simplify iterating blocks in the block manager.
   *
   * Note that we don't provide a conventional iterator interface, because
   * each block read/write may return error due to failed reading/writing blocks.
   */
  class BlockIterator
  {
    BlockManager *bm;
    u64 cur_block_off;
    block_id_t start_block_id;
    block_id_t end_block_id;

    std::vector<u8> buffer;

  public:
    /**
     * Creates a new block iterator.
     *
     * @param bm the block manager to iterate
     * @param start_block_id the start block id of the iterator
     * @param end_block_id the end block id of the iterator
     */
    static auto create(BlockManager *bm, block_id_t start_block_id,
                       block_id_t end_block_id) -> ChfsResult<BlockIterator>;

    /**
     * Iterate to the cur_block_off to an offset
     *
     * **Assumption**: a previous call of has_next() returns true
     *
     * @param offset the offset to iterate to
     *
     * @return Ok(the iterator itself)
     *         Err(DONE) // the iteration is done, i.e., we have passed the
     * end_block_id Other errors
     */
    auto next(usize offset) -> ChfsNullResult;

    auto has_next() -> bool
    {
      return this->cur_block_off <
             (this->end_block_id - this->start_block_id) * bm->block_sz;
    }

    /**
     *  Assumption: a prior call of has_next() must return true
     */
    auto flush_cur_block() -> ChfsNullResult
    {
      auto target_block_id =
          this->start_block_id + this->cur_block_off / bm->block_sz;
      return this->bm->write_block(target_block_id, this->buffer.data());
    }

    auto get_cur_byte() const -> u8
    {
      return this->buffer[this->cur_block_off % bm->block_sz];
    }

    template <typename T>
    auto unsafe_get_value_ptr() -> T *
    {
      return reinterpret_cast<T *>(this->buffer.data() +
                                   this->cur_block_off % bm->block_sz);
    }
  };

} // namespace chfs
