#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>
#include <memory>

namespace chfs
{
    template <typename Command>
    struct Entry
    {
        int term;
        Command cmd;
    };

    /**
     * RaftLog uses a BlockManager to manage the data.
     * RaftLog will persist the Raft log and metadata.
     * And you can use the interface provided by ChfsCommand, such as size, deserialize and serialize to implement the log persistency.
     */
    template <typename Command>
    class RaftLog
    {
    public:
        RaftLog()
        {
            bm_ = std::make_shared<BlockManager>("log");
            log_entries.push_back(Entry<Command>());
            log_block_id = 1;
            log_block_offset = 0;
        }
        RaftLog(std::shared_ptr<BlockManager> bm)
        {
            bm_ = bm;
            log_entries.push_back(Entry<Command>());
            log_block_id = 1;
            log_block_offset = 0;
        }
        RaftLog(const RaftLog &raft_log)
        {
            bm_ = raft_log.bm_;
            log_entries = raft_log.log_entries;
            log_block_id = raft_log.log_block_id;
            log_block_offset = raft_log.log_block_offset;
        }
        ~RaftLog()
        {
            bm_.reset();
        }

        /* Lab3: Your code here */

        /**
         * Notice that the first log index is 1 instead of 0.
         * We need to append an empty log entry to the logs at the very beginning.
         * And since the 'lastApplied' index starts from 0, the first empty log entry will never be applied to the state machine.
         */
        std::vector<Entry<Command>> log_entries;

        int last_log_index() const;
        int last_log_term() const;
        int prev_log_index() const;
        int prev_log_term() const;
        void append_log(int term, Command cmd);
        void erase_log(int index);
        void recover_from_disk();

    private:
        std::shared_ptr<BlockManager> bm_;
        mutable std::mutex mtx;
        /* Lab3: Your code here */

        /**
         * Log block id corresponds to the index of the log entry.
         */
        // std::vector<int> log_block_ids;
        int log_block_id;
        int log_block_offset;

        /**
         * 由于 log 只有 append 和 erase 两种操作，因此对 bm_ 的设计遵循以下原则
         * 1.记录下一次 persist 的 log_block_id 和 log_block_offset
         * 2. log_block_id 初始化为 1，log_block_offset 初始化为 0
         * - 3. block 0 用于存储 metadata，因此 log_block_id 从 1 开始
         * - 4. metedata 存储格式为 [log_block_id, log_block_offset]
         * 5.每个 log entry 的存储格式为 [term, cmd.value]
         * 6.每个 log entry 和 metadata 的大小为 8(int 4 + int 4) 字节
         * 7.每个 block 的大小为 4096 字节，存储的 log entry 数量为 512 个
         */

        void store_log(int term, Command cmd);
        void delete_log(int index);
    };

    /* Lab3: Your code here */

    /**
     * Get the index of the last log.
     * @return The index of the last log.
     */
    template <typename Command>
    int RaftLog<Command>::last_log_index() const
    {
        std::unique_lock<std::mutex> lock(mtx);
        return log_entries.size() - 1;
    }

    /**
     * Get the term of the last log.
     * @return The term of the last log.
     */
    template <typename Command>
    int RaftLog<Command>::last_log_term() const
    {
        std::unique_lock<std::mutex> lock(mtx);
        return log_entries[log_entries.size() - 1].term;
    }

    /**
     * Get the index of the previous log.
     * @return The index of the previous log.
     */
    template <typename Command>
    int RaftLog<Command>::prev_log_index() const
    {
        std::unique_lock<std::mutex> lock(mtx);
        return log_entries.size() - 2;
    }

    /**
     * Get the term of the previous log.
     * @return The term of the previous log.
     */
    template <typename Command>
    int RaftLog<Command>::prev_log_term() const
    {
        std::unique_lock<std::mutex> lock(mtx);
        if (log_entries.size() <= 1)
            return -1;
        else
            return log_entries[log_entries.size() - 2].term;
    }

    /**
     * Append a log to the RaftLog.
     * @param term The term of the log.
     * @param value The value of the log.
     */
    template <typename Command>
    void RaftLog<Command>::append_log(int term, Command cmd)
    {
        std::unique_lock<std::mutex> lock(mtx);
        log_entries.push_back(Entry<Command>{term, cmd});
        store_log(term, cmd);
    }

    /**
     * Erase a log starting from the given index.
     * @param index The first index of the log to be erased.
     */
    template <typename Command>
    void RaftLog<Command>::erase_log(int index)
    {
        std::unique_lock<std::mutex> lock(mtx);
        log_entries.erase(log_entries.begin() + index, log_entries.end());
        delete_log(index);
    }

    /**
     * Recover the RaftLog from the BlockManager.
     */
    template <typename Command>
    void RaftLog<Command>::recover_from_disk()
    {
        std::unique_lock<std::mutex> lock(mtx);

        int block_id = 1;
        int block_offset = 0;
        int term;
        Command cmd;
        usize total_storage_size = bm_->total_storage_sz();
        while (true)
        {
            if (block_id * bm_->block_size() > total_storage_size)
                break;
            bm_->read_partial_block(block_id, (u8 *)&term, block_offset * 8, 4);
            bm_->read_partial_block(block_id, (u8 *)&cmd.value, block_offset * 8 + 4, 4);
            if (term == 0)
                break;
            log_entries.push_back(Entry<Command>{term, cmd});
            block_offset++;
            if (block_offset == 512)
            {
                block_id++;
                block_offset = 0;
            }
        }

        std::cout << "log_entries.size() = " << log_entries.size() << std::endl;
    }

    /**
     * Store a log entry to the BlockManager.
     * @param term The term of the log.
     * @param value The value of the log.
     * @return The index of the log entry.
     */
    template <typename Command>
    void RaftLog<Command>::store_log(int term, Command cmd)
    {
        std::unique_lock<std::mutex> lock(mtx);

        bm_->write_partial_block(log_block_id, (u8 *)&term, log_block_offset * 8, 4);
        bm_->write_partial_block(log_block_id, (u8 *)&cmd.value, log_block_offset * 8 + 4, 4);
        log_block_offset++;

        if (log_block_offset == 512)
        {
            log_block_id++;
            log_block_offset = 0;
        }
    }

    /**
     * Delete a log entry from the BlockManager.
     * @param index The index of the log entry.
     */
    template <typename Command>
    void RaftLog<Command>::delete_log(int index)
    {
        std::unique_lock<std::mutex> lock(mtx);

        int delete_id = index / 512 + 1;
        int delete_offset = index % 512;
        int zero = 0;
        for (int i = index; i < log_entries.size(); i++)
        {
            bm_->write_partial_block(delete_id, (u8 *)&zero, delete_offset * 8, 4);
            bm_->write_partial_block(delete_id, (u8 *)&zero, delete_offset * 8 + 4, 4);
            delete_offset++;
            if (delete_offset == 512)
            {
                delete_id++;
                delete_offset = 0;
            }
        }
    }

} /* namespace chfs */
