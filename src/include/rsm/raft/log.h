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

    struct Snapshot
    {
        int last_included_index;
        int last_included_term;
        int size;
        std::vector<u8> data;
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
            bm_ = std::make_shared<BlockManager>("/tmp/raft_log/log");
            snapshot_bm_ = std::make_shared<BlockManager>("/tmp/raft_log/snapshot");
            log_entries.push_back(Entry<Command>());
            log_block_id = 1;
            log_block_offset = 0;
            start_block_id = 1;
            start_block_offset = 0;
            last_included_index_ = 0;
            last_included_term_ = 0;
            snapshot_size = 0;
            bm_->write_partial_block(0, (u8 *)&log_block_id, 0, 4);
            bm_->write_partial_block(0, (u8 *)&log_block_offset, 4, 4);
            bm_->write_partial_block(0, (u8 *)&start_block_id, 8, 4);
            bm_->write_partial_block(0, (u8 *)&start_block_offset, 12, 4);
            snapshot_bm_->write_partial_block(0, (u8 *)&last_included_index_, 0, 4);
            snapshot_bm_->write_partial_block(0, (u8 *)&last_included_term_, 4, 4);
            snapshot_bm_->write_partial_block(0, (u8 *)&snapshot_size, 8, 4);
        }
        RaftLog(const std::string &file_path)
        {
            bm_ = std::make_shared<BlockManager>(file_path + "_log");
            snapshot_bm_ = std::make_shared<BlockManager>(file_path + "_snapshot");
        }
        RaftLog(const RaftLog &raft_log)
        {
            bm_ = raft_log.bm_;
            snapshot_bm_ = raft_log.snapshot_bm_;
            log_entries = raft_log.log_entries;
            log_block_id = raft_log.log_block_id;
            log_block_offset = raft_log.log_block_offset;
            start_block_id = raft_log.start_block_id;
            start_block_offset = raft_log.start_block_offset;
            last_included_index_ = raft_log.last_included_index_;
            last_included_term_ = raft_log.last_included_term_;
            snapshot_size = raft_log.snapshot_size;
        }
        ~RaftLog()
        {
            bm_.reset();
        }

        int last_log_index() const;
        int last_log_term() const;
        int prev_log_index() const;
        int prev_log_term() const;
        int get_term(int logical_index) const;
        Command get_command(int logical_index) const;
        std::vector<Entry<Command>> get_log_entries(int start_logical_index) const;
        void append_log(int term, Command cmd);
        void erase_log(int logical_index);
        void recover_from_disk(); // RaftLog 自身不能决定是否从磁盘恢复，需要在外部判断，因此该函数只能在外部调用

        void save_snapshot(int logical_index);
        void save_snapshot(Snapshot snapshot);
        // void save_snapshot(int last_included_index, std::vector<u8> data);
        void save_snapshot(int last_included_index, int last_included_term, std::vector<u8> data);
        int last_included_index();
        int last_included_term();
        Snapshot get_snapshot();

    private:
        /**
         * 由于 log 只有 append 和 erase 两种操作，因此对 bm_ 的设计遵循以下原则
         * 1.记录下一次要 persist 的 log_block_id 和 log_block_offset
         * 2. log_block_id 初始化为 1，log_block_offset 初始化为 0
         * 3. block 0 用于存储 metadata，因此 log_block_id 从 1 开始
         * 4. metedata 存储 log_block_id, log_block_offset, start_block_id, start_block_offset
         * 5.每个 log entry 的存储格式为 [term, cmd.value]
         * 6.每个 log entry 和 metadata 的大小为 8(int 4 + int 4) 字节
         * 7.每个 block 的大小为 4096 字节，存储的 log entry 数量为 512 个
         * 8. bm_ 中并不存储 log_entries 的第一个空值
         */
        std::shared_ptr<BlockManager> bm_; // 用于存储 log

        /**
         * 由于 BlockManager 本身并没有实现删除功能，因此对 snapshot_bm_ 的设计遵循以下原则
         * 1.将 snapshot 的数据存储到 snapshot_bm_ 中
         * 2. snapshot_bm_ 数据格式为 [last_included_index, last_included_term, snapshot_size, data]
         * 3.删除时更新 start_block_id 和 start_block_offset，而不是从 BlockManager 中删除
         * 4. snapshot_bm_ 中的所有数据均存储在硬盘中，恢复时只需要恢复 last_included_index_, last_included_term_ 和 snapshot_size 即可
         * 5. snapshot_bm_ 中的数据不包含 log_entries 的第一个空值
         * 6. snapshot_bm_ 中的数据是 serialized 的，因此需要在恢复时进行 deserialize
         */
        std::shared_ptr<BlockManager> snapshot_bm_; // 用于存储快照

        mutable std::mutex mtx;

        /**
         * Notice that the first log index is 1 instead of 0.
         * We need to append an empty log entry to the logs at the very beginning.
         * And since the 'lastApplied' index starts from 0, the first empty log entry will never be applied to the state machine.
         *
         * 已保存到 snapshot 中的 log_entry 将从 log_entries 中删除
         * 但是为了 log_index 的一致性，在处理 log_index 时需要加上 last_included_index
         */
        std::vector<Entry<Command>> log_entries;

        int log_block_id;         // 下一次要 persist 的 log_block_id
        int log_block_offset;     // 下一次要 persist 的 log_block_offset
        int start_block_id;       // log 的起始 block_id
        int start_block_offset;   // log 的起始 block_offset
        int last_included_index_; // 最后一个已保存到 snapshot 中的 log_entry 的 index
        int last_included_term_;  // 最后一个已保存到 snapshot 中的 log_entry 的 term
        int snapshot_size;        // snapshot 的大小

        void store_log(int term, Command cmd);
        void delete_log(int logical_index);
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
        // -1 是因为 log_entries 中的第一个元素是空值
        return log_entries.size() - 1 + last_included_index_;
    }

    /**
     * Get the term of the last log.
     * @return The term of the last log.
     */
    template <typename Command>
    int RaftLog<Command>::last_log_term() const
    {
        std::unique_lock<std::mutex> lock(mtx);
        // 如果 log_entries 不包含 log_entry 则返回 last_included_term
        if (log_entries.size() <= 1)
            return last_included_term_;
        else
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
        return log_entries.size() - 2 + last_included_index_;
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
            return last_included_term_;
        else
            return log_entries[log_entries.size() - 2].term;
    }

    /**
     * Get the term of the log at the given index.
     * @param logical_index The index of the log.
     * @return The term of the log.
     */
    template <typename Command>
    int RaftLog<Command>::get_term(int logical_index) const
    {
        std::unique_lock<std::mutex> lock(mtx);
        // 如果 log_entries 不包含 log_entry 则抛出异常
        if (logical_index < last_included_index_)
            throw std::runtime_error("logical_index <= last_included_index_ in get_term");
        else if (logical_index == last_included_index_)
            return last_included_term_;
        else
            return log_entries[logical_index - last_included_index_].term;
    }

    /**
     * Get the command of the log at the given index.
     * @param logical_index The index of the log.
     * @return The command of the log.
     */
    template <typename Command>
    Command RaftLog<Command>::get_command(int logical_index) const
    {
        std::unique_lock<std::mutex> lock(mtx);
        // 如果 log_entries 不包含 log_entry 则抛出异常
        if (logical_index <= last_included_index_)
            throw std::runtime_error("logical_index <= last_included_index_ in get_command");
        else
            return log_entries[logical_index - last_included_index_].cmd;
    }

    /**
     * Get the log entries starting from the given index.
     * @param start_logical_index The index of the first log entry.
     * @return The log entries.
     */
    template <typename Command>
    std::vector<Entry<Command>> RaftLog<Command>::get_log_entries(int start_logical_index) const
    {
        std::unique_lock<std::mutex> lock(mtx);
        // 如果 log_entries 不包含 log_entry 则抛出异常
        if (start_logical_index <= last_included_index_)
            throw std::runtime_error("start_logical_index <= last_included_index_ in get_log_entries");
        else
            return std::vector<Entry<Command>>(log_entries.begin() + start_logical_index - last_included_index_, log_entries.end());
    }

    /**
     * Append a log to the RaftLog.
     * @param term The term of the log.
     * @param value The value of the log.
     */
    template <typename Command>
    void RaftLog<Command>::append_log(int term, Command cmd)
    {
        mtx.lock();
        log_entries.push_back(Entry<Command>{term, cmd});
        mtx.unlock();

        store_log(term, cmd);
    }

    /**
     * Erase a log starting from the given index.
     * @param index The first index of the log to be erased.
     */
    template <typename Command>
    void RaftLog<Command>::erase_log(int logical_index)
    {
        mtx.lock();
        int physical_index = logical_index - last_included_index_;
        log_entries.erase(log_entries.begin() + physical_index, log_entries.end());
        mtx.unlock();

        delete_log(logical_index);
    }

    /**
     * Recover the RaftLog and Snapshot from the BlockManager.
     */
    template <typename Command>
    void RaftLog<Command>::recover_from_disk()
    {
        std::cout << "recover_from_disk" << std::endl;
        std::unique_lock<std::mutex> lock(mtx);

        // 恢复时的初始化
        log_entries.clear();
        log_entries.push_back(Entry<Command>());
        // 如果 log 为空则 log_block_id 和 log_block_offset 为 0
        bm_->read_partial_block(0, (u8 *)&log_block_id, 0, 4);
        bm_->read_partial_block(0, (u8 *)&log_block_offset, 4, 4);
        bm_->read_partial_block(0, (u8 *)&start_block_id, 8, 4);
        bm_->read_partial_block(0, (u8 *)&start_block_offset, 12, 4);
        snapshot_bm_->read_partial_block(0, (u8 *)&last_included_index_, 0, 4);
        snapshot_bm_->read_partial_block(0, (u8 *)&last_included_term_, 4, 4);
        snapshot_bm_->read_partial_block(0, (u8 *)&snapshot_size, 8, 4);
        std::cout << "log_block_id = " << log_block_id << " log_block_offset = " << log_block_offset << std::endl;
        std::cout << "start_block_id = " << start_block_id << " start_block_offset = " << start_block_offset << std::endl;
        std::cout << "last_included_index = " << last_included_index_ << " last_included_term = " << last_included_term_ << " snapshot_size = " << snapshot_size << std::endl;

        if (log_block_id == 0 && log_block_offset == 0)
        {
            // 当 log 为空时初始化 log
            std::cout << "log is empty, init log" << std::endl;
            log_block_id = 1;
            log_block_offset = 0;
            start_block_id = 1;
            start_block_offset = 0;
            bm_->write_partial_block(0, (u8 *)&log_block_id, 0, 4);
            bm_->write_partial_block(0, (u8 *)&log_block_offset, 4, 4);
            bm_->write_partial_block(0, (u8 *)&start_block_id, 8, 4);
            bm_->write_partial_block(0, (u8 *)&start_block_offset, 12, 4);
            return;
        }

        // 恢复 log
        int block_id = 1;
        int block_offset = 0; // Entry 的偏移量
        int term;
        Command cmd;
        while (block_id < log_block_id || (block_id == log_block_id && block_offset < log_block_offset))
        {
            bm_->read_partial_block(block_id, (u8 *)&term, block_offset * 8, 4);
            bm_->read_partial_block(block_id, (u8 *)&cmd.value, block_offset * 8 + 4, 4);
            log_entries.push_back(Entry<Command>{term, cmd});
            block_offset++;
            if (block_offset == 512)
            {
                block_id++;
                block_offset = 0;
            }
        }
    }

    /**
     * Save all the logs before the given index to the BlockManager.
     */
    template <typename Command>
    void RaftLog<Command>::save_snapshot(int logical_index)
    {
        mtx.lock();

        if (logical_index <= last_included_index_){
            mtx.unlock();
            return;
        }

        // Save snapshot file, discard any existing or partial snapshot with a smaller index
        int physical_index = logical_index - last_included_index_;
        int last_included_term = log_entries[physical_index].term;
        std::vector<u8> data;
        for (int i = 1; i <= physical_index; i++)
        {
            std::vector<u8> temp = log_entries[i].cmd.serialize(log_entries[i].cmd.size());
            data.insert(data.end(), temp.begin(), temp.end());
        }
        Snapshot snapshot{logical_index, last_included_term, int(data.size()), data};

        // Retain log entries following last_included_index and discard the log entries before last_included_index
        // 由于 BlockManager 本身并没有实现删除功能，因此通过修改 start_block_id 和 start_block_offset 来实现删除
        start_block_id = logical_index / 512 + 1;
        start_block_offset = logical_index % 512;

        // 丢弃 log_entries 中 last_included_index 之前的 log_entry
        log_entries.erase(log_entries.begin() + 1, log_entries.begin() + physical_index + 1);

        mtx.unlock();

        // std::cout << "save_snapshot logical_index = " << logical_index << " last_included_index_ = " << last_included_index_ << " last_included_term_ = " << last_included_term_ << std::endl;
        save_snapshot(snapshot);
    }

    /**
     * Save a snapshot to the BlockManager.
     * 此处的 save 为在已经保存的数据上进行修改，而不是覆盖
     * @param snapshot The snapshot to be saved.
     */
    template <typename Command>
    void RaftLog<Command>::save_snapshot(Snapshot snapshot)
    {
        std::unique_lock<std::mutex> lock(mtx);

        if (snapshot.last_included_index <= last_included_index_)
            return;

        int size = snapshot.size;                       // 需要写入的数据大小
        int block_id = (snapshot_size + 12) / 4096;     // 需要写入的 block_id
        int block_offset = (snapshot_size + 12) % 4096; // 需要写入的 block_offset
        while (size > 0)
        {
            int len = std::min(size, 4096 - block_offset);
            snapshot_bm_->write_partial_block(block_id, snapshot.data.data() + snapshot.data.size() - size, block_offset, len);
            size -= len;
            block_id++;
            block_offset = 0;
        }

        last_included_index_ = snapshot.last_included_index;
        last_included_term_ = snapshot.last_included_term;
        snapshot_size += snapshot.size;

        snapshot_bm_->write_partial_block(0, (u8 *)&snapshot.last_included_index, 0, 4);
        snapshot_bm_->write_partial_block(0, (u8 *)&snapshot.last_included_term, 4, 4);
        snapshot_bm_->write_partial_block(0, (u8 *)&snapshot_size, 8, 4);

        // std::cout << "finish save_snapshot last_included_index_ = " << last_included_index_ << " last_included_term_ = " << last_included_term_ << " snapshot_size = " << snapshot_size << std::endl;
    }

    /**
     * 在调用时需要保证 last_included_index < log_entries.size()
     * 丢弃 log_entries 中 last_included_index 之前的 log_entry
     * 而后将 data 中的数据写入到 snapshot_bm_ 中
     * @param last_included_index The index of the last log entry included in the snapshot.
     * @param data The data of the snapshot.
     */
    // template <typename Command>
    // void RaftLog<Command>::save_snapshot(int last_included_index, std::vector<u8> data)
    // {
    //     mtx.lock();
    //     if (last_included_index <= last_included_index_)
    //         return;

    //     // Save snapshot file, discard any existing or partial snapshot with a smaller index
    //     int physical_index = last_included_index - this->last_included_index_;
    //     int last_included_term = log_entries[physical_index].term;
    //     Snapshot snapshot{last_included_index, last_included_term, data};

    //     // Retain log entries following last_included_index and discard the log entries before last_included_index
    //     // 由于 BlockManager 本身并没有实现删除功能，因此通过修改 start_block_id 和 start_block_offset 来实现删除
    //     start_block_id = last_included_index / 512 + 1;
    //     start_block_offset = last_included_index % 512;

    //     // 丢弃 log_entries 中 last_included_index 之前的 log_entry
    //     log_entries.erase(log_entries.begin() + 1, log_entries.begin() + 1 + physical_index + 1);

    //     mtx.unlock();
    //     save_snapshot(snapshot);
    // }

    /**
     * Save a snapshot to the BlockManager.
     * @param last_included_index The index of the last log entry included in the snapshot.
     * @param last_included_term The term of the last log entry included in the snapshot.
     * @param data The data of the snapshot.
     */
    template <typename Command>
    void RaftLog<Command>::save_snapshot(int last_included_index, int last_included_term, std::vector<u8> data)
    {
        // Save snapshot file, discard any existing or partial snapshot with a smaller index
        // If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
        // Discard the entire log
        
        if (last_included_index <= last_included_index_)
            return;

        mtx.lock();
        // 删除 log_entries 中 last_included_index 之前的 log_entry
        // 如果 log_entries 中存在与 snapshot 的最后一个 log_entry 相同的 log_entry，则保留该 log_entry 之后的 log_entry
        // 否则将 log_entries 中的所有 log_entry 删除
        int physical_index = last_included_index - this->last_included_index_;
        if (log_entries.size() > physical_index && log_entries[physical_index].term == last_included_term)
        {
            log_entries.erase(log_entries.begin() + 1, log_entries.begin() + 1 + physical_index);
        }
        else
        {
            log_entries.clear();
            log_entries.push_back(Entry<Command>());
        }

        // 保存 snapshot
        last_included_index_ = last_included_index;
        last_included_term_ = last_included_term;
        snapshot_size = data.size();

        snapshot_bm_->write_partial_block(0, (u8 *)&last_included_index, 0, 4);
        snapshot_bm_->write_partial_block(0, (u8 *)&last_included_term, 4, 4);
        snapshot_bm_->write_partial_block(0, (u8 *)&snapshot_size, 8, 4);
        int size = data.size();
        int block_id = 0;
        int block_offset = 12;
        while (size > 0)
        {
            int len = std::min(size, 4096 - block_offset);
            snapshot_bm_->write_partial_block(block_id, data.data() + data.size() - size, block_offset, len);
            size -= len;
            block_id++;
            block_offset = 0;
        }

        start_block_id = last_included_index / 512 + 1;
        start_block_offset = last_included_index % 512;

        mtx.unlock();
    }

    /**
     * Get the index of the last log entry included in the snapshot.
     */
    template <typename Command>
    int RaftLog<Command>::last_included_index()
    {
        return last_included_index_;
    }

    /**
     * Get the term of the last log entry included in the snapshot.
     */
    template <typename Command>
    int RaftLog<Command>::last_included_term()
    {
        return last_included_term_;
    }

    /**
     * Get the data of the snapshot.
     */
    template <typename Command>
    Snapshot RaftLog<Command>::get_snapshot()
    {
        std::unique_lock<std::mutex> lock(mtx);

        std::vector<u8> data;
        int size = snapshot_size;
        int block_id = 0;
        int block_offset = 12;
        while (size > 0)
        {
            int len = std::min(size, 4096 - block_offset);
            std::vector<u8> buf(len);
            snapshot_bm_->read_partial_block(block_id, buf.data(), block_offset, len);
            data.insert(data.end(), buf.begin(), buf.end());
            size -= len;
            block_id++;
            block_offset = 0;
        }

        return Snapshot{last_included_index_, last_included_term_, snapshot_size, data};
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

        bm_->write_partial_block(0, (u8 *)&log_block_id, 0, 4);
        bm_->write_partial_block(0, (u8 *)&log_block_offset, 4, 4);
    }

    /**
     * Delete a log entry from the BlockManager.
     * @param logical_index The index of the log entry.
     */
    template <typename Command>
    void RaftLog<Command>::delete_log(int logical_index)
    {
        std::unique_lock<std::mutex> lock(mtx);

        int delete_id = logical_index / 512 + 1;
        int delete_offset = logical_index % 512;
        int zero = 0;
        for (int i = logical_index; i < log_entries.size(); i++)
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

        log_block_id = delete_id;
        log_block_offset = delete_offset;

        bm_->write_partial_block(0, (u8 *)&log_block_id, 0, 4);
        bm_->write_partial_block(0, (u8 *)&log_block_offset, 4, 4);
    }

} /* namespace chfs */
