#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>
#include <memory>

namespace chfs
{
    /**
     * RaftLog uses a BlockManager to manage the data.
     * RaftLog will persist the Raft log and metadata.
     * And you can use the interface provided by ChfsCommand, such as size, deserialize and serialize to implement the log persistency.
     */
    template <typename Command>
    class RaftLog
    {
    public:
        RaftLog(std::shared_ptr<BlockManager> bm, int last_log_index, int last_log_term, std::vector<u8> entries);
        RaftLog(const RaftLog &raft_log)
        {
            this->bm_ = raft_log.bm_;
            this->last_log_index_ = raft_log.last_log_index_;
            this->last_log_term_ = raft_log.last_log_term_;
            this->entries = raft_log.entries;
        }
        ~RaftLog();

        /* Lab3: Your code here */
        std::vector<u8> entries; // log entries to store

        int last_log_index() const { return last_log_index_; }
        int last_log_term() const { return last_log_term_; }

    private:
        std::shared_ptr<BlockManager> bm_;
        std::mutex mtx;
        /* Lab3: Your code here */
        int last_log_index_; // index of last log entry
        int last_log_term_;  // term of last log entry
    };

    template <typename Command>
    RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm, int last_log_index, int last_log_term, std::vector<u8> entries)
        : bm_(bm), last_log_index_(last_log_index), last_log_term_(last_log_term), entries(entries)
    {
        /* Lab3: Your code here */
    }

    template <typename Command>
    RaftLog<Command>::~RaftLog()
    {
        /* Lab3: Your code here */
    }

    /* Lab3: Your code here */

} /* namespace chfs */
