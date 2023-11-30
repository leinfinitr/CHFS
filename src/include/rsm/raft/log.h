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
            log_entries.push_back(Entry<Command>());
        }
        RaftLog(std::shared_ptr<BlockManager> bm)
        {
            bm_ = bm;
            log_entries.push_back(Entry<Command>());
        }
        RaftLog(const RaftLog &raft_log)
        {
            this->bm_ = raft_log.bm_;
            this->log_entries = raft_log.log_entries;
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

        int last_log_index();
        int last_log_term();
        void append_log(int term, Command cmd);

    private:
        std::shared_ptr<BlockManager> bm_;
        std::mutex mtx;
        /* Lab3: Your code here */
    };

    /* Lab3: Your code here */

    /**
     * Get the index of the last log.
     * @return The index of the last log.
    */
    template <typename Command>
    int RaftLog<Command>::last_log_index()
    {
        std::unique_lock<std::mutex> lock(mtx);
        return log_entries.size() - 1;
    }

    /**
     * Get the term of the last log.
     * @return The term of the last log.
    */
    template <typename Command>
    int RaftLog<Command>::last_log_term()
    {
        std::unique_lock<std::mutex> lock(mtx);
        return log_entries[log_entries.size() - 1].term;
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
    }

} /* namespace chfs */
