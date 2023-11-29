#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs
{

    const std::string RAFT_RPC_START_NODE = "start node";
    const std::string RAFT_RPC_STOP_NODE = "stop node";
    const std::string RAFT_RPC_NEW_COMMEND = "new commend";
    const std::string RAFT_RPC_CHECK_LEADER = "check leader";
    const std::string RAFT_RPC_IS_STOPPED = "check stopped";
    const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
    const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

    const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
    const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
    const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

    struct RequestVoteArgs
    {
        /* Lab3: Your code here */
        int term;           // candidate's term
        int candidate_id;   // candidate requesting vote
        int last_log_index; // index of candidate's last log entry
        int last_log_term;  // term of candidate's last log entry

        MSGPACK_DEFINE(
            term,
            candidate_id,
            last_log_index,
            last_log_term)
    };

    struct RequestVoteReply
    {
        /* Lab3: Your code here */
        int term;          // currentTerm, for candidate to update itself
        bool vote_granted; // true means candidate received vote

        MSGPACK_DEFINE(
            term,
            vote_granted)
    };

    template <typename Command>
    struct AppendEntriesArgs
    {
        /* Lab3: Your code here */
        int term;                              // leader's term
        int leader_id;                         // so follower can redirect clients
        int prev_log_index;                    // index of log entry immediately preceding new ones
        int prev_log_term;                     // term of prevLogIndex entry
        int leader_commit;                     // leader's commitIndex
        std::vector<RaftLog<Command>> entries; // log entries to store (empty for heartbeat; may send more than one for efficiency)
    };

    struct RpcAppendEntriesArgs
    {
        /* Lab3: Your code here */
        int term;                // leader's term
        int leader_id;           // so follower can redirect clients
        int prev_log_index;      // index of log entry immediately preceding new ones
        int prev_log_term;       // term of prevLogIndex entry
        int leader_commit;       // leader's commitIndex
        std::vector<u8> entries; // log entries to store (empty for heartbeat; may send more than one for efficiency)

        MSGPACK_DEFINE(
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            entries)
    };

    template <typename Command>
    RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
    {
        /* Lab3: Your code here */
        RpcAppendEntriesArgs rpc_arg;
        rpc_arg.term = arg.term;
        rpc_arg.leader_id = arg.leader_id;
        rpc_arg.prev_log_index = arg.prev_log_index;
        rpc_arg.prev_log_term = arg.prev_log_term;
        rpc_arg.leader_commit = arg.leader_commit;
        std::vector<u8> entries;
        for (const RaftLog<Command> &entry : arg.entries)
        {
            entries.insert(entries.end(), entry.entries.begin(), entry.entries.end());
        }
        rpc_arg.entries = entries;
        return rpc_arg;
    }

    template <typename Command>
    AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
    {
        /* Lab3: Your code here */
        AppendEntriesArgs<Command> arg;
        arg.term = rpc_arg.term;
        arg.leader_id = rpc_arg.leader_id;
        arg.prev_log_index = rpc_arg.prev_log_index;
        arg.prev_log_term = rpc_arg.prev_log_term;
        arg.leader_commit = rpc_arg.leader_commit;
        arg.entries.insert(arg.entries.end(), rpc_arg.entries.begin(), rpc_arg.entries.end());

        return arg;
    }

    struct AppendEntriesReply
    {
        /* Lab3: Your code here */
        int term;     // leader's term
        bool success; // true if follower contained entry matching prevLogIndex and prevLogTerm

        MSGPACK_DEFINE(
            term,
            success)
    };

    struct InstallSnapshotArgs
    {
        /* Lab3: Your code here */

        MSGPACK_DEFINE(

        )
    };

    struct InstallSnapshotReply
    {
        /* Lab3: Your code here */

        MSGPACK_DEFINE(

        )
    };

} /* namespace chfs */