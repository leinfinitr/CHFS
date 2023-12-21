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
        int term;                                // leader's term
        int leader_id;                           // so follower can redirect clients
        int prev_log_index;                      // index of log entry immediately preceding new ones
        int prev_log_term;                       // term of prevLogIndex entry
        int leader_commit;                       // leader's commitIndex
        std::vector<Entry<Command>> log_entries; // log entries to store (empty for heartbeat; may send more than one for efficiency)
    };

    struct RpcAppendEntriesArgs
    {
        /* Lab3: Your code here */
        int term;           // leader's term
        int leader_id;      // so follower can redirect clients
        int prev_log_index; // index of log entry immediately preceding new ones
        int prev_log_term;  // term of prevLogIndex entry
        int leader_commit;  // leader's commitIndex
        // Each entry contains a command and the term number when the entry was received by the leader
        std::vector<int> command_value; // log command to store (empty for heartbeat; may send more than one for efficiency)
        std::vector<int> entry_term;    // log term to store (empty for heartbeat; may send more than one for efficiency)

        MSGPACK_DEFINE(
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            command_value,
            entry_term)
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
        // std::cout << "arg.log_entries.size() = " << arg.log_entries.size() << std::endl;
        for (const Entry<Command> &cmd_entry : arg.log_entries)
        {
            rpc_arg.command_value.push_back(cmd_entry.cmd.value);
            rpc_arg.entry_term.push_back(cmd_entry.term);
        }
        // std::cout << "rpc_arg.command_value.size() = " << rpc_arg.command_value.size() << " rpc_arg.entry_term.size() = " << rpc_arg.entry_term.size() << std::endl;
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
        // std::cout << "rpc_arg.entries.size() = " << rpc_arg.entries.size() << std::endl;
        for (int i = 0; i < rpc_arg.command_value.size(); i++)
        {
            Entry<Command> cmd_entry;
            cmd_entry.cmd.value = rpc_arg.command_value[i];
            cmd_entry.term = rpc_arg.entry_term[i];
            arg.log_entries.push_back(cmd_entry);
        }

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
        int term;                // leader's term
        int leader_id;           // so follower can redirect clients
        int last_included_index; // the snapshot replaces all entries up through and including this index
        int last_included_term;  // term of last_included_index
        int offset;              // byte offset where chunk is positioned in the snapshot file
        std::vector<u8> data;    // raw bytes of the snapshot chunk, starting at offset
        bool done;               // true if this is the last chunk

        MSGPACK_DEFINE(
            term,
            leader_id,
            last_included_index,
            last_included_term,
            offset,
            data,
            done)
    };

    struct InstallSnapshotReply
    {
        /* Lab3: Your code here */
        int term; // currentTerm, for leader to update itself

        MSGPACK_DEFINE(
            term)
    };

} /* namespace chfs */