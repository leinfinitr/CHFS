#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs
{

    enum class RaftRole
    {
        Follower,
        Candidate,
        Leader
    };

    struct RaftNodeConfig
    {
        int node_id;
        uint16_t port;
        std::string ip_address;
    };

    /**
     * The RaftNode class represents a Raft node (or Raft server).
     * RaftNode is a class template with two template parameters, StateMachine and Command.
     * Remember we're implementing a raft library that decouples the consensus algorithm from the replicated state machine.
     * Therefore, the user can implement their own state machine and pass it to the Raft library via the two template parameters.
     */
    template <typename StateMachine, typename Command>
    class RaftNode
    {

#define RAFT_LOG(fmt, args...)                                                                                                            \
    do                                                                                                                                    \
    {                                                                                                                                     \
        auto now =                                                                                                                        \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                                                        \
                std::chrono::system_clock::now().time_since_epoch())                                                                      \
                .count();                                                                                                                 \
        char buf[512];                                                                                                                    \
        /*sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); */ \
        sprintf(buf, "[%ld][node %d term %d role %d] " fmt "\n", now, my_id, current_term, role, ##args);                                 \
        thread_pool->enqueue([=]() { std::cerr << buf; });                                                                                \
    } while (0);

    public:
        RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
        ~RaftNode();

        /* interfaces for test */
        void set_network(std::map<int, bool> &network_availablility);
        void set_reliable(bool flag);
        int get_list_state_log_num();
        int rpc_count();
        std::vector<u8> get_snapshot_direct();

    private:
        /*
         * Start the raft node.
         * Please make sure all of the rpc request handlers have been registered before this method.
         */
        auto start() -> int;

        /*
         * Stop the raft node.
         */
        auto stop() -> int;

        /* Returns whether this node is the leader, you should also return the current term. */
        auto is_leader() -> std::tuple<bool, int>;

        /* Checks whether the node is stopped */
        auto is_stopped() -> bool;

        /*
         * Send a new command to the raft nodes.
         * The returned tuple of the method contains three values:
         * 1. bool:  True if this raft node is the leader that successfully appends the log,
         *      false If this node is not the leader.
         * 2. int: Current term.
         * 3. int: Log index.
         */
        auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

        /* Save a snapshot of the state machine and compact the log. */
        auto save_snapshot() -> bool;

        /* Get a snapshot of the state machine */
        auto get_snapshot() -> std::vector<u8>;

        /* Internal RPC handlers */
        auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
        auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
        auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

        /* RPC helpers */
        void send_request_vote(int target, RequestVoteArgs arg);
        void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

        void send_append_entries(int target, AppendEntriesArgs<Command> arg);
        void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

        void send_install_snapshot(int target, InstallSnapshotArgs arg);
        void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

        /* background workers */
        void run_background_ping();
        void run_background_election();
        void run_background_commit();
        void run_background_apply();

        /* Data structures */
        bool network_stat; /* for test */

        std::mutex mtx;                                /* A big lock to protect the whole data structure. */
        std::mutex clients_mtx;                        /* A lock to protect RpcClient pointers */
        std::unique_ptr<ThreadPool> thread_pool;       /* A thread pool to run the background workers. */
        std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
        std::unique_ptr<StateMachine> state;           /*  The state machine that applies the raft log, e.g. a kv store. */

        std::unique_ptr<RpcServer> rpc_server;                     /* RPC server to recieve and handle the RPC requests. */
        std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map; /* RPC clients of all raft nodes including this node. */
        std::vector<RaftNodeConfig> node_configs;                  /* Configuration for all nodes */
        int my_id;                                                 /* The index of this node in rpc_clients, start from 0. */

        std::atomic_bool stopped; /* Whether this node is stopped. */

        RaftRole role;
        int current_term; /* Latest term server has seen (initialized to 0 on first boot, increases monotonically) */
        int leader_id;

        int voted_for;        /* CandidateId that received vote in current term (or null if none) */
        int vote_count;       /* The number of votes this node received in the current election */
        int election_timer;   /* The number of ticks since the last election timeout */
        int election_timeout; /* Chosen randomly from a fixed interval to ensure that split votes are rare */

        int last_ping_timer; /* The number of ticks since the last receiverd heartbeat */
        int ping_timeout;    /* Chosen a fixed interval to check the liveness of the leader */

        // Volatile state on all servers:
        int commit_index; /* Index of highest log entry known to be committed (initialized to 0, increases monotonically) */
        int last_applied; /* Index of highest log entry applied to state machine (initialized to 0, increases monotonically) */

        // Volatile state on leaders:
        std::vector<int> next_index;  /* For each server, index of the next log entry to send to that server (initialized to leader last log index + 1) */
        std::vector<int> match_index; /* For each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically) */

        std::unique_ptr<std::thread> background_election;
        std::unique_ptr<std::thread> background_ping;
        std::unique_ptr<std::thread> background_commit;
        std::unique_ptr<std::thread> background_apply;

        /* Lab3: Your code here */

        /**
         * Reset the vote_count = 0, election_timer = 0, last_ping_timer = 0
         */
        void reset_counter()
        {
            vote_count = 0;
            election_timer = 0;
            last_ping_timer = 0;
        }
    };

    template <typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs) : network_stat(true),
                                                                                                  node_configs(configs),
                                                                                                  my_id(node_id),
                                                                                                  stopped(true),
                                                                                                  role(RaftRole::Follower),
                                                                                                  current_term(0),
                                                                                                  leader_id(-1),
                                                                                                  voted_for(-1),
                                                                                                  vote_count(0),
                                                                                                  election_timer(0),
                                                                                                  election_timeout(rand() % 150 + 150),
                                                                                                  last_ping_timer(0),
                                                                                                  ping_timeout(1000),
                                                                                                  commit_index(0),
                                                                                                  last_applied(0)
    {
        auto my_config = node_configs[my_id];

        /* launch RPC server */
        rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

        /* Register the RPCs. */
        rpc_server->bind(RAFT_RPC_START_NODE, [this]()
                         { return this->start(); });
        rpc_server->bind(RAFT_RPC_STOP_NODE, [this]()
                         { return this->stop(); });
        rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]()
                         { return this->is_leader(); });
        rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]()
                         { return this->is_stopped(); });
        rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size)
                         { return this->new_command(data, cmd_size); });
        rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]()
                         { return this->save_snapshot(); });
        rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]()
                         { return this->get_snapshot(); });

        rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg)
                         { return this->request_vote(arg); });
        rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg)
                         { return this->append_entries(arg); });
        rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg)
                         { return this->install_snapshot(arg); });

        /* Lab3: Your code here */
        thread_pool = std::make_unique<ThreadPool>(4);
        log_storage = std::make_unique<RaftLog<Command>>(nullptr);
        state = std::make_unique<StateMachine>();
        // Initialize rpc_clients_map
        // RAFT_LOG("Node %d init rpc_clients_map", my_id);
        for (auto config : node_configs)
        {
            RAFT_LOG("Insert: %d, %s:%d", config.node_id, config.ip_address.c_str(), static_cast<int>(config.port));
            rpc_clients_map[config.node_id] = std::make_unique<RpcClient>(config.ip_address, config.port, true);
        }
        // Initialize next_index and match_index
        // RAFT_LOG("Node %d init next_index and match_index", my_id);
        for (int i = 0; i < node_configs.size(); i++)
        {
            next_index.push_back(log_storage->last_log_index() + 1);
            match_index.push_back(0);
        }

        // RAFT_LOG("Node %d init rpc_clients_map done", my_id);
        rpc_server->run(true, 8); // MUST BE TRUE!
        RAFT_LOG("Node %d init rpc_server done", my_id);
    }

    template <typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::~RaftNode()
    {
        // 不能加锁，否则会进入死循环
        stop();

        thread_pool.reset();
        // std::cout << "Node " << my_id << " thread_pool reset" << std::endl;
        rpc_server.reset();
        // std::cout << "Node " << my_id << " rpc_server reset" << std::endl;
        state.reset();
        // std::cout << "Node " << my_id << " state reset" << std::endl;
        log_storage.reset();
        std::cout << "Node " << my_id << " destructed" << std::endl;
    }

    /******************************************************************

                            RPC Interfaces

    *******************************************************************/

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::start() -> int
    {
        /* Lab3: Your code here */
        RAFT_LOG("Node %d start: config_size: %d, my_config: %d, %s:%d", my_id, static_cast<int>(node_configs.size()), my_id, node_configs[my_id].ip_address.c_str(), static_cast<int>(node_configs[my_id].port));
        mtx.lock();
        stopped.store(false);
        mtx.unlock();

        background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
        background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
        background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
        background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::stop() -> int
    {
        /* Lab3: Your code here */
        mtx.lock();
        stopped.store(true);
        mtx.unlock();

        // 等待所有子线程结束
        // 否则在析构对象时会报错 terminate called without an active exception
        RAFT_LOG("Node %d wait for background threads to join", my_id);
        background_election->join();
        // std::cout << "Node " << my_id << " background_election joined" << std::endl;
        background_ping->join();
        // std::cout << "Node " << my_id << " background_ping joined" << std::endl;
        background_commit->join();
        // std::cout << "Node " << my_id << " background_commit joined" << std::endl;
        background_apply->join();
        std::cout << "Node " << my_id << " background_apply joined" << std::endl;

        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
    {
        /* Lab3: Your code here */
        RAFT_LOG("Node %d check leader", my_id);
        if (role == RaftRole::Leader)
        {
            return std::make_tuple(true, current_term);
        }
        else
        {
            return std::make_tuple(false, current_term);
        }
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_stopped() -> bool
    {
        return stopped.load();
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
    {
        /* Lab3: Your code here */
        // Only work for leader
        // Append entry to local log, respond after entry applied to state machine
        if (role != RaftRole::Leader)
        {
            return std::make_tuple(false, current_term, -1);
        }
        Command cmd;
        cmd.deserialize(cmd_data, cmd_size);
        log_storage->append_log(current_term, cmd);
        state->apply_log(cmd);
        RAFT_LOG("Node %d append log, term %d, index %d, data %d", my_id, current_term, log_storage->last_log_index(), cmd.value);
        return std::make_tuple(true, current_term, log_storage->last_log_index());
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
    {
        /* Lab3: Your code here */
        return true;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
    {
        /* Lab3: Your code here */
        return std::vector<u8>();
    }

    /******************************************************************

                             Internal RPC Related

    *******************************************************************/

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
    {
        /* Lab3: Your code here */
        RAFT_LOG("Node %d receive request vote from node %d", my_id, args.candidate_id);
        RequestVoteReply reply;
        // Reply false if term < currentTerm

        bool vote_granted = false;
        if (args.term > current_term)
        {
            vote_granted = true;
        }
        //  If votedFor is null or candidateId, and candidate’s log is at least as up - to - date as receiver’s log, grant vote
        else if (voted_for == -1 || voted_for == args.candidate_id)
        {
            // If the logs have last entries with different terms, then the log with the later term is more up-to-date.
            if (log_storage->last_log_term() < args.last_log_term)
            {
                vote_granted = true;
            }
            else if (log_storage->last_log_term() == args.last_log_term)
            {
                // If the logs end with the same term, then whichever log is longer is more up-to-date.
                if (log_storage->last_log_index() <= args.last_log_index)
                {
                    vote_granted = true;
                }
            }
        }

        if (vote_granted)
        {
            mtx.lock();
            current_term = args.term;
            role = RaftRole::Follower;
            leader_id = -1;
            reset_counter();
            voted_for = args.candidate_id;
            mtx.unlock();

            reply.term = current_term;
            reply.vote_granted = true;
        }
        else
        {
            reply.term = current_term;
            reply.vote_granted = false;
        }

        RAFT_LOG("Node %d reply request vote to node %d, term %d, vote_granted %d", my_id, args.candidate_id, reply.term, reply.vote_granted);
        return reply;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
    {
        /* Lab3: Your code here */
        RAFT_LOG("Node %d receive request vote reply from node %d, term %d, vote_granted %d", my_id, target, reply.term, reply.vote_granted);
        std::unique_lock<std::mutex> lock(mtx);
        // If role is not candidate, return
        if (role != RaftRole::Candidate)
        {
            return;
        }
        // If receive vote from majority of servers: become leader
        if (reply.vote_granted)
        {
            vote_count++;
            if (vote_count > node_configs.size() / 2)
            {
                role = RaftRole::Leader;
                leader_id = my_id;
                election_timer = 0;

                // Sends heartbeat messages to all of the other servers to establish its authority and prevent new elections.
                RAFT_LOG("Node %d become leader", my_id);
                for (int i = 0; i < node_configs.size(); i++)
                {
                    if (i != my_id)
                    {
                        AppendEntriesArgs<Command> arg;
                        arg.term = current_term;
                        arg.leader_id = my_id;
                        arg.prev_log_index = log_storage->last_log_index();
                        arg.prev_log_term = log_storage->last_log_term();
                        arg.leader_commit = commit_index;

                        RAFT_LOG("Node %d send heartbeat to node %d in handle_request_vote_reply", my_id, i);
                        thread_pool->enqueue([this, i, arg]()
                                             { send_append_entries(i, arg); });
                    }
                }
            }
            return;
        }
        // If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
        if (reply.term > current_term)
        {
            current_term = reply.term;
            role = RaftRole::Follower;
            reset_counter();
            voted_for = -1;
        }

        return;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
    {
        /* Lab3: Your code here */
        AppendEntriesReply reply;

        // Reply false if term < currentTerm
        if (rpc_arg.term < current_term)
        {
            reply.term = current_term;
            reply.success = false;
            return reply;
        }

        /**
         * While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
         * If the leader’s term (included in its RPC) is at least as large as the candidate’s current term, then the candidate recognizes the leader as legitimate and returns to follower state.
         * If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
         */

        // If entries is empty, this is a heartbeat message
        if (rpc_arg.command_value.size() == 0)
        {
            RAFT_LOG("Node %d receive heartbeat from node %d", my_id, rpc_arg.leader_id);

            if (rpc_arg.term >= current_term)
            {
                mtx.lock();
                current_term = rpc_arg.term;
                role = RaftRole::Follower;
                leader_id = rpc_arg.leader_id;
                reset_counter();
                mtx.unlock();

                reply.term = current_term;
                reply.success = true;
                return reply;
            }
            else
            {
                reply.term = current_term;
                reply.success = false;
                return reply;
            }
        }
        else
        {
            RAFT_LOG("Node %d receive append entries from node %d", my_id, rpc_arg.leader_id);
            RAFT_LOG("prev_log_index: %d, prev_log_term: %d", rpc_arg.prev_log_index, rpc_arg.prev_log_term);
            // Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
            if ((log_storage->last_log_index() < rpc_arg.prev_log_index) ||
                (log_storage->last_log_index() == rpc_arg.prev_log_index && log_storage->last_log_term() != rpc_arg.prev_log_term))
            {
                reply.term = current_term;
                reply.success = false;
                return reply;
            }
            // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
            while (log_storage->last_log_index() > rpc_arg.prev_log_index)
            {
                log_storage->log_entries.pop_back();
            }
            // To this step, we can ensure all logs before prev_log_index are the same as the leader's
            // Append any new entries not already in the log
            RAFT_LOG("Node %d append entries, term %d, index %d, size %d", my_id, rpc_arg.term, log_storage->last_log_index() + 1, int(rpc_arg.command_value.size()));
            for (int i = 0; i < rpc_arg.command_value.size(); i++)
            {
                Command cmd;
                cmd.value = rpc_arg.command_value[i];
                log_storage->append_log(rpc_arg.term, cmd);
                state->apply_log(cmd);
            }
            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if (rpc_arg.leader_commit > commit_index)
            {
                commit_index = std::min(rpc_arg.leader_commit, log_storage->last_log_index());
            }

            reply.term = current_term;
            reply.success = true;
        }

        return reply;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
    {
        /* Lab3: Your code here */
        // It should only work for leader
        if (role != RaftRole::Leader)
        {
            return;
        }
        // If log_entries.size() == 0, this is a heartbeat message
        if (arg.log_entries.size() == 0)
        {
            if (reply.term > current_term)
            {
                mtx.lock();
                current_term = reply.term;
                role = RaftRole::Follower;
                leader_id = -1;
                reset_counter();
                voted_for = -1;
                mtx.unlock();
            }
            return;
        }
        else
        {
            if (reply.term > current_term)
            {
                mtx.lock();
                current_term = reply.term;
                role = RaftRole::Follower;
                leader_id = -1;
                reset_counter();
                voted_for = -1;
                mtx.unlock();
                return;
            }
            if (reply.success)
            {
                // If successful: update nextIndex and matchIndex for follower
                // If there exists an N such that N > commitIndex, a majority
                // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                // set commitIndex = N
                mtx.lock();
                next_index[node_id] = arg.prev_log_index + arg.log_entries.size() + 1;
                match_index[node_id] = arg.prev_log_index + arg.log_entries.size();
                int N = commit_index + 1;
                while (N <= log_storage->last_log_index())
                {
                    int count = 1;
                    for (int i = 0; i < node_configs.size(); i++)
                    {
                        if (i != my_id && match_index[i] >= N)
                        {
                            count++;
                        }
                    }
                    if (count > node_configs.size() / 2 && log_storage->log_entries[N].term == current_term)
                    {
                        commit_index = N;
                    }
                    N++;
                }
                mtx.unlock();
            } // If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
            else
            {
                mtx.lock();
                next_index[node_id]--;
                AppendEntriesArgs<Command> arg;
                arg.term = current_term;
                arg.leader_id = my_id;
                arg.prev_log_index = next_index[node_id] - 1;
                arg.prev_log_term = log_storage->log_entries[arg.prev_log_index].term;
                arg.leader_commit = commit_index;

                // Construct log_entries which contains the logs after prev_log_index
                std::vector<Entry<Command>> log_entries;
                log_entries.assign(log_storage->log_entries.begin() + arg.prev_log_index + 1, log_storage->log_entries.end());
                arg.log_entries = log_entries;

                mtx.unlock();
                RAFT_LOG("Node %d send append entries to node %d in handle_append_entries_reply", my_id, node_id);
                thread_pool->enqueue([this, node_id, arg]()
                                     { send_append_entries(node_id, arg); });
            }
        }
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
    {
        /* Lab3: Your code here */
        return InstallSnapshotReply();
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
    {
        /* Lab3: Your code here */
        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        RAFT_LOG("Send request vote to node %d", target_id)

        if (rpc_clients_map[target_id] == nullptr)
        {
            RAFT_LOG("Node %d is nullptr in send_request_vote", target_id);
            return;
        }
        else if (rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
            RAFT_LOG("Node %d is not connected in send_request_vote", target_id);
            return;
        }

        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
        clients_lock.unlock();
        if (res.is_ok())
        {
            handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
        }
        else
        {
            // RPC fails
            RAFT_LOG("Node %d RPC fails", target_id);
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
    {
        // RAFT_LOG("Send append entries to node %d", target_id);
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
            RAFT_LOG("Node %d is not connected in send_append_entries", target_id);
            return;
        }

        RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
        clients_lock.unlock();
        if (res.is_ok())
        {
            handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
        }
        else
        {
            // RPC fails
            RAFT_LOG("Node %d RPC fails in send_append_entries", target_id);
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
            return;
        }

        auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
        clients_lock.unlock();
        if (res.is_ok())
        {
            handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
        }
        else
        {
            // RPC fails
        }
    }

    /******************************************************************

                            Background Workers

    *******************************************************************/

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_election()
    {
        // Work for followers and candidates.

        /* Uncomment following code when you finish */
        RAFT_LOG("Node %d run background election", my_id);
        while (true)
        {
            usleep(1000);
            if (is_stopped())
            {
                return;
            }
            /* Lab3: Your code here */
            if (role == RaftRole::Follower)
            {
                // Periodly check the liveness of the leader.
                // If the leader is dead, start a new election.
                if (last_ping_timer < ping_timeout)
                {
                    last_ping_timer++;
                    continue;
                }
                last_ping_timer = 0;
                if (leader_id == -1 || rpc_clients_map[leader_id] == nullptr || rpc_clients_map[leader_id]->get_connection_state() != rpc::client::connection_state::connected)
                {
                    role = RaftRole::Candidate;

                    mtx.lock();
                    current_term++;
                    voted_for = my_id;
                    vote_count = 1;
                    election_timer = 0;
                    mtx.unlock();

                    RAFT_LOG("Node %d start election", my_id);
                    // Issues RequestVote RPCs in parallel to each of the other servers in the cluster.
                    for (int i = 0; i < node_configs.size(); i++)
                    {
                        if (i != my_id)
                        {
                            RequestVoteArgs arg;
                            arg.term = current_term;
                            arg.candidate_id = my_id;
                            arg.last_log_index = log_storage->last_log_index();
                            arg.last_log_term = log_storage->last_log_term();

                            thread_pool->enqueue([this, i, arg]()
                                                 { send_request_vote(i, arg); });
                        }
                    }
                }
            }
            else if (role == RaftRole::Candidate)
            {
                election_timer++;
                // If election timeout elapses: start new election
                if (election_timer > election_timeout)
                {
                    mtx.lock();
                    current_term++;
                    voted_for = my_id;
                    vote_count = 1;
                    election_timer = 0;
                    mtx.unlock();

                    RAFT_LOG("Node %d start new election", my_id);
                    for (int i = 0; i < node_configs.size(); i++)
                    {
                        if (i != my_id)
                        {
                            mtx.lock();
                            RequestVoteArgs arg;
                            arg.term = current_term;
                            arg.candidate_id = my_id;
                            arg.last_log_index = log_storage->last_log_index();
                            arg.last_log_term = log_storage->last_log_term();
                            mtx.unlock();

                            thread_pool->enqueue([this, i, arg]()
                                                 { send_request_vote(i, arg); });
                        }
                    }
                }
            }
        }
        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_commit()
    {
        // Periodly send logs to the follower.
        // Only work for the leader.
        /* Uncomment following code when you finish */
        while (true)
        {
            usleep(100000);
            if (is_stopped())
            {
                return;
            }
            if (role != RaftRole::Leader)
            {
                continue;
            }
            else
            {
                RAFT_LOG("Node %d send logs to followers in run_background_commit", my_id);
                AppendEntriesArgs<Command> arg;
                mtx.lock();
                arg.term = current_term;
                arg.leader_id = my_id;
                arg.prev_log_index = log_storage->prev_log_index();
                arg.prev_log_term = log_storage->prev_log_term();
                arg.leader_commit = commit_index;
                mtx.unlock();

                // If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
                for (int i = 0; i < node_configs.size(); i++)
                {
                    if (i != my_id && next_index[i] <= log_storage->last_log_index())
                    {
                        // Construct log_entries which contains the logs starting at next_index
                        std::vector<Entry<Command>> log_entries;
                        log_entries.assign(log_storage->log_entries.begin() + next_index[i], log_storage->log_entries.end());
                        arg.log_entries = log_entries;

                        RAFT_LOG("Node %d send append entries to node %d in run_background_commit, next_index = %d, last_log_index = %d, log_entries.size() = %d", my_id, i, next_index[i], log_storage->last_log_index(), int(arg.log_entries.size()));
                        thread_pool->enqueue([this, i, arg]()
                                             { send_append_entries(i, arg); });
                    }
                }
            }
        }

        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_apply()
    {
        // Periodly apply committed logs the state machine
        // Work for all the nodes.

        /* Uncomment following code when you finish */
        while (true)
        {
            usleep(100000);
            if (is_stopped())
            {
                return;
            }
            if (last_applied == commit_index)
            {
                continue;
            }
            else
            {
                RAFT_LOG("Node %d apply logs to state machine", my_id);
                mtx.lock();
                last_applied = commit_index;
                mtx.unlock();
                for (int i = last_applied + 1; i <= commit_index; i++)
                {
                    Command cmd_entry = log_storage->log_entries[i].cmd;
                    state->apply_log(cmd_entry);
                }
            }
        }

        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_ping()
    {
        // Periodly send empty append_entries RPC to the followers.
        // Only work for the leader.

        /* Uncomment following code when you finish */
        while (true)
        {
            usleep(100000);
            if (is_stopped())
            {
                return;
            }
            /* Lab3: Your code here */
            if (role == RaftRole::Leader)
            {
                for (int i = 0; i < node_configs.size(); i++)
                {
                    if (i != my_id)
                    {
                        AppendEntriesArgs<Command> arg;
                        mtx.lock();
                        arg.term = current_term;
                        arg.leader_id = my_id;
                        arg.prev_log_index = log_storage->last_log_index();
                        arg.prev_log_term = log_storage->last_log_term();
                        arg.leader_commit = commit_index;
                        mtx.unlock();

                        RAFT_LOG("Node %d send heartbeat to node %d in run_background_ping", my_id, i);
                        thread_pool->enqueue([this, i, arg]()
                                             { send_append_entries(i, arg); });
                    }
                }
            }
        }

        return;
    }

    /******************************************************************

                              Test Functions (must not edit)

    *******************************************************************/

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        /* turn off network */
        if (!network_availability[my_id])
        {
            for (auto &&client : rpc_clients_map)
            {
                if (client.second != nullptr)
                    client.second.reset();
            }

            return;
        }

        for (auto node_network : network_availability)
        {
            int node_id = node_network.first;
            bool node_status = node_network.second;

            if (node_status && rpc_clients_map[node_id] == nullptr)
            {
                RaftNodeConfig target_config;
                for (auto config : node_configs)
                {
                    if (config.node_id == node_id)
                        target_config = config;
                }

                rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
                // Check rpc state
                // if (rpc_clients_map[node_id]->get_connection_state() != rpc::client::connection_state::connected)
                // {
                //     RAFT_LOG("Node %d is not connected in set_network", node_id);
                // } else {
                //     RAFT_LOG("Node %d is connected in set_network", node_id);
                // }
                while (rpc_clients_map[node_id]->get_connection_state() != rpc::client::connection_state::connected)
                {
                    RAFT_LOG("Node %d is not connected in set_network", node_id);
                    usleep(10000);
                }
            }

            if (!node_status && rpc_clients_map[node_id] != nullptr)
            {
                rpc_clients_map[node_id].reset();
            }
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::set_reliable(bool flag)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        for (auto &&client : rpc_clients_map)
        {
            if (client.second)
            {
                client.second->set_reliable(flag);
            }
        }
    }

    template <typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::get_list_state_log_num()
    {
        /* only applied to ListStateMachine*/
        std::unique_lock<std::mutex> lock(mtx);

        return state->num_append_logs;
    }

    template <typename StateMachine, typename Command>
    int RaftNode<StateMachine, Command>::rpc_count()
    {
        int sum = 0;
        std::unique_lock<std::mutex> clients_lock(clients_mtx);

        for (auto &&client : rpc_clients_map)
        {
            if (client.second)
            {
                sum += client.second->count();
            }
        }

        return sum;
    }

    template <typename StateMachine, typename Command>
    std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
    {
        if (is_stopped())
        {
            return std::vector<u8>();
        }

        std::unique_lock<std::mutex> lock(mtx);

        return state->snapshot();
    }
}