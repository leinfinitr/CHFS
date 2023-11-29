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

#define RAFT_LOG(fmt, args...)                                                                                                       \
    do                                                                                                                               \
    {                                                                                                                                \
        auto now =                                                                                                                   \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                                                   \
                std::chrono::system_clock::now().time_since_epoch())                                                                 \
                .count();                                                                                                            \
        char buf[512];                                                                                                               \
        sprintf(buf, "[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf; });                                                                           \
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

        int commit_index; /* Index of highest log entry known to be committed (initialized to 0, increases monotonically) */
        int last_applied; /* Index of highest log entry applied to state machine (initialized to 0, increases monotonically) */

        std::unique_ptr<std::thread> background_election;
        std::unique_ptr<std::thread> background_ping;
        std::unique_ptr<std::thread> background_commit;
        std::unique_ptr<std::thread> background_apply;

        /* Lab3: Your code here */

        /**
         * Reset the vote_for = -1, vote_count = 0, election_timer = 0.
         */
        void reset_vote()
        {
            voted_for = -1;
            vote_count = 0;
            election_timer = 0;
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
    }

    template <typename StateMachine, typename Command>
    RaftNode<StateMachine, Command>::~RaftNode()
    {
        stop();

        thread_pool.reset();
        rpc_server.reset();
        state.reset();
        log_storage.reset();
    }

    /******************************************************************

                            RPC Interfaces

    *******************************************************************/

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::start() -> int
    {
        /* Lab3: Your code here */
        if (!stopped.load())
        {
            return 0;
        }
        stopped.store(false);

        RAFT_LOG("Node start");
        std::cout << "Node %d start" << my_id << std::endl;

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
        if (stopped.load())
        {
            return 0;
        }
        stopped.store(true);
        return 0;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
    {
        /* Lab3: Your code here */
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
        return std::make_tuple(false, -1, -1);
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
        RequestVoteReply reply;
        // Reply false if term < currentTerm
        mtx.lock();

        if (args.term < current_term)
        {
            reply.term = current_term;
            reply.vote_granted = false;
        }
        else if (args.term > current_term)
        {
            current_term = args.term;
            role = RaftRole::Follower;
            leader_id = -1;
            voted_for = args.candidate_id;
            vote_count = 0;

            reply.term = current_term;
            reply.vote_granted = true;
        }
        //  If votedFor is null or candidateId, and candidate’s log is at least as up - to - date as receiver’s log, grant vote
        else if (voted_for == -1 || voted_for == args.candidate_id)
        {
            // If the logs have last entries with different terms, then the log with the later term is more up-to-date.
            if (log_storage->last_log_term() < args.last_log_term)
            {
                reply.term = current_term;
                reply.vote_granted = true;
            }
            else if (log_storage->last_log_term() == args.last_log_term)
            {
                // If the logs end with the same term, then whichever log is longer is more up-to-date.
                if (log_storage->last_log_index() <= args.last_log_index)
                {
                    reply.term = current_term;
                    reply.vote_granted = true;
                }
                else
                {
                    reply.term = current_term;
                    reply.vote_granted = false;
                }
            }
            else
            {
                reply.term = current_term;
                reply.vote_granted = false;
            }
        }
        else
        {
            reply.term = current_term;
            reply.vote_granted = false;
        }

        mtx.unlock();
        return reply;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
    {
        /* Lab3: Your code here */
        mtx.lock();
        // If receive vote from majority of servers: become leader
        if (reply.vote_granted)
        {
            vote_count++;
            if (vote_count > node_configs.size() / 2)
            {
                role = RaftRole::Leader;
                leader_id = my_id;
                reset_vote();

                // Sends heartbeat messages to all of the other servers to establish its authority and prevent new elections.
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

                        std::thread t([this, i, arg]()
                                      { send_append_entries(i, arg); });
                        t.detach();
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
            reset_vote();
        }

        mtx.unlock();
        return;
    }

    template <typename StateMachine, typename Command>
    auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
    {
        /* Lab3: Your code here */
        AppendEntriesReply reply;

        /**
         * While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader.
         * If the leader’s term (included in its RPC) is at least as large as the candidate’s current term, then the candidate recognizes the leader as legitimate and returns to follower state.
         * If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
         */

        // If entries is empty, this is a heartbeat message
        if (rpc_arg.entries.size() == 0)
        {
            if (rpc_arg.term >= current_term)
            {
                current_term = rpc_arg.term;
                role = RaftRole::Follower;
                leader_id = rpc_arg.leader_id;
                reset_vote();

                reply.term = current_term;
                reply.success = true;
            }
            else
            {
                reply.term = current_term;
                reply.success = false;
            }
        }
        return reply;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
    {
        /* Lab3: Your code here */
        // If entries is empty, this is a heartbeat message
        if (arg.entries.size() == 0)
        {
            if (reply.term > current_term)
            {
                current_term = reply.term;
                role = RaftRole::Follower;
                leader_id = -1;
                reset_vote();
            }
            return;
        }
        return;
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
        if (rpc_clients_map[target_id] == nullptr || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
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
        }
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
    {
        std::unique_lock<std::mutex> clients_lock(clients_mtx);
        if (rpc_clients_map[target_id] == nullptr || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected)
        {
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
        RAFT_LOG("Run backgroung election");
        std::cout << "Node %d run backgroung election" << my_id << std::endl;
        while (true)
        {
            if (is_stopped())
            {
                return;
            }
            /* Lab3: Your code here */
            if (role == RaftRole::Follower)
            {
                // Periodly check the liveness of the leader.
                // If the leader is dead, start a new election.
                sleep(1);
                if (leader_id == -1 || rpc_clients_map[leader_id] == nullptr || rpc_clients_map[leader_id]->get_connection_state() != rpc::client::connection_state::connected)
                {
                    RAFT_LOG("Leader dead and start new election");
                    role = RaftRole::Candidate;

                    mtx.lock();
                    current_term++;
                    voted_for = my_id;
                    vote_count = 1;
                    election_timer = 0;
                    mtx.unlock();

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

                            std::thread t([this, i, arg]()
                                          { send_request_vote(i, arg); });
                            t.detach();
                        }
                    }
                }
            }
            else if (role == RaftRole::Candidate)
            {
                usleep(1000);
                election_timer++;
                RAFT_LOG("Election timer: %d", election_timer);
                std::cout << "Node %d election timer: %d" << my_id << election_timer << std::endl;
                // If election timeout elapses: start new election
                if (election_timer > election_timeout)
                {
                    mtx.lock();
                    current_term++;
                    voted_for = my_id;
                    vote_count = 1;
                    election_timer = 0;
                    mtx.unlock();
                    for (int i = 0; i < node_configs.size(); i++)
                    {
                        if (i != my_id)
                        {
                            RequestVoteArgs arg;
                            arg.term = current_term;
                            arg.candidate_id = my_id;
                            arg.last_log_index = log_storage->last_log_index();
                            arg.last_log_term = log_storage->last_log_term();

                            std::thread t([this, i, arg]()
                                          { send_request_vote(i, arg); });
                            t.detach();
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
        // while (true) {
        //     {
        //         if (is_stopped()) {
        //             return;
        //         }
        //         /* Lab3: Your code here */
        //     }
        // }

        return;
    }

    template <typename StateMachine, typename Command>
    void RaftNode<StateMachine, Command>::run_background_apply()
    {
        // Periodly apply committed logs the state machine

        // Work for all the nodes.

        /* Uncomment following code when you finish */
        // while (true) {
        //     {
        //         if (is_stopped()) {
        //             return;
        //         }
        //         /* Lab3: Your code here */
        //     }
        // }

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
            if (is_stopped())
            {
                return;
            }
            /* Lab3: Your code here */
            if (role == RaftRole::Leader)
            {
                sleep(1);
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

                        std::thread t([this, i, arg]()
                                      { send_append_entries(i, arg); });
                        t.detach();
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