#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include "librpc/client.h"
#include "librpc/server.h"
#include "distributed/client.h"

// Lab4: Free to modify this file

namespace mapReduce
{
    struct KeyVal
    {
        KeyVal(const std::string &key, const std::string &val) : key(key), val(val) {}
        KeyVal() {}
        std::string key;
        std::string val;
    };

    enum mr_tasktype
    {
        NONE = 0,
        MAP,
        REDUCE
    };

    std::vector<KeyVal> Map(const std::string &content);

    std::string Reduce(const std::string &key, const std::vector<std::string> &values);

    const std::string ASK_TASK = "ask_task";
    const std::string SUBMIT_TASK = "submit_task";

    struct MR_CoordinatorConfig
    {
        uint16_t port;
        std::string ip_address;
        std::string resultFile;
        std::shared_ptr<chfs::ChfsClient> client;

        MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                             std::string resultFile) : port(port), ip_address(std::move(ip_address)),
                                                       resultFile(resultFile), client(std::move(client)) {}
    };

    struct TaskArgs
    {
        int taskType;
        // 当 taskType 为 MAP 时，fileIndex 为文件编号，fileName 为文件名
        // 当 taskType 为 REDUCE 时，fileIndex 为 reduce 任务编号，fileName 为文件数
        int fileIndex;
        std::string fileName;

        MSGPACK_DEFINE(
            taskType,
            fileIndex,
            fileName
        )
    };

    class SequentialMapReduce
    {
    public:
        SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files, std::string resultFile);
        void doWork();
        void doMap();
        void doReduce();

    private:
        std::shared_ptr<chfs::ChfsClient> chfs_client; // 保存所有文件内容的客户端
        std::vector<std::string> files;                // 所有输入文件的文件名
        std::string outPutFile;                        // 输出文件名
    };

    class Coordinator
    {
    public:
        Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int workerNum);
        TaskArgs askTask();
        int submitTask(int taskType);
        bool Done();

    private:
        std::vector<std::string> files; // 所有输入文件的文件名
        std::mutex mtx;
        bool isFinished;
        std::unique_ptr<chfs::RpcServer> rpc_server; // 用于接收 Worker 的 RPC 请求
        std::string outPutFile;                      // 输出文件名
        std::shared_ptr<chfs::ChfsClient> chfs_client;

        int mapTasksRemaining;    // 剩余的 Map 任务数，初始与文件数相同
        int reduceTasksRemaining; // 剩余的 Reduce 任务数，初始与 Worker 的数量相同
        int finishedMapTasks;     // 已完成的 Map 任务数
        int finishedReduceTasks;  // 已完成的 Reduce 任务数
        int mapTaskNum;           // Map 任务的数量
        int reduceTaskNum;        // Reduce 任务的数量

        int workerNum; // Worker 的数量
    };

    class Worker
    {
    public:
        explicit Worker(MR_CoordinatorConfig config);
        void doWork();
        void stop();

    private:
        void doMap(int index, const std::string &filename);
        void doReduce(int mapTaskCount, int Y);
        void doSubmit(int taskType);

        std::string outPutFile;
        std::unique_ptr<chfs::RpcClient> mr_client;
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::unique_ptr<std::thread> work_thread;
        bool shouldStop = false;
    };
}