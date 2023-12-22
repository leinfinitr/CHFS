#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>
#include <thread>
#include <chrono>

#include "map_reduce/protocol.h"

namespace mapReduce
{
    /**
     * The basic loop of the coordinator is the following:
     * 1.assign the Map tasks first;
     * 2.when all Map tasks are done, then assign the Reduce tasks;
     * 3.when all Reduce tasks are done, the Done() loop returns true indicating that all tasks are completely finished.
     */
    void Coordinator::run()
    {
        // Lab4: Your code goes here.
        while (!Done())
        {
            
        }
    }

    /**
     * 当收到一个 worker 的 ask_task 请求时，返回一个任务
     * 如果返回的是 MAP 任务，则返回文件名
     * 如果返回的是 REDUCE 任务，则返回中间文件名 mr-X-Y 的 Y
     */
    TaskArgs Coordinator::askTask()
    {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
        std::unique_lock<std::mutex> lock(mtx);
        if (mapTasksRemaining > 0)
        {
            int fileIndex = files.size() - mapTasksRemaining;
            std::string fileName = files[fileIndex];
            return TaskArgs{MAP, fileIndex, fileName};
        }
        else if (mapTasksRemaining == 0 && reduceTasksRemaining > 0)
        {
            int reduceIndex = workerNum - reduceTasksRemaining;
            return TaskArgs{REDUCE, reduceIndex, std::to_string(files.size())};
        }
        return TaskArgs{NONE, -1, ""};
    }

    int Coordinator::submitTask(int taskType)
    {
        // Lab4 : Your code goes here.
        std::unique_lock<std::mutex> lock(mtx);
        if (taskType == MAP)
        {
            mapTasksRemaining--;
        }
        else if (taskType == REDUCE)
        {
            reduceTasksRemaining--;
        }
        if (mapTasksRemaining == 0 && reduceTasksRemaining == 0)
        {
            isFinished = true;
        }
        return 0;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done()
    {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int workerNum)
    {
        this->files = files;
        this->isFinished = false;
        this->mapTasksRemaining = files.size();
        this->reduceTasksRemaining = workerNum;
        this->workerNum = workerNum;

        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this]()
                         { return this->askTask(); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType)
                         { return this->submitTask(taskType); });
        rpc_server->run(true, 1);
    }
}