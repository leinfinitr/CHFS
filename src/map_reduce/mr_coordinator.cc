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

    /**
     * 当收到一个 worker 的 ask_task 请求时，返回一个任务
     * 如果返回的是 MAP 任务，则返回文件名
     * 如果返回的是 REDUCE 任务，则返回中间文件名 mr-X-Y 的 Y
     */
    TaskArgs Coordinator::askTask()
    {
        // Lab4 : Your code goes here.
        std::cout << "Coordinator: Ask task..." << std::endl;
        // Free to change the type of return value.
        std::unique_lock<std::mutex> lock(mtx);
        if (mapTasksRemaining > 0)
        {
            int fileIndex = mapTaskNum - mapTasksRemaining;
            std::string fileName = files[fileIndex];
            mapTasksRemaining--;
            std::cout << "Coordinator: Assign map task " << fileIndex << " " << fileName << std::endl;
            return TaskArgs{MAP, fileIndex, fileName};
        }
        else if (finishedMapTasks == mapTaskNum && reduceTasksRemaining > 0)
        {
            int reduceIndex = reduceTaskNum - reduceTasksRemaining;
            reduceTasksRemaining--;
            std::cout << "Coordinator: Assign reduce task " << reduceIndex << std::endl;
            return TaskArgs{REDUCE, reduceIndex, std::to_string(mapTaskNum)};
        }
        return TaskArgs{NONE, -1, ""};
    }

    int Coordinator::submitTask(int taskType)
    {
        // Lab4 : Your code goes here.
        std::cout << "Coordinator: Submit task..." << std::endl;
        std::unique_lock<std::mutex> lock(mtx);
        if (taskType == MAP)
        {
            finishedMapTasks++;
        }
        else if (taskType == REDUCE)
        {
            finishedReduceTasks++;
        }
        if (finishedMapTasks == mapTaskNum && finishedReduceTasks == reduceTaskNum)
        {
            // 将中间文件合并
            std::cout << "Coordinator: Merge intermediate files..." << std::endl;
            std::string content;
            for (int i = 0; i < reduceTaskNum; i++)
            {
                std::string filename = "mr-" + std::to_string(i);
                auto res_lookup = chfs_client->lookup(1, filename);
                auto inode_id = res_lookup.unwrap();
                auto res_type = chfs_client->get_type_attr(inode_id);
                auto length = res_type.unwrap().second.size;
                auto res_read = chfs_client->read_file(inode_id, 0, length);
                auto char_vec = res_read.unwrap();
                std::string fileContent(char_vec.begin(), char_vec.end());
                content += fileContent;
            }
            auto res_create = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, outPutFile);
            auto output_inode_id = res_create.unwrap();
            std::vector<chfs::u8> content_vec(content.begin(), content.end());
            chfs_client->write_file(output_inode_id, 0, content_vec);

            std::cout << "Coordinator: Merge intermediate files done" << std::endl;

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
        this->outPutFile = config.resultFile;
        this->chfs_client = config.client;

        this->mapTasksRemaining = files.size();
        this->reduceTasksRemaining = workerNum;
        this->finishedMapTasks = 0;
        this->finishedReduceTasks = 0;
        this->mapTaskNum = files.size();
        this->reduceTaskNum = workerNum;

        this->workerNum = workerNum;

        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this]()
                         { return this->askTask(); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType)
                         { return this->submitTask(taskType); });
        rpc_server->run(true, 1);
    }
}