#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <chrono>
#include <thread>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include "map_reduce/protocol.h"

namespace mapReduce
{
    /**
     * The basic loop of one worker is the following:
     * 1.ask one task (Map or Reduce) from the coordinator, do the task and write the intermediate key-value into a file
     * 2.then submit the task to the coordinator in order to hint a completion.
     */
    void Worker::doWork()
    {
        std::cout << "Worker: Start working..." << std::endl;
        while (!shouldStop)
        {
            std::cout << "Worker: Ask task..." << std::endl;
            // Lab4: Your code goes here.
            auto res_ask = mr_client->call(ASK_TASK);
            auto taskArgs = res_ask.unwrap()->as<TaskArgs>();
            std::cout << "Worker: Get task " << taskArgs.taskType << " " << taskArgs.fileIndex << " " << taskArgs.fileName << std::endl;
            if(taskArgs.taskType == NONE)
            {
                std::cout << "Worker: No task, sleep 10ms" << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            if (taskArgs.taskType == MAP)
            {
                doMap(taskArgs.fileIndex, taskArgs.fileName);
            }
            else if (taskArgs.taskType == REDUCE)
            {
                doReduce(std::stoi(taskArgs.fileName), taskArgs.fileIndex);
            }
        }
    }

    /**
     * A reasonable naming convention for intermediate files is mr-X-Y,
     * where X is the Map task number, and Y is the reduce task number.
     * The worker's map task code will need a way to store intermediate key/value pairs in files in a way
     * that can be correctly read back during reduce tasks.
     */

    /**
     * 根据文件名读取文件内容，记录单词出现次数，将结果写入四个不同的文件
     * @param mapTaskIndex Map 任务编号
     * @param filename 文件名
     */
    void Worker::doMap(int mapTaskIndex, const std::string &filename)
    {
        // Lab4: Your code goes here.
        // 读取文件内容
        auto res_lookup = chfs_client->lookup(1, filename);
        auto inode_id = res_lookup.unwrap();
        auto res_type = chfs_client->get_type_attr(inode_id);
        auto length = res_type.unwrap().second.size;
        auto res_read = chfs_client->read_file(inode_id, 0, length);
        auto char_vec = res_read.unwrap();
        std::string content(char_vec.begin(), char_vec.end());

        // 统计单词出现次数
        std::map<std::string, int> wordCount;
        std::string word;
        for (auto c : content)
        {
            // 只要 c 不是字母，则将之前的所有字母的组合视为 word
            if (!isalpha(c))
            {
                if (!word.empty())
                {
                    // 查找单词是否已经在map中
                    std::map<std::string, int>::iterator it = wordCount.find(word);
                    // 如果单词已经在map中，则增加其计数
                    if (it != wordCount.end())
                    {
                        it->second++;
                    }
                    else
                    {
                        // 否则，将单词插入map，并设置计数为1
                        wordCount.insert(std::make_pair(word, 1));
                    }

                    word.clear();
                }
            }
            else
            {
                word.push_back(c);
            }
        }

        /**
         * 将结果写入四个不同的文件
         * 文件名为 mr-X-Y，其中 X 为文件编号，Y 为 0~3
         * 文件内容为单词和其出现次数，以空格分隔，每个单词一行
         * 字母 a-g 和 A-G 开头的单词写入 mr-X-0
         * 字母 h-n 和 H-N 开头的单词写入 mr-X-1
         * 字母 o-t 和 O-T 开头的单词写入 mr-X-2
         * 字母 u-z 和 U-Z 开头的单词写入 mr-X-3
         */
        std::string content0, content1, content2, content3;
        for (auto it = wordCount.begin(); it != wordCount.end(); it++)
        {
            std::string word = it->first;
            int count = it->second;
            if ((word[0] >= 'a' && word[0] <= 'g') || (word[0] >= 'A' && word[0] <= 'G'))
            {
                content0 += word + " " + std::to_string(count) + "\n";
            }
            else if ((word[0] >= 'h' && word[0] <= 'n') || (word[0] >= 'H' && word[0] <= 'N'))
            {
                content1 += word + " " + std::to_string(count) + "\n";
            }
            else if ((word[0] >= 'o' && word[0] <= 't') || (word[0] >= 'O' && word[0] <= 'T'))
            {
                content2 += word + " " + std::to_string(count) + "\n";
            }
            else if ((word[0] >= 'u' && word[0] <= 'z') || (word[0] >= 'U' && word[0] <= 'Z'))
            {
                content3 += word + " " + std::to_string(count) + "\n";
            }
        }
        std::vector<chfs::u8> content_vec0(content0.begin(), content0.end());
        std::vector<chfs::u8> content_vec1(content1.begin(), content1.end());
        std::vector<chfs::u8> content_vec2(content2.begin(), content2.end());
        std::vector<chfs::u8> content_vec3(content3.begin(), content3.end());
        auto output_inode_id0 = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(mapTaskIndex) + "-0").unwrap();
        auto output_inode_id1 = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(mapTaskIndex) + "-1").unwrap();
        auto output_inode_id2 = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(mapTaskIndex) + "-2").unwrap();
        auto output_inode_id3 = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(mapTaskIndex) + "-3").unwrap();
        chfs_client->write_file(output_inode_id0, 0, content_vec0);
        chfs_client->write_file(output_inode_id1, 0, content_vec1);
        chfs_client->write_file(output_inode_id2, 0, content_vec2);
        chfs_client->write_file(output_inode_id3, 0, content_vec3);

        // 提交任务
        std::cout << "Worker: Submit task file " << mapTaskIndex << " " << filename << std::endl;
        doSubmit(MAP);
    }

    /**
     * 读取中间文件内容，将相同单词的出现次数相加，将结果写入文件
     * @param mapTaskCount Map 任务数量
     * @param Y mr-X-Y 中的 Y
     */
    void Worker::doReduce(int mapTaskCount, int Y)
    {
        // Lab4: Your code goes here.
        // 读取中间文件内容
        std::unordered_map<std::string, int> wordCount;
        for (int i = 0; i < mapTaskCount; i++)
        {
            std::string filename = "mr-" + std::to_string(i) + "-" + std::to_string(Y);
            auto res_lookup = chfs_client->lookup(1, filename);
            auto inode_id = res_lookup.unwrap();
            auto res_type = chfs_client->get_type_attr(inode_id);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id, 0, length);
            auto char_vec = res_read.unwrap();
            std::string content(char_vec.begin(), char_vec.end());

            // 统计单词出现次数
            std::string word;
            int count;
            std::stringstream stringstream(content);
            while (stringstream >> word >> count)
            {
                // 查找单词是否已经在map中
                std::unordered_map<std::string, int>::iterator it = wordCount.find(word);
                // 如果单词已经在map中，则增加其计数
                if (it != wordCount.end())
                {
                    it->second += count;
                }
                else
                {
                    // 否则，将单词插入map，并设置计数为1
                    wordCount.insert(std::make_pair(word, count));
                }
            }
        }

        // 将结果写入文件
        std::string content;
        for (auto it = wordCount.begin(); it != wordCount.end(); it++)
        {
            std::string word = it->first;
            int count = it->second;
            content += word + " " + std::to_string(count) + "\n";
        }
        std::vector<chfs::u8> content_vec(content.begin(), content.end());
        auto output_inode_id = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(Y)).unwrap();
        chfs_client->write_file(output_inode_id, 0, content_vec);

        // 提交任务
        doSubmit(REDUCE);
    }

    void Worker::doSubmit(int taskType)
    {
        // Lab4: Your code goes here.
        mr_client->call(SUBMIT_TASK, taskType);
    }

    void Worker::stop()
    {
        shouldStop = true;
        work_thread->join();
    }

    Worker::Worker(MR_CoordinatorConfig config)
    {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
    }
}