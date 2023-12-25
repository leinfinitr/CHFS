#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce
{
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile)
    {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork()
    {
        // Your code goes here
        doMap();
        doReduce();
    }

    /**
     * 读取所有文件内容，调用 Map 函数，将结果写入文件
     */
    void SequentialMapReduce::doMap()
    {
        // 这里提供了两种实现方式
        // 第一种和 DistributedMapReduce 逻辑保持一致
        // 第二种使用了简化的 SequentialMapReduce ，只有一个中间文件

        // -------------------- 第一种实现方式 --------------------
        for (int i = 0; i < files.size(); i++)
        {
            // 读取文件内容
            auto res_lookup = chfs_client->lookup(1, files[i]);
            auto inode_id = res_lookup.unwrap();
            auto res_type = chfs_client->get_type_attr(inode_id);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id, 0, length);
            auto char_vec = res_read.unwrap();
            std::string content(char_vec.begin(), char_vec.end());

            // 调用 Map 函数
            std::vector<KeyVal> kvs = Map(content);

            // 根据首字母 Hash 到四个不同的文件中
            std::string content0, content1, content2, content3;
            for (auto kv : kvs)
            {
                std::string key = kv.key;
                std::string value = kv.val;

                int hash = key[0] % 4;
                if (hash == 0)
                {
                    content0 += key + " " + value + "\n";
                }
                else if (hash == 1)
                {
                    content1 += key + " " + value + "\n";
                }
                else if (hash == 2)
                {
                    content2 += key + " " + value + "\n";
                }
                else if (hash == 3)
                {
                    content3 += key + " " + value + "\n";
                }
            }

            // 保存到文件中
            std::vector<chfs::u8> content_vec0(content0.begin(), content0.end());
            std::vector<chfs::u8> content_vec1(content1.begin(), content1.end());
            std::vector<chfs::u8> content_vec2(content2.begin(), content2.end());
            std::vector<chfs::u8> content_vec3(content3.begin(), content3.end());
            auto output_inode_id0 = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(i) + "-0").unwrap();
            auto output_inode_id1 = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(i) + "-1").unwrap();
            auto output_inode_id2 = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(i) + "-2").unwrap();
            auto output_inode_id3 = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-" + std::to_string(i) + "-3").unwrap();
            chfs_client->write_file(output_inode_id0, 0, content_vec0);
            chfs_client->write_file(output_inode_id1, 0, content_vec1);
            chfs_client->write_file(output_inode_id2, 0, content_vec2);
            chfs_client->write_file(output_inode_id3, 0, content_vec3);
        }
        // -------------------------------------------------------

        // -------------------- 第二种实现方式 --------------------
        // // 读取所有文件内容
        // std::string contents;
        // // std::cout << "SequentialMapReduce doMap: Reading files..." << std::endl;
        // for (auto file : files)
        // {
        //     auto res_lookup = chfs_client->lookup(1, file);
        //     auto inode_id = res_lookup.unwrap();
        //     auto res_type = chfs_client->get_type_attr(inode_id);
        //     auto length = res_type.unwrap().second.size;
        //     auto res_read = chfs_client->read_file(inode_id, 0, length);
        //     auto char_vec = res_read.unwrap();
        //     std::string content(char_vec.begin(), char_vec.end());
        //     contents += content;
        // }

        // // 调用 Map 函数
        // std::vector<KeyVal> kvs = Map(contents);

        // // 将结果写入文件
        // std::cout << "SequentialMapReduce doMap: Writing files..." << std::endl;
        // std::string content;
        // for (auto kv : kvs)
        // {
        //     content += kv.key + " " + kv.val + "\n";
        // }
        // std::vector<chfs::u8> content_vec(content.begin(), content.end());
        // auto output_inode_id = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-0").unwrap();
        // chfs_client->write_file(output_inode_id, 0, content_vec);
        // -------------------------------------------------------
    }

    /**
     * 读取中间文件内容，对其进行排序，调用 Reduce 函数，将结果写入文件
     */
    void SequentialMapReduce::doReduce()
    {
        // -------------------- 第一种实现方式 --------------------
        std::unordered_map<std::string, std::vector<std::string>> wordCount;

        for (int i = 0; i < files.size(); i++)
        {
            // 读取中间文件内容
            for (int j = 0; j < 4; j++)
            {
                std::string filename = "mr-" + std::to_string(i) + "-" + std::to_string(j);
                auto res_lookup = chfs_client->lookup(1, filename);
                auto inode_id = res_lookup.unwrap();
                auto res_type = chfs_client->get_type_attr(inode_id);
                auto length = res_type.unwrap().second.size;
                auto res_read = chfs_client->read_file(inode_id, 0, length);
                auto char_vec = res_read.unwrap();
                std::string content(char_vec.begin(), char_vec.end());
                // 删除 content 中的空字符
                content.erase(std::remove(content.begin(), content.end(), '\0'), content.end());

                // 统计单词出现次数
                std::string word, count;
                std::stringstream stringstream(content);
                while (stringstream >> word >> count)
                {
                    // 查找单词是否已经在map中
                    std::unordered_map<std::string, std::vector<std::string>>::iterator it = wordCount.find(word);
                    // 如果单词已经在map中，则增加其计数
                    if (it != wordCount.end())
                    {
                        it->second.push_back(count);
                    }
                    else
                    {
                        // 否则，将单词插入map
                        wordCount.insert(std::make_pair(word, std::vector<std::string>{count}));
                    }
                }
            }
        }
        // -------------------------------------------------------

        // -------------------- 第二种实现方式 --------------------
        // 读取中间文件内容
        // std::cout << "SequentialMapReduce doReduce: Reading intermediate files..." << std::endl;
        // auto res_lookup = chfs_client->lookup(1, "mr-0");
        // auto inode_id = res_lookup.unwrap();
        // auto res_type = chfs_client->get_type_attr(inode_id);
        // auto length = res_type.unwrap().second.size;
        // auto res_read = chfs_client->read_file(inode_id, 0, length);
        // auto char_vec = res_read.unwrap();
        // std::string content(char_vec.begin(), char_vec.end());
        // // 删除 content 中的空字符
        // content.erase(std::remove(content.begin(), content.end(), '\0'), content.end());

        // // 统计单词出现次数
        // std::unordered_map<std::string, std::vector<std::string>> wordCount;
        // std::string word, count;
        // std::stringstream stringstream(content);
        // while (stringstream >> word >> count)
        // {
        //     // 查找单词是否已经在map中
        //     std::unordered_map<std::string, std::vector<std::string>>::iterator it = wordCount.find(word);
        //     // 如果单词已经在map中，则增加其计数
        //     if (it != wordCount.end())
        //     {
        //         it->second.push_back(count);
        //     }
        //     else
        //     {
        //         // 否则，将单词插入map
        //         wordCount.insert(std::make_pair(word, std::vector<std::string>{count}));
        //     }
        // }
        // -------------------------------------------------------

        // 调用 Reduce 函数
        std::string result;
        for (auto it = wordCount.begin(); it != wordCount.end(); it++)
        {
            std::string word = it->first;
            std::vector<std::string> counts = it->second;
            std::string count = Reduce(word, counts);
            result += word + " " + count + "\n";
        }

        // 将结果写入文件
        // std::cout << "SequentialMapReduce doReduce: Writing files..." << std::endl;
        std::vector<chfs::u8> content_vec(result.begin(), result.end());
        auto output_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(output_inode_id, 0, content_vec);
    }
}