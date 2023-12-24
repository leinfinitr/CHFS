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
        // 读取所有文件内容
        std::string contents;
        // std::cout << "SequentialMapReduce doMap: Reading files..." << std::endl;
        for (auto file : files)
        {
            auto res_lookup = chfs_client->lookup(1, file);
            auto inode_id = res_lookup.unwrap();
            auto res_type = chfs_client->get_type_attr(inode_id);
            auto length = res_type.unwrap().second.size;
            auto res_read = chfs_client->read_file(inode_id, 0, length);
            auto char_vec = res_read.unwrap();
            std::string content(char_vec.begin(), char_vec.end());
            contents += content;
        }

        // 调用 Map 函数
        std::vector<KeyVal> kvs = Map(contents);

        // 将结果写入文件
        // std::cout << "SequentialMapReduce doMap: Writing files..." << std::endl;
        std::string content;
        for (auto kv : kvs)
        {
            content += kv.key + " " + kv.val + "\n";
        }
        std::vector<chfs::u8> content_vec(content.begin(), content.end());
        auto output_inode_id = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "mr-0").unwrap();
        chfs_client->write_file(output_inode_id, 0, content_vec);
    }

    /**
     * 读取中间文件内容，对其进行排序，调用 Reduce 函数，将结果写入文件
     */
    void SequentialMapReduce::doReduce()
    {
        // 读取中间文件内容
        // std::cout << "SequentialMapReduce doReduce: Reading intermediate files..." << std::endl;
        auto res_lookup = chfs_client->lookup(1, "mr-0");
        auto inode_id = res_lookup.unwrap();
        auto res_type = chfs_client->get_type_attr(inode_id);
        auto length = res_type.unwrap().second.size;
        auto res_read = chfs_client->read_file(inode_id, 0, length);
        auto char_vec = res_read.unwrap();
        std::string content(char_vec.begin(), char_vec.end());
        // 删除 content 中的空字符
        content.erase(std::remove(content.begin(), content.end(), '\0'), content.end());
       
        // 统计单词出现次数
        std::unordered_map<std::string, std::vector<std::string>> wordCount;
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

        // 调用 Reduce 函数
        std::string result;
        for (auto it = wordCount.begin(); it != wordCount.end(); it++)
        {
            std::string word = it->first;
            std::vector<std::string> counts = it->second;
            std::string count = Reduce(word, counts);
            result += word + " " + count + "\n";
        }
        
        
        // std::cout << "SequentialMapReduce doReduce: Writing files..." << std::endl;
        std::vector<chfs::u8> content_vec(result.begin(), result.end());
        auto output_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(output_inode_id, 0, content_vec);
    }
}