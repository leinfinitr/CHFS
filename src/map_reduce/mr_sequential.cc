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
        // 读取所有文件内容
        std::string contents;
        std::cout << "SequentialMapReduce: Reading files..." << std::endl;
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
        std::cout << "SequentialMapReduce: Writing files..." << std::endl;
        std::string content;
        for (auto kv : kvs)
        {
            content += kv.key + " " + kv.val + "\n";
        }
        std::vector<chfs::u8> content_vec(content.begin(), content.end());
        auto output_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
        chfs_client->write_file(output_inode_id, 0, content_vec);
    }
}