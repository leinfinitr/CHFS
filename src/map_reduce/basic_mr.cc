#include <string>
#include <fstream>
#include <iostream>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce
{
    //
    // The map function is called once for each file of input. The first
    // argument is the name of the input file, and the second is the
    // file's complete contents. You should ignore the input file name,
    // and look only at the contents argument. The return value is a slice
    // of key/value pairs.
    //

    /**
     * Word Count 的 Map 函数，输入为文件内容，输出为单词及其出现次数
     * @param content 文件内容
     * @return std::vector<KeyVal>，key 为单词，value 为出现次数
     */
    std::vector<KeyVal> Map(const std::string &content)
    {
        // Your code goes here
        // Hints: split contents into an array of words.

        // 将 content 按照分割为单词
        std::vector<std::string> words;
        std::string word;
        // std::cout << "Map: Splitting words..." << std::endl;
        for (auto c : content)
        {
            // 只要 c 不是字母，则将之前的所有字母的组合视为 word 放入 words 中
            if (!isalpha(c))
            {
                if (!word.empty())
                {
                    words.push_back(word);
                    word.clear();
                }
            }
            else
            {
                word.push_back(c);
            }
        }

        // 统计单词出现次数
        // std::cout << "Map: Counting words..." << std::endl;
        std::map<std::string, int> wordCount;
        for (auto word : words)
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
        }

        // 将 map 转换为 vector
        std::vector<std::pair<std::string, int>> result(wordCount.begin(), wordCount.end());

        // 将 vector 转换为 KeyVal
        std::vector<KeyVal> ret;
        for (auto it = result.begin(); it != result.end(); it++)
        {
            ret.push_back(KeyVal(it->first, std::to_string(it->second)));
        }

        return ret;
    }

    //
    // The reduce function is called once for each key generated by the
    // map tasks, with a list of all the values created for that key by
    // any map task.
    //
    std::string Reduce(const std::string &key, const std::vector<std::string> &values)
    {
        // Your code goes here
        // Hints: return the number of occurrences of the word.
        std::string count = "0";
        for (auto value : values)
        {
            count = std::to_string(std::stoi(count) + std::stoi(value));
        }
        return count;
    }
}