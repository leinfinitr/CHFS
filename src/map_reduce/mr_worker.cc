#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

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

    /**
     * A reasonable naming convention for intermediate files is mr-X-Y, 
     * where X is the Map task number, and Y is the reduce task number. 
     * The worker's map task code will need a way to store intermediate key/value pairs in files in a way 
     * that can be correctly read back during reduce tasks.
     */

    Worker::Worker(MR_CoordinatorConfig config)
    {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename)
    {
        // Lab4: Your code goes here.
    }

    void Worker::doReduce(int index, int nfiles)
    {
        // Lab4: Your code goes here.
    }

    void Worker::doSubmit(mr_tasktype taskType, int index)
    {
        // Lab4: Your code goes here.
    }

    void Worker::stop()
    {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork()
    {
        while (!shouldStop)
        {
            // Lab4: Your code goes here.
        }
    }
}