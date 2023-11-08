#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs
{

  ChfsClient::ChfsClient() : num_data_servers(0) {}

  auto ChfsClient::reg_server(ServerType type, const std::string &address,
                              u16 port, bool reliable) -> ChfsNullResult
  {
    switch (type)
    {
    case ServerType::DATA_SERVER:
      num_data_servers += 1;
      data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                  address, port, reliable)});
      break;
    case ServerType::METADATA_SERVER:
      metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
      break;
    default:
      std::cerr << "Unknown Type" << std::endl;
      exit(1);
    }

    return KNullOk;
  }

  // {Your code here}
  auto ChfsClient::mknode(FileType type, inode_id_t parent,
                          const std::string &name) -> ChfsResult<inode_id_t>
  {
    u8 type_u8 = static_cast<u8>(type);
    auto call_res = metadata_server_->call("mknode", type_u8, parent, name);
    auto res = call_res.unwrap()->as<inode_id_t>();
    if (res == 0)
    {
      return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
    }
    else
    {
      return ChfsResult<inode_id_t>(res);
    }
  }

  // {Your code here}
  auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
      -> ChfsNullResult
  {
    auto call_res = metadata_server_->call("unlink", parent, name);
    auto res = call_res.unwrap()->as<bool>();
    if (res)
    {
      return KNullOk;
    }
    else
    {
      return ChfsNullResult(ErrorType::NotExist);
    }
  }

  // {Your code here}
  auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
      -> ChfsResult<inode_id_t>
  {
    auto call_res = metadata_server_->call("lookup", parent, name);
    auto res = call_res.unwrap()->as<inode_id_t>();
    if (res == 0)
    {
      return ChfsResult<inode_id_t>(ErrorType::NotExist);
    }
    else
    {
      return ChfsResult<inode_id_t>(res);
    }
  }

  // {Your code here}
  auto ChfsClient::readdir(inode_id_t id)
      -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>
  {
    auto call_res = metadata_server_->call("readdir", id);
    auto res = call_res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
    if (res.size() == 0)
    {
      return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(ErrorType::NotExist);
    }
    else
    {
      return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>(res);
    }
  }

  // {Your code here}
  auto ChfsClient::get_type_attr(inode_id_t id)
      -> ChfsResult<std::pair<InodeType, FileAttr>>
  {
    auto call_res = metadata_server_->call("get_type_attr", id);
    auto res = call_res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
    if (std::get<4>(res) == 0)
    {
      return ChfsResult<std::pair<InodeType, FileAttr>>(ErrorType::NotExist);
    }
    else
    {
      FileAttr file_attr;
      file_attr.size = std::get<0>(res);
      file_attr.atime = std::get<1>(res);
      file_attr.mtime = std::get<2>(res);
      file_attr.ctime = std::get<3>(res);
      return ChfsResult<std::pair<InodeType, FileAttr>>(std::make_pair(static_cast<InodeType>(std::get<4>(res)), file_attr));
    }
  }

  /**
   * Read and Write operations are more complicated.
   */
  // {Your code here}
  auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
      -> ChfsResult<std::vector<u8>>
  {
    // 从 metadata server 得到 block map
    auto call_res = metadata_server_->call("get_block_map", id);
    auto res = call_res.unwrap()->as<std::vector<BlockInfo>>();
    if (res.size() == 0)
    {
      return ChfsResult<std::vector<u8>>(ErrorType::NotExist);
    }
    else
    {

      auto read_block_num = offset / 4096;    // 计算从第几个 block 开始读取
      auto read_block_offset = offset % 4096; // 计算从 block 的第几个字节开始读取
      std::vector<u8> result(size);           // 保存读取的数据
      usize read_size = 0;                    // 已经读取的数据大小

      // 遍历 block map，从 data server 读取数据
      for (auto block_info : res)
      {
        // 跳过 read_block_num 个 block
        if (read_block_num > 0)
        {
          read_block_num--;
          continue;
        }

        block_id_t block_id = std::get<0>(block_info);
        mac_id_t mac_id = std::get<1>(block_info);
        version_t version = std::get<2>(block_info);
        // 遍历 data_servers_ 以找到 mac_id 对应的 data server，从 data server 读取数据
        for (auto client : data_servers_)
        {
          if (client.first == mac_id)
          {
            if (read_size == size)
            {
              return ChfsResult<std::vector<u8>>(result);
            }

            // 如果不是从第一个 block 开始读取，将 offset 置为 0
            if (read_size != 0)
            {
              read_block_offset = 0;
            }
            // 计算本次需要读取的数据大小
            auto read_size_this = std::min(size - read_size, 4096 - read_block_offset);

            // 从 data server 读取数据
            auto call_res = client.second->call("read_data", block_id, read_block_offset, read_size_this, version);
            auto res = call_res.unwrap()->as<std::vector<u8>>();
            if (res.size() == 0)
            {
              return ChfsResult<std::vector<u8>>(ErrorType::NotExist);
            }
            else
            {
              result.insert(result.end(), res.begin(), res.end());
            }
            read_size += read_size_this;
          }
        }
      }
      return ChfsResult<std::vector<u8>>(result);
    }
  }

  // {Your code here}
  auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
      -> ChfsNullResult
  {
    // Get the block map of the file from metadata server
    auto call_res = metadata_server_->call("get_block_map", id);
    auto res = call_res.unwrap()->as<std::vector<BlockInfo>>();

    auto write_block_num = offset / 4096;    // 从第几个 block 开始写入
    auto write_block_offset = offset % 4096; // 从 block 的第几个字节开始写入
    usize write_size = 0;                    // 已经写入的数据大小
    usize size = data.size();                // 需要写入的数据大小

    // 首先在已有的 block 上写入数据
    for (auto block_info : res)
    {
      // 跳过 write_block_num 个 block
      if (write_block_num > 0)
      {
        write_block_num--;
        continue;
      }

      // 得到需要写入的 block 的信息
      block_id_t block_id = std::get<0>(block_info);
      mac_id_t mac_id = std::get<1>(block_info);

      // 遍历 data_servers_ 以找到 mac_id 对应的 data server，向 data server 写入数据
      for (auto client : data_servers_)
      {
        if (client.first == mac_id)
        {
          if (write_size == size)
          {
            return KNullOk;
          }

          // 如果不是从第一个 block 开始写入，将 offset 置为 0
          if (write_size != 0)
          {
            write_block_offset = 0;
          }
          // 计算本次需要写入的数据大小
          auto write_size_this = std::min(size - write_size, 4096 - write_block_offset);
          // 得到本次需要写入的数据
          std::vector<u8> write_data(data.begin() + write_size, data.begin() + write_size + write_size_this);

          // 向 data server 写入数据
          auto call_res = client.second->call("write_data", block_id, write_block_offset, write_data);
          auto res = call_res.unwrap()->as<bool>();
          if (!res)
          {
            return ChfsNullResult(ErrorType::NotExist);
          }
          write_size += write_size_this;
        }
      }
    }

    // 如果还有数据没有写入，需要分配新的 block
    if (write_size < size)
    {
      // 计算还需要多少个 block
      auto block_num = (size - write_size) / 4096;
      if ((size - write_size) % 4096 != 0)
      {
        block_num++;
      }

      // 分配新的 block
      for (int i = 0; i < block_num; i++)
      {
        auto call_res = metadata_server_->call("allocate_block", id);
        auto res = call_res.unwrap()->as<BlockInfo>();

        // 得到新分配的 block 的信息
        block_id_t block_id = std::get<0>(res);
        mac_id_t mac_id = std::get<1>(res);

        // 遍历 data_servers_ 以找到 mac_id 对应的 data server，向 data server 写入数据
        for (auto client : data_servers_)
        {
          if (client.first == mac_id)
          {
            if (write_size == size)
            {
              return KNullOk;
            }

            // 如果不是从第一个 block 开始写入，将 offset 置为 0
            if (write_size != 0)
            {
              write_block_offset = 0;
            }
            // 计算本次需要写入的数据大小
            auto write_size_this = std::min(size - write_size, 4096 - write_block_offset);
            // 得到本次需要写入的数据
            std::vector<u8> write_data(data.begin() + write_size, data.begin() + write_size + write_size_this);

            // 向 data server 写入数据
            auto call_res = client.second->call("write_data", block_id, write_block_offset, write_data);
            auto res = call_res.unwrap()->as<bool>();
            if (!res)
            {
              return ChfsNullResult(ErrorType::NotExist);
            }
            write_size += write_size_this;
          }
        }
      }
    }

    return KNullOk;
  }

  // {Your code here}
  auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                   mac_id_t mac_id) -> ChfsNullResult
  {
    // 释放 mac_id 对应的 data server 上的 block
    for (auto client : data_servers_)
    {
      if (client.first == mac_id)
      {
        auto call_res = client.second->call("free_block", block_id);
        auto res = call_res.unwrap()->as<bool>();
        if (!res)
        {
          return ChfsNullResult(ErrorType::NotExist);
        }
      }
    }

    // 从 metadata server 中删除 block map 中的 block
    auto call_res = metadata_server_->call("free_block", id, block_id, mac_id);
    auto res = call_res.unwrap()->as<bool>();
    if (!res)
    {
      return ChfsNullResult(ErrorType::NotExist);
    }

    return KNullOk;
  }

} // namespace chfs