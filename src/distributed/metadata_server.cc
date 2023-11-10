#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs
{

  inline auto MetadataServer::bind_handlers()
  {
    server_->bind("mknode",
                  [this](u8 type, inode_id_t parent, std::string const &name)
                  {
                    return this->mknode(type, parent, name);
                  });
    server_->bind("unlink", [this](inode_id_t parent, std::string const &name)
                  { return this->unlink(parent, name); });
    server_->bind("lookup", [this](inode_id_t parent, std::string const &name)
                  { return this->lookup(parent, name); });
    server_->bind("get_block_map",
                  [this](inode_id_t id)
                  { return this->get_block_map(id); });
    server_->bind("alloc_block",
                  [this](inode_id_t id)
                  { return this->allocate_block(id); });
    server_->bind("free_block",
                  [this](inode_id_t id, block_id_t block, mac_id_t machine_id)
                  {
                    return this->free_block(id, block, machine_id);
                  });
    server_->bind("readdir", [this](inode_id_t id)
                  { return this->readdir(id); });
    server_->bind("get_type_attr",
                  [this](inode_id_t id)
                  { return this->get_type_attr(id); });
  }

  inline auto MetadataServer::init_fs(const std::string &data_path)
  {
    /**
     * Check whether the metadata exists or not.
     * If exists, we wouldn't create one from scratch.
     */
    bool is_initialed = is_file_exist(data_path);

    auto block_manager = std::shared_ptr<BlockManager>(nullptr);
    if (is_log_enabled_)
    {
      block_manager =
          std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
    }
    else
    {
      block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
    }

    CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

    if (is_initialed)
    {
      auto origin_res = FileOperation::create_from_raw(block_manager);
      std::cout << "Restarting..." << std::endl;
      if (origin_res.is_err())
      {
        std::cerr << "Original FS is bad, please remove files manually."
                  << std::endl;
        exit(1);
      }

      operation_ = origin_res.unwrap();
    }
    else
    {
      operation_ = std::make_shared<FileOperation>(block_manager,
                                                   DistributedMaxInodeSupported);
      std::cout << "We should init one new FS..." << std::endl;
      /**
       * If the filesystem on metadata server is not initialized, create
       * a root directory.
       */
      auto init_res = operation_->alloc_inode(InodeType::Directory);
      if (init_res.is_err())
      {
        std::cerr << "Cannot allocate inode for root directory." << std::endl;
        exit(1);
      }

      CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
    }

    running = false;
    num_data_servers =
        0; // Default no data server. Need to call `reg_server` to add.

    if (is_log_enabled_)
    {
      if (may_failed_)
        operation_->block_manager_->set_may_fail(true);
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled_);
    }

    bind_handlers();

    /**
     * The metadata server wouldn't start immediately after construction.
     * It should be launched after all the data servers are registered.
     */
  }

  MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                                 bool is_log_enabled, bool is_checkpoint_enabled,
                                 bool may_failed)
      : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
        is_checkpoint_enabled_(is_checkpoint_enabled)
  {
    server_ = std::make_unique<RpcServer>(port);
    init_fs(data_path);
    if (is_log_enabled_)
    {
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled);
    }
  }

  MetadataServer::MetadataServer(std::string const &address, u16 port,
                                 const std::string &data_path,
                                 bool is_log_enabled, bool is_checkpoint_enabled,
                                 bool may_failed)
      : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
        is_checkpoint_enabled_(is_checkpoint_enabled)
  {
    server_ = std::make_unique<RpcServer>(address, port);
    init_fs(data_path);
    if (is_log_enabled_)
    {
      commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                               is_checkpoint_enabled);
    }
  }

  // {Your code here}
  auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
      -> inode_id_t
  {
    std::lock_guard<std::mutex> lock(mknode_mutex);
    // allocate inode
    std::cout << "mk_helper" << std::endl;
    auto allo_res = operation_->mk_helper(parent, name.c_str(), InodeType(type));

    // write log
    if(is_log_enabled_) {
      std::cout << "\nwriting log ..." << std::endl;

      std::vector<std::shared_ptr<BlockOperation>> ops;
      for(auto log : operation_->block_manager_->log_buffer) {
        auto op = std::make_shared<BlockOperation>(log.first, log.second);
        ops.push_back(op);
      }

      std::cout << "log_buffer size: " << operation_->block_manager_->log_buffer.size() << std::endl;
      std::cout << "log_buffer: " << std::endl;
      for(auto log : operation_->block_manager_->log_buffer) {
        std::cout << log.first << " " << log.second.size() << std::endl;
      }
      std::cout << "ops size: " << ops.size() << std::endl;
      std::cout << "ops: " << std::endl;
      for(auto op : ops) {
        std::cout << op->block_id_ << " " << op->new_block_state_.size() << std::endl;
      }
      
      commit_log->append_log(operation_->block_manager_->last_txn_id, ops);
      operation_->block_manager_->log_buffer.clear();
      operation_->block_manager_->last_txn_id += 1;

      std::cout << "commit_log\n" << std::endl;
    }

    if (allo_res.is_err())
    {
      return 0;
    }
    auto inode = allo_res.unwrap();
    std::cout << "inode: " << inode << std::endl;

    return inode;
  }

  // {Your code here}
  auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
      -> bool
  {
    std::lock_guard<std::mutex> lock(mknode_mutex);
    auto res = operation_->unlink(parent, name.c_str());

    // write log
    if(is_log_enabled_) {
      std::vector<std::shared_ptr<BlockOperation>> ops;
      for(auto log : operation_->block_manager_->log_buffer) {
        auto op = std::make_shared<BlockOperation>(log.first, log.second);
        ops.push_back(op);
      }
      commit_log->append_log(operation_->block_manager_->last_txn_id, ops);
      operation_->block_manager_->log_buffer.clear();
      operation_->block_manager_->last_txn_id += 1;

      std::cout << "commit_log" << std::endl;
    }

    if (res.is_err())
    {
      return false;
    }

    return true;
  }

  // {Your code here}
  auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
      -> inode_id_t
  {
    auto res = operation_->lookup(parent, name.c_str());
    if (res.is_ok())
    {
      return res.unwrap();
    }

    return 0;
  }

  // {Your code here}
  auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo>
  {
    // Read the inode
    auto inode_block_id = operation_->inode_manager_->get(id).unwrap();
    chfs::u8 *data = new chfs::u8[operation_->block_manager_->block_size()];
    auto read_res = operation_->block_manager_->read_block(inode_block_id, data);
    if (read_res.is_err())
    {
      return {};
    }
    auto inode = reinterpret_cast<Inode *>(data);

    // Get block info
    std::vector<BlockInfo> res;
    for (auto i = 0; i < inode->get_nblocks(); i++)
    {
      // 跳过 block_id 为 0 的 block
      if (inode->blocks[i] == 0)
      {
        continue;
      }
      block_attr attr = inode->block_attrs[i];
      res.push_back(BlockInfo{inode->blocks[i], attr.mac_id, attr.version});
    }

    delete[] data;

    return res;
  }

  // {Your code here}
  auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo
  {
    std::lock_guard<std::mutex> lock(allo_mutex);
    // 读取 inode
    auto inode_block_id = operation_->inode_manager_->get(id).unwrap();
    u8 *data = new chfs::u8[operation_->block_manager_->block_size()];
    auto read_res = operation_->block_manager_->read_block(inode_block_id, data);
    if (read_res.is_err())
    {
      return {};
    }
    auto inode = reinterpret_cast<Inode *>(data);

    // 遍历所有的 data server，直至 allocate 成功
    bool success = false;
    auto res = BlockInfo{0, 0, 0};
    for (auto client : clients_)
    {
      auto cli = client.second;
      auto allo_res = cli->call("alloc_block");
      auto [allo_block_id, version] = allo_res.unwrap()->as<std::pair<block_id_t, version_t>>();
      if (allo_block_id != 0)
      {
        // 计算 block_id 在 inode 中的位置
        auto block_id_pos = inode->get_size() / operation_->block_manager_->block_size();
        if (inode->get_size() % operation_->block_manager_->block_size() != 0)
        {
          block_id_pos += 1;
        }
        if (block_id_pos >= inode->get_nblocks())
        {
          // inode 中的 block 不够用了，需要分配新的 block
          std::cout << "The inode is full, need to allocate new block." << std::endl;
          return {};
        }

        // 更新 inode block
        inode->blocks[block_id_pos] = allo_block_id;
        inode->block_attrs[block_id_pos] = block_attr{client.first, version};
        inode->inner_attr.size += operation_->block_manager_->block_size();

        // 将 inode block 写回
        auto write_res = operation_->block_manager_->write_block(inode_block_id, data);
        if (write_res.is_err())
        {
          return {};
        }
        success = true;
        res = BlockInfo{allo_block_id, client.first, version};
        break;
      }
    }

    // free data
    delete[] data;

    if (success)
    {
      return res;
    }

    return {};
  }

  // {Your code here}
  auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                  mac_id_t machine_id) -> bool
  {
    std::lock_guard<std::mutex> lock(allo_mutex);
    // 读取 inode
    auto inode_block_id = operation_->inode_manager_->get(id).unwrap();
    chfs::u8 *data = new chfs::u8[operation_->block_manager_->block_size()];
    auto read_res = operation_->block_manager_->read_block(inode_block_id, data);
    if (read_res.is_err())
    {
      return {};
    }
    auto inode = reinterpret_cast<Inode *>(data);

    bool success = false;
    auto cli = clients_.find(machine_id)->second;
    auto res = cli->call("free_block", block_id);
    if (res.unwrap()->as<bool>())
    {
      // 更新 inode
      for (auto i = 0; i < inode->get_nblocks(); i++)
      {
        if (inode->blocks[i] == block_id)
        {
          inode->blocks[i] = 0;
          inode->block_attrs[i] = block_attr{0, 0};
          inode->inner_attr.size -= operation_->block_manager_->block_size();
          std::cout << "free block: " << block_id << std::endl;
          break;
        }
      }
      auto write_res = operation_->block_manager_->write_block(block_id, data);
      if (write_res.is_err())
      {
        return {};
      }
      success = true;
      std::cout << "free block success" << std::endl;
    }

    delete[] data;

    return success;
  }

  // {Your code here}
  auto MetadataServer::readdir(inode_id_t node)
      -> std::vector<std::pair<std::string, inode_id_t>>
  {
    std::list<DirectoryEntry> entries;
    auto read_res = read_directory(operation_.get(), node, entries);
    if (read_res.is_err())
    {
      return {};
    }

    std::vector<std::pair<std::string, inode_id_t>> res;
    for (auto entry : entries)
    {
      res.push_back(std::make_pair(entry.name, entry.id));
    }
    return res;
  }

  // {Your code here}
  auto MetadataServer::get_type_attr(inode_id_t id)
      -> std::tuple<u64, u64, u64, u64, u8>
  {
    auto res = operation_->get_type_attr(id);
    if (res.is_err())
    {
      return {};
    }

    std::pair<InodeType, FileAttr> attr = res.unwrap();
    return std::make_tuple(attr.second.size, attr.second.atime, attr.second.mtime,
                           attr.second.ctime, static_cast<u8>(attr.first));
  }

  auto MetadataServer::reg_server(const std::string &address, u16 port,
                                  bool reliable) -> bool
  {
    num_data_servers += 1;
    auto cli = std::make_shared<RpcClient>(address, port, reliable);
    clients_.insert(std::make_pair(num_data_servers, cli));

    return true;
  }

  auto MetadataServer::run() -> bool
  {
    if (running)
      return false;

    // Currently we only support async start
    server_->run(true, num_worker_threads);
    running = true;
    return true;
  }

} // namespace chfs