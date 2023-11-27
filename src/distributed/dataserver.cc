#include "distributed/dataserver.h"
#include "common/util.h"
#include "common/config.h"

namespace chfs
{

  auto DataServer::initialize(std::string const &data_path)
  {
    /**
     * At first check whether the file exists or not.
     * If so, which means the distributed chfs has
     * already been initialized and can be rebuilt from
     * existing data.
     */
    bool is_initialized = is_file_exist(data_path);

    auto bm = std::shared_ptr<BlockManager>(
        new BlockManager(data_path, KDefaultBlockCnt));

    // Calculate the total version blocks required
    const auto version_per_block = bm->block_size() / sizeof(version_t);
    auto total_version_block = bm->total_blocks() / version_per_block;
    if (bm->total_blocks() % version_per_block != 0)
    {
      total_version_block += 1;
    }

    if (is_initialized)
    {
      block_allocator_ =
          std::make_shared<BlockAllocator>(bm, total_version_block, false);
    }
    else
    {
      // We need to reserve some blocks for storing the version of each block
      // Version block are stored before the bitmap block, starting from block 0

      // Initialize the version blocks
      for (block_id_t i = 0; i < total_version_block; i++)
      {
        bm->zero_block(i);
      }

      block_allocator_ = std::shared_ptr<BlockAllocator>(
          new BlockAllocator(bm, total_version_block, true));
    }

    // Initialize the RPC server and bind all handlers
    server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                      usize len, version_t version)
                  { return this->read_data(block_id, offset, len, version); });
    server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                       std::vector<u8> &buffer)
                  { return this->write_data(block_id, offset, buffer); });
    server_->bind("alloc_block", [this]()
                  { return this->alloc_block(); });
    server_->bind("free_block", [this](block_id_t block_id)
                  { return this->free_block(block_id); });

    // Launch the rpc server to listen for requests
    server_->run(true, num_worker_threads);
  }

  DataServer::DataServer(u16 port, const std::string &data_path)
      : server_(std::make_unique<RpcServer>(port))
  {
    initialize(data_path);
  }

  DataServer::DataServer(std::string const &address, u16 port,
                         const std::string &data_path)
      : server_(std::make_unique<RpcServer>(address, port))
  {
    initialize(data_path);
  }

  DataServer::~DataServer() { server_.reset(); }

  auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                             version_t version) -> std::vector<u8>
  {
    // 读取 block_id 对应的 block，然后返回 offset 开始的 len 长度的数据
    // 如果以下非法读取情况，返回 size 为 0 的 vector
    // 1.block_id 对应的 block 的版本号和 version 不一致
    // 2.offset + len 超过了 block 的大小
    // 3.block_id 对应的 block 不存在

    std::vector<u8> result(this->block_allocator_->bm->block_size());
    // 读取 block_id 对应的 block
    ChfsNullResult read_result = this->block_allocator_->bm->read_block(block_id, result.data());
    // 如果 block_id 对应的 block 不存在，返回 size 为 0 的 vector
    if (read_result.is_err())
    {
      return {};
    }
    // 如果 offset + len 超过了 block 的大小，返回 size 为 0 的 vector
    if (offset + len > this->block_allocator_->bm->block_size())
    {
      return {};
    }
    // 如果 block_id 对应的 block 的版本号和 version 不一致，返回 size 为 0 的 vector
    auto version_block_id = block_id / version_per_block;
    auto version_offset = block_id % version_per_block;
    auto version_block = std::vector<u8>(this->block_allocator_->bm->block_size());
    ChfsNullResult version_result = this->block_allocator_->bm->read_block(version_block_id, version_block.data());
    if (version_result.is_err())
    {
      return {};
    }
    version_t version_read = *(version_block.begin() + version_offset * sizeof(version_t));
    if (version_read != version)
    {
      return {};
    }

    // 返回 offset 开始的 len 长度的数据
    return std::vector<u8>(result.begin() + offset, result.begin() + offset + len);
  }

  auto DataServer::write_data(block_id_t block_id, usize offset,
                              std::vector<u8> &buffer) -> bool
  {
    auto write_res = this->block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
    if (write_res.is_err())
    {
      return false;
    }

    return true;
  }

  auto DataServer::alloc_block() -> std::pair<block_id_t, version_t>
  {
    auto allo_res = this->block_allocator_->allocate();
    if (allo_res.is_err())
    {
      return {0, 0};
    }
    
    auto block_id = allo_res.unwrap();
    // 更新 block_id 对应的 block 的版本号
    auto version_block_id = block_id / version_per_block;
    auto version_offset = block_id % version_per_block;
    auto version_block = std::vector<u8>(this->block_allocator_->bm->block_size());
    ChfsNullResult version_result = this->block_allocator_->bm->read_block(version_block_id, version_block.data());
    if (version_result.is_err())
    {
      return {0, 0};
    }
    version_t version = *(version_block.begin() + version_offset * sizeof(version_t));
    version++;
    memcpy(version_block.data() + version_offset * sizeof(version_t), &version, sizeof(version_t));
    ChfsNullResult write_result = this->block_allocator_->bm->write_block(version_block_id, version_block.data());
    if (write_result.is_err())
    {
      return {0, 0};
    }

    return {block_id, version};
  }

  auto DataServer::free_block(block_id_t block_id) -> bool
  {
    ChfsNullResult result = this->block_allocator_->deallocate(block_id);
    if (result.is_err())
    {
      return false;
    }
    
    // 更新 block_id 对应的 block 的版本号
    auto version_block_id = block_id / version_per_block;
    auto version_offset = block_id % version_per_block;
    auto version_block = std::vector<u8>(this->block_allocator_->bm->block_size());
    ChfsNullResult version_result = this->block_allocator_->bm->read_block(version_block_id, version_block.data());
    if (version_result.is_err())
    {
      return false;
    }
    version_t version = *(version_block.begin() + version_offset * sizeof(version_t));
    version++;
    memcpy(version_block.data() + version_offset * sizeof(version_t), &version, sizeof(version_t));
    ChfsNullResult write_result = this->block_allocator_->bm->write_block(version_block_id, version_block.data());
    if (write_result.is_err())
    {
      return false;
    }

    return true;
  }
} // namespace chfs