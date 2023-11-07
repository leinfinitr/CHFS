#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // 读取 block_id 对应的 block，然后返回 offset 开始的 len 长度的数据
  // 如果以下非法读取情况，返回 size 为 0 的 vector
  // 1.block_id 对应的 block 的版本号和 version 不一致
  // 2.offset + len 超过了 block 的大小
  // 3.block_id 对应的 block 不存在

  std::vector<u8> result;
  // 读取 block_id 对应的 block
  ChfsNullResult read_result = this->block_allocator_->bm->read_block(block_id, result.data());
  // 如果 block_id 对应的 block 不存在，返回 size 为 0 的 vector
  if (read_result.is_err()) {
    return {};
  }
  // 如果 offset + len 超过了 block 的大小，返回 size 为 0 的 vector
  if (offset + len > this->block_allocator_->bm->block_size()) {
    return {};
  }
  // TODO: 如果 block_id 对应的 block 的版本号和 version 不一致，返回 size 为 0 的 vector
  
  // 返回 offset 开始的 len 长度的数据
  return std::vector<u8>(result.begin() + offset, result.begin() + offset + len);
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // 读取 block_id 对应的 block，然后将 buffer 中的数据写入到 block 中 offset 开始的位置
  // 如果以下非法写入情况，返回 false
  // 1.offset + buffer.size() 超过了 block 的大小
  // 2.block_id 对应的 block 不存在

  std::vector<u8> result;
  ChfsNullResult write_result = this->block_allocator_->bm->read_block(block_id, result.data());
  if (write_result.is_err()) {
    return {};
  }
  if (offset + buffer.size() > this->block_allocator_->bm->block_size()) {
    return false;
  }

  this->block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());

  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  ChfsResult<block_id_t> block_id = this->block_allocator_->allocate();
  if(block_id.is_err()) {
    return {0, 0};
  }
  block_id_t block_id_t = block_id.unwrap();

  return {block_id_t, 0};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  ChfsNullResult result = this->block_allocator_->deallocate(block_id);
  if(result.is_err()) {
    return false;
  }
  return true;
}
} // namespace chfs