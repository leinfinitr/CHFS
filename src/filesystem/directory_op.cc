#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // TODO: Implement this function.
  //       Append the new directory entry to `src`.
  std::string res = src;
  if (res.size() > 0) {
    res += '/';
  }
  res += filename + ':' + inode_id_to_string(id);
  return res;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  // TODO: Implement this function.
  //       Parse the directory string `src` and store the result in `list`.
  DirectoryEntry entry;
  std::string name;
  std::string id;
  std::string tmp;
  std::stringstream ss(src);
  while (std::getline(ss, tmp, '/')) {
    std::stringstream ss2(tmp);
    std::getline(ss2, name, ':');
    id = tmp.substr(name.size() + 1);
    entry.name = name;
    entry.id = string_to_inode_id(id);
    list.push_back(entry);
  }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // TODO: Implement this function.
  //       Remove the directory entry from `src`.
  std::list<DirectoryEntry> list;
  parse_directory(src, list);
  for (auto &entry : list) {
    if (entry.name != filename) {
      res = append_to_directory(res, entry.name, entry.id);
    }
  }

  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  // TODO: Implement this function.
  //       Read the directory with inode id `id` and store the result in `list`.
  auto res = fs->read_file(id).unwrap();
  std::string data(res.begin(), res.end());
  parse_directory(data, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  // TODO: Implement this function.
  //       Lookup the file with name `name` in the directory with inode id `id`.
  //       If the file exists, return the inode id of the file.
  //       If the file does not exist, return ErrorType::NotExist.
  read_directory(this, id, list).unwrap();
  for (auto &entry : list) {
    if (entry.name == name) {
      return ChfsResult<inode_id_t>(entry.id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // TODO:
  // 1. Check if `name` already exists in the parent.
  //    If already exist, return ErrorType::AlreadyExist.
  // 2. Create the new inode.
  // 3. Append the new entry to the parent directory.
  if(lookup(id, name).is_ok()) {
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }
  auto inode = alloc_inode(type).unwrap();
  auto res = read_file(id).unwrap();
  auto data = std::string(res.begin(), res.end());
  data = append_to_directory(data, name, inode);
  std::vector<u8> vec(data.begin(), data.end());
  write_file(id, vec);

  return ChfsResult<inode_id_t>(inode);
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // TODO: 
  // 1. Remove the file, you can use the function `remove_file`
  // 2. Remove the entry from the directory.
  auto res = lookup(parent, name).unwrap();
  remove_file(res);
  auto res2 = read_file(parent).unwrap();
  auto data = std::string(res2.begin(), res2.end());
  data = rm_from_directory(data, name);
  std::vector<u8> vec(data.begin(), data.end());
  write_file(parent, vec);
  
  return KNullOk;
}

} // namespace chfs
