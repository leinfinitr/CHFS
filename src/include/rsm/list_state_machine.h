#include "rsm/state_machine.h"
#include <mutex>
#include <sstream>

namespace chfs
{

    class ListCommand : public ChfsCommand
    {
    public:
        ListCommand() {}
        ListCommand(int v) : value(v) {}
        virtual ~ListCommand() {}

        virtual size_t size() const override { return 4; }

        /**
         * 对象的数据按照特定的规则序列化为字节流，并将字节流存储在buf向量中。
         * 序列化的过程按照大端字节序（Big-Endian）进行，将对象的value成员按字节顺序存储在buf中。
         * 返回值：如果传入的sz参数与对象的大小不相等，则返回一个空的字节向量buf；否则，返回存储着序列化数据的buf向量。
         * @return std::vector<u8>，即一个无符号字节向量。
         * @param int sz，表示序列化的数据大小。
         */
        virtual std::vector<u8> serialize(int sz) const override
        {
            std::vector<u8> buf;

            if (sz != size())
                return buf;

            buf.push_back((value >> 24) & 0xff);
            buf.push_back((value >> 16) & 0xff);
            buf.push_back((value >> 8) & 0xff);
            buf.push_back(value & 0xff);

            return buf;
        }

        /**
         * 根据特定的规则，从字节流data中反序列化数据，并将结果存储在对象的value成员中。
         * 反序列化的过程按照大端字节序（Big-Endian）进行，将字节流中的字节按照顺序重新组合成一个整数，并将结果赋值给对象的value成员。
         * @param std::vector<u8> data，即一个无符号字节向量。
         * @param int sz，表示反序列化的数据大小。
         */
        virtual void deserialize(std::vector<u8> data, int sz) override
        {
            if (sz != size())
                return;
            value = (data[0] & 0xff) << 24;
            value |= (data[1] & 0xff) << 16;
            value |= (data[2] & 0xff) << 8;
            value |= data[3] & 0xff;
        }

        int value;
    };

    class ListStateMachine : public ChfsStateMachine
    {
    public:
        ListStateMachine()
        {
            store.push_back(0);
            num_append_logs = 0;
        }

        virtual ~ListStateMachine() {}

        virtual std::vector<u8> snapshot() override
        {
            std::unique_lock<std::mutex> lock(mtx);
            std::vector<u8> data;
            std::stringstream ss;
            ss << (int)store.size();
            for (auto value : store)
                ss << ' ' << value;
            std::string str = ss.str();
            data.assign(str.begin(), str.end());
            return data;
        }

        virtual void apply_log(ChfsCommand &cmd) override
        {
            std::unique_lock<std::mutex> lock(mtx);
            const ListCommand &list_cmd = dynamic_cast<const ListCommand &>(cmd);
            store.push_back(list_cmd.value);
            num_append_logs++;
        }

        virtual void apply_snapshot(const std::vector<u8> &snapshot) override
        {
            std::unique_lock<std::mutex> lock(mtx);
            std::string str;
            str.assign(snapshot.begin(), snapshot.end());
            std::stringstream ss(str);
            store = std::vector<int>();
            int size;
            ss >> size;
            for (int i = 0; i < size; i++)
            {
                int temp;
                ss >> temp;
                store.push_back(temp);
            }
        }

        std::mutex mtx;
        std::vector<int> store;
        int num_append_logs;
    };

} /* namespace chfs */