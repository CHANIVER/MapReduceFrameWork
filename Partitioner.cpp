#include <fstream>
#include <map>
#include <vector>
#include <string>
#include <unordered_map>
#include <filesystem> // 디렉토리 순회를 위한 헤더

using namespace std;
namespace fs = std::filesystem; // namespace alias 설정
template <typename U, typename W>
class Partitioner
{
private:
    string inputPath = "mapout/";
    string outputPath = "partition/";

public:
    void run()
    {
        for (const auto &entry : fs::directory_iterator(inputPath))
        {
            unordered_map<U, vector<W>> partitionMap;

            ifstream inFile(entry.path(), ios::binary);

            if (inFile.is_open())
            {
                size_t keySize;
                char *keyBuffer;
                W value;

                while (inFile.read(reinterpret_cast<char *>(&keySize), sizeof(keySize)))
                {
                    keyBuffer = new char[keySize];
                    inFile.read(keyBuffer, keySize);
                    U key(keyBuffer, keySize);
                    delete[] keyBuffer;

                    inFile.read(reinterpret_cast<char *>(&value), sizeof(value));
                    partitionMap[key].push_back(value);
                }

                inFile.close();
            }
            // 추가한 디버깅 코드
            std::cout << "Number of keys in partitionMap: " << partitionMap.size() << '\n';
            for (const auto &pair : partitionMap)
            {
                std::cout << "Key: " << pair.first << ", Value count: " << pair.second.size() << '\n';
            }

            for (const auto &pair : partitionMap)
            {
                string filename = outputPath + pair.first;
                ofstream outFile(filename, ios::binary | ios::app);

                if (outFile.is_open())
                {
                    for (const auto &val : pair.second)
                    {
                        outFile.write(reinterpret_cast<const char *>(&val), sizeof(val));
                    }
                    outFile.close();
                }
                else
                {
                    std::cerr << "Failed to open file: " << filename << '\n';
                }
            }
        }
    }
};