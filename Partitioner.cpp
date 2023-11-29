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
    // unordered_map<U, vector<W>> partitionMap;

public:
    void run()
    {
        for (const auto &entry : fs::directory_iterator(inputPath)) // directory_iterator를 이용한 디렉토리 순회
        {
            unordered_map<U, vector<W>> partitionMap;   // Move the unordered_map here
            ifstream inFile(entry.path(), ios::binary); // 각 파일을 순회하며 열기

            if (inFile.is_open())
            {
                while (!inFile.eof())
                {
                    char keyBuffer[2048]; // Replace KEY_SIZE with the actual size of the key
                    W value;
                    inFile.read(keyBuffer, sizeof(keyBuffer));
                    inFile.read(reinterpret_cast<char *>(&value), sizeof(value));

                    // Convert the key to a string
                    U key(keyBuffer, sizeof(keyBuffer));

                    // Check if key exists before inserting
                    auto it = partitionMap.find(key);
                    if (it != partitionMap.end())
                    {
                        it->second.push_back(value);
                    }
                    else
                    {
                        partitionMap[key] = vector<W>{value};
                    }
                }

                inFile.close();
            }

            for (auto it = partitionMap.begin(); it != partitionMap.end(); ++it) // partitionMap의 모든 요소에 대해
            {
                std::string filename = outputPath + it->first;
                std::ofstream outFile(filename, std::ios::binary);

                if (outFile.is_open())
                {
                    for (auto val : it->second)
                    {
                        outFile.write(reinterpret_cast<char *>(&val), sizeof(val));
                    }
                    outFile.close();
                }
            }
        }
    }
};
