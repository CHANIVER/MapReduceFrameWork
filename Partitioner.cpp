#include <fstream>
#include <map>
#include <vector>
#include <string>

using namespace std;
template <typename U, typename W>
class Partitioner
{
private:
    string inputPath = "mapout/";
    string outputPath = "partition/";
    map<U, vector<W>> partitionMap;

public:
    void read(int count)
    {
        for (int i = 0; i < count; i++)
        {
            string filename = inputPath + to_string(i);
            ifstream inFile(filename, ios::binary);

            if (inFile.is_open())
            {
                while (!inFile.eof())
                {
                    U key;
                    W value;
                    inFile.read(reinterpret_cast<char *>(&key), sizeof(key));
                    inFile.read(reinterpret_cast<char *>(&value), sizeof(value));
                    partitionMap[key].push_back(value);
                }
                inFile.close();
            }
        }
    }

    void write()
    {
        for (auto it = partitionMap.begin(); it != partitionMap.end(); ++it)
        {
            std::string filename = outputPath + "/" + it->first;
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
};
