#include <fstream>
#include <unordered_map>
#include <list>
#include <filesystem>
#include <vector>
#include <queue>
#include <algorithm>
#include <iostream>

template <typename K, typename V>
class Partitioner
{
private:
    std::unordered_map<K, std::string> indexList;
    size_t indexCount = 0;

    void appendValuesToFile(const std::string &filename, const std::vector<V> &values)
    {
        std::ofstream file("partition/" + filename, std::ios::app);
        for (const auto &value : values)
        {
            file << value << " ";
        }
    }

public:
    // get all keys as a vector
    vector<K> getKeys()
    {
        vector<K> keys;
        for (const auto &[key, value] : indexList)
        {
            keys.push_back(key);
        }
        return keys;
    }

    string getFilePath(const K &key)
    {
        return "partition/" + indexList[key];
    }

    void processDirectory(const std::string &mapoutDir)
    {
        for (const auto &entry : std::filesystem::directory_iterator(mapoutDir))
        {
            std::string filePath = entry.path().string();
            std::ifstream mapFile(filePath);

            std::unordered_map<K, std::vector<V>> KVmap;

            // Read the key-value pairs
            K key;
            V value;

            while (mapFile >> key >> value)
            {
                KVmap[key].push_back(value);
            }

            for (const auto &[key, values] : KVmap)
            {
                if (indexList.find(key) == indexList.end())
                {
                    indexList[key] = std::to_string(indexCount++);
                }
                appendValuesToFile(indexList[key], values);
            }
        }
    }
};
