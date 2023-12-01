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
        std::ofstream file("partition/" + filename, std::ios::binary | std::ios::app);
        for (const auto &value : values)
        {
            file.write(reinterpret_cast<const char *>(&value), sizeof(V));
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
            std::ifstream mapFile(filePath, std::ios::binary);

            std::unordered_map<K, std::vector<V>> KVmap;

            // Read the size of the key
            size_t keySize;

            while (mapFile.read(reinterpret_cast<char *>(&keySize), sizeof(keySize)))
            {
                // Read the key
                char *keyBuffer = new char[keySize];
                mapFile.read(keyBuffer, keySize);
                K key(keyBuffer, keySize);
                V value;
                delete[] keyBuffer;
                mapFile.read(reinterpret_cast<char *>(&value), sizeof(V));
                KVmap[key].push_back(value);
                // cout << key << '\n';
            }

            for (const auto &[key, values] : KVmap)
            {
                // cout << indexCount << '\n';
                // cout << key << '\n';
                if (indexList.find(key) == indexList.end())
                {
                    indexList[key] = std::to_string(indexCount++);
                }
                appendValuesToFile(indexList[key], values);
            }
        }
    }
    // 다른 필요한 메서드들...
};
