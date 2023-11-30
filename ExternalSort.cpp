#include <filesystem>
#include <fstream>
#include <queue>
#include <set>
#include <vector>

using namespace std;
namespace fs = std::filesystem;

class Sorter
{
private:
    const size_t BLOCK_SIZE;
    multiset<string> buffer;
    vector<string> tempFiles;
    const string dirPath;

    void flush()
    {
        if (buffer.empty())
            return;

        string tempFile = dirPath + "/temp_" + to_string(tempFiles.size());
        tempFiles.push_back(tempFile);

        ofstream outFile(tempFile);
        for (const auto &key : buffer)
            outFile << key << '\n';
        outFile.close();

        buffer.clear();
    }

    void merge()
    {
        auto comp = [](const pair<string, ifstream *> &a, const pair<string, ifstream *> &b)
        {
            return a.first > b.first;
        };

        priority_queue<pair<string, ifstream *>, vector<pair<string, ifstream *>>, decltype(comp)> minHeap(comp);
        vector<ifstream> inFiles(tempFiles.size());

        for (size_t i = 0; i < tempFiles.size(); ++i)
        {
            inFiles[i].open(tempFiles[i]);
            string key;
            if (getline(inFiles[i], key))
                minHeap.push({key, &inFiles[i]});
        }

        ofstream outFile(dirPath + "/sorted_keys.txt");

        while (!minHeap.empty())
        {
            auto [key, inFile] = minHeap.top();
            minHeap.pop();

            outFile << key << '\n';

            if (getline(*inFile, key))
                minHeap.push({key, inFile});
        }

        outFile.close();
    }

public:
    Sorter(const string &dirPath, size_t blockSize) : dirPath(dirPath), BLOCK_SIZE(blockSize) {}

    void sort()
    {
        for (const auto &entry : fs::directory_iterator(dirPath))
        {
            string key = entry.path().filename().string();
            buffer.insert(key);

            if (buffer.size() >= BLOCK_SIZE)
                flush();
        }

        flush();
        merge();
    }
};
