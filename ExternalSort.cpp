#include <queue>
#include <vector>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <iterator>
#include <set>
#include <filesystem>

using namespace std;
namespace fs = std::filesystem;

template <typename K>
class Sorter
{
private:
    static const int BLOCK_SIZE = 10; // Block size 설정
    multiset<K> buffer;               // 버퍼 (키를 저장)
    string dirPath;                   // Partition 디렉토리 경로
    vector<string> sortedFiles;       // 정렬된 파일들의 이름을 저장할 vector

public:
    Sorter(const string &dirPath) : dirPath(dirPath) {}

    void sort()
    {
        for (const auto &entry : fs::directory_iterator(dirPath))
        {
            // 파일의 이름을 키로 사용
            K key = entry.path().filename().string();

            buffer.insert(key);

            // 버퍼의 크기가 block size를 넘으면 flush
            if (buffer.size() >= BLOCK_SIZE)
            {
                flush();
            }
        }

        // 처리되지 않은 마지막 블록을 저장
        if (!buffer.empty())
        {
            flush();
        }

        // 병합 과정 수행
        merge();
    }

    vector<string> getSortedFiles() const
    {
        return sortedFiles;
    }

    // merged.bin 파일을 읽어 키를 출력하는 함수
    void print()
    {
        ifstream inFile(dirPath + "/merged_result", ios::binary);

        if (inFile.is_open())
        {
            K key;
            while (inFile.read(reinterpret_cast<char *>(&key), sizeof(K)))
            {
                cout << key << endl;
            }
            inFile.close();
        }
        else
        {
            cout << "Failed to open file: " << dirPath + "/merged_result" << endl;
        }
    }

private:
    void flush()
    {
        string filename = dirPath + "/sorted_" + to_string(sortedFiles.size());
        sortedFiles.push_back(filename);

        ofstream outFile(filename, ios::binary);

        for (const auto &key : buffer)
        {
            outFile.write(reinterpret_cast<const char *>(&key), sizeof(K));
        }

        outFile.close();
        buffer.clear();
    }

    void merge()
    {
        using P = pair<K, ifstream *>;

        auto comp = [](const P &a, const P &b)
        {
            return a.first > b.first;
        };

        priority_queue<P, vector<P>, decltype(comp)> minHeap(comp);
        vector<ifstream> inFiles(sortedFiles.size());

        // 각 파일을 열고, 첫 번째 키를 minHeap에 추가
        for (size_t i = 0; i < sortedFiles.size(); ++i)
        {
            inFiles[i].open(sortedFiles[i], ios::binary);
            K key;
            if (inFiles[i].read(reinterpret_cast<char *>(&key), sizeof(K)))
            {
                minHeap.push(make_pair(key, &inFiles[i]));
            }
        }

        ofstream outFile(dirPath + "/merged_result", ios::binary);

        while (!minHeap.empty())
        {
            // 가장 작은 키를 가진 파일을 찾아 그 키를 outFile에 쓴다.
            auto [key, inFile] = minHeap.top();
            minHeap.pop();

            outFile.write(reinterpret_cast<const char *>(&key), sizeof(K));

            // 해당 파일에서 다음 키를 읽어 minHeap에 추가한다.
            if (inFile->read(reinterpret_cast<char *>(&key), sizeof(K)))
            {
                minHeap.push(make_pair(key, inFile));
            }
        }

        outFile.close();
    }
};
