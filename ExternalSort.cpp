#include <iostream>
#include <algorithm>
#include <vector>
#include <queue>
#include <fstream>
#include <string>

template <typename K>
class Sorter
{
public:
    int blockSize;

    Sorter(int blockSize) : blockSize(blockSize) {}

    void externalSort(std::vector<K> &input, std::string outputFileName)
    {
        int numFiles = divideIntoSortedFiles(input);
        mergeFiles(numFiles, outputFileName);
    }

private:
    struct QueueNode
    {
        K value;
        int fileIndex;

        QueueNode(K v, int index) : value(v), fileIndex(index) {}

        bool operator>(const QueueNode &node) const
        {
            return value > node.value;
        }
    };

    int divideIntoSortedFiles(std::vector<K> &input)
    {
        int fileCount = 0;
        for (int i = 0; i < input.size(); i += blockSize)
        {
            std::vector<K> block(input.begin() + i, input.begin() + std::min(i + blockSize, (int)input.size()));
            std::sort(block.begin(), block.end());
            std::ofstream out(std::to_string(fileCount));
            for (const K &value : block)
            {
                out << value << std::endl;
            }
            fileCount++;
        }
        return fileCount;
    }

    void mergeFiles(int numFiles, std::string outputFileName)
    {
        std::vector<std::ifstream> inputStreams(numFiles);
        for (int i = 0; i < numFiles; i++)
        {
            inputStreams[i].open(std::to_string(i));
        }

        std::priority_queue<QueueNode, std::vector<QueueNode>, std::greater<>> minHeap;
        for (int i = 0; i < numFiles; i++)
        {
            K value;
            if (inputStreams[i] >> value)
            {
                minHeap.push(QueueNode(value, i));
            }
        }

        std::ofstream out(outputFileName);
        while (!minHeap.empty())
        {
            QueueNode node = minHeap.top();
            minHeap.pop();
            out << node.value << std::endl;
            if (inputStreams[node.fileIndex] >> node.value)
            {
                minHeap.push(node);
            }
        }

        for (int i = 0; i < numFiles; i++)
        {
            inputStreams[i].close();
            remove(std::to_string(i).c_str());
        }
    }
};
