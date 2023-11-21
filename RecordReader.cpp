#include "RecordReader.h"
#include <iostream>
#include <cstdlib>

RecordReader::RecordReader(const std::string &filename, int splitSize) : file(filename), splitSize(splitSize)
{
    if (!file.is_open())
    {
        std::cerr << "파일을 열지 못했습니다: " << filename << std::endl;
        exit(1);
    }
    else
    {
        readSplits();
    }
}

void RecordReader::readSplits()
{
    std::string line;
    std::vector<std::string> split;
    int lineCount = 0;
    while (getline(file, line))
    {
        if (!line.empty())
        {
            split.push_back(line);
            lineCount++;
        }

        if (lineCount == splitSize)
        {
            splits.push_back(join(split, "\n"));
            split.clear();
            lineCount = 0;
        }
    }

    if (!split.empty())
    {
        splits.push_back(join(split, "\n"));
    }
}

std::string RecordReader::join(const std::vector<std::string> &vec, const std::string &delimiter)
{
    std::string result;
    for (const auto &str : vec)
    {
        if (!result.empty())
        {
            result += delimiter;
        }
        result += str;
    }
    return result;
}

std::vector<std::string> RecordReader::getSplits() const
{
    return splits;
}
