#ifndef RECORD_READER_H
#define RECORD_READER_H

#include <fstream>
#include <vector>
#include <string>

class RecordReader
{
private:
    std::ifstream file;
    std::vector<std::string> splits;
    int splitSize;

public:
    RecordReader(const std::string &filename, int splitSize);
    void readSplits();
    std::string join(const std::vector<std::string> &vec, const std::string &delimiter);
    std::vector<std::string> getSplits() const;
};

#endif
