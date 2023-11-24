#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include "Mapper.cpp"
#include "RecordReader.cpp"
#include "Partitioner.cpp"

using namespace std;

namespace myutil
{
    template <typename U, typename W>
    void readBinaryPair(const string &filename)
    {
        ifstream inFile(filename, ios::binary);
        U key;
        W value;
        if (inFile.is_open())
        {
            while (inFile.read(reinterpret_cast<char *>(&key), sizeof(key)))
            {
                inFile.read(reinterpret_cast<char *>(&value), sizeof(value));
                cout << "Key: " << key << ", Value: " << value << endl;
            }
            inFile.close();
        }
        else
        {
            cout << "Failed to open file: " << filename << endl;
        }
    }

#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

    template <typename W>
    void readBinaryMap(const std::string &dirPath)
    {
        for (const auto &entry : std::filesystem::directory_iterator(dirPath))
        {
            std::string filePath = entry.path().string();
            std::string key = entry.path().filename().string();

            std::ifstream inFile(filePath, std::ios::binary);
            std::vector<W> values;
            W value;

            if (inFile.is_open())
            {
                while (inFile.read(reinterpret_cast<char *>(&value), sizeof(W)))
                {
                    values.push_back(value);
                }

                std::cout << "Key: " << key << ", Values: ";
                for (const auto &val : values)
                {
                    std::cout << val << " ";
                }
                std::cout << std::endl;

                inFile.close();
            }
            else
            {
                std::cout << "Failed to open file: " << filePath << std::endl;
            }
        }
    }

    vector<string> split(const string &s, string delimiters)
    {
        vector<string> tokens;
        string str = s;

        for (char delimiter : delimiters)
        {
            replace(str.begin(), str.end(), delimiter, ' '); // replace를 통해 범위 내 특정 값(delimiter)를 ' '로 변경
        }

        stringstream ss(str);
        string temp;

        // 스트림 추출 연산자 >> 사용하여 공백 구분자로 문자열 분리
        while (ss >> temp)
        {
            tokens.push_back(temp);
        }

        return tokens;
    }
}

template <typename K, typename V, typename U, typename W>
class WordCount : public Mapper<K, V, U, W>
{
public:
    WordCount(vector<V> splits) : Mapper<K, V, U, W>(splits) {}

    void map(const K &key, const V &value) override
    {
        vector<string> words = myutil::split(value, "./ #^%$!@(_):?!,'");

        for (const auto &word : words)
        {
            this->pair.setKey(word);
            this->pair.setValue(1);
            this->pair.write();
        }
    }
};

int main()
{

    RecordReader reader("input/short.txt", 2);
    reader.readSplits();
    vector<string> Splits = reader.getSplits();

    WordCount<string, string, string, int> wc(Splits);
    int count = 0;
    for (const auto &split : Splits)
    {
        wc.setOutputPath("mapout/" + to_string(count++));
        wc.map("", split);
    }
    count--;

    Partitioner<string, int> partitioner;
    partitioner.read(count);
    partitioner.write();

    // // mapper test
    // while (count > 0)
    // {
    //     myutil::readBinaryFile<string, int>("mapout/" + to_string(count--));
    // }
    // // myutil::readBinaryFile<string, int>("mapout/tmp");

    // myutil::readBinaryFile<string, int>("mapout/tmp");
    myutil::readBinaryMap<int>("./partition");

    std::cout << "DatabaseSystem Team Project";
    return 0;
}
