#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <filesystem>
#include <fstream>
#include <thread>

#include "Mapper.cpp"
#include "RecordReader.cpp"
#include "Partitioner.cpp"
#include "ExternalSort.cpp"

#define MAPKEY_IN string
#define MAPVALUE_IN string
#define MAPKEY_OUT string
#define MAPVALUE_OUT int
#define REDUCEKEY_IN MAPKEY_OUT
#define REDUCEVALUE_IN MAPVALUE_OUT
#define REDUCEKEY_OUT string
#define REDUCEVALUE_OUT int

using namespace std;

namespace myutil
{
    // 편의를 위해 이전 결과 파일들을 제거하는 함수
    void removeFiles(const string &dirPath)
    {
        for (const auto &entry : std::filesystem::directory_iterator(dirPath))
        {
            std::filesystem::remove(entry.path());
        }
    }

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
    // clean up all files in mapout, partition, sorted
    myutil::removeFiles("mapout");
    myutil::removeFiles("partition");
    myutil::removeFiles("sorted");

    // wait 2 seconds
    std::this_thread::sleep_for(std::chrono::seconds(20));

    RecordReader reader("input/english_text_file.txt", 500000);
    reader.readSplits();
    vector<string> Splits = reader.getSplits();

    // map
    WordCount<MAPKEY_IN, MAPVALUE_IN, MAPKEY_OUT, MAPVALUE_OUT> wc(Splits);
    int count = 0;
    for (const auto &split : Splits)
    {
        cout << count << '\n';
        wc.setOutputPath("mapout/" + to_string(count++));
        wc.map("", split);
        wc.flush();
    }
    count--;

    // partition
    Partitioner<MAPKEY_OUT, MAPVALUE_OUT> partitioner;
    partitioner.read(count);
    partitioner.write();

    // output all mapper files
    while (count >= 0)
    {
        myutil::readBinaryPair<MAPKEY_OUT, MAPVALUE_OUT>("mapout/" + to_string(count--));
    }

    cout << "partition 입니다." << '\n';
    myutil::readBinaryMap<MAPVALUE_OUT>("./partition");

    // sort(external)
    Sorter<REDUCEKEY_IN> sorter("./partition");
    sorter.sort();
    cout << "sorted 입니다." << '\n';
    sorter.print();

    std::cout << "DatabaseSystem Team Project";
    return 0;
}
