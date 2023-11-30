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
#include "Reducer.cpp"

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
        if (inFile.is_open())
        {
            while (!inFile.eof())
            {
                // Read the size of the key
                size_t keySize;
                inFile.read(reinterpret_cast<char *>(&keySize), sizeof(keySize));

                // Read the key
                char *keyBuffer = new char[keySize];
                inFile.read(keyBuffer, keySize);
                U key(keyBuffer, keySize);
                delete[] keyBuffer;

                // Read the value
                W value;
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
class WordCountMapper : public Mapper<K, V, U, W>
{
public:
    WordCountMapper(int blockSize) : Mapper<K, V, U, W>(blockSize) {}

    void map(const K &key, const V &value) override
    {
        vector<string> words = myutil::split(value, ",./#^%$!@(_):?!'`‘“-&|\" ");

        for (const auto &word : words)
        {
            this->pair.setKey(word);
            this->pair.setValue(1);
            this->pair.write();
        }
        this->flush(); // map 함수가 끝나면 flush 호출
    }
};

template <typename K, typename V, typename U, typename W>
class WordCountReducer : public Reducer<K, V, U, W>
{
public:
    WordCountReducer(int blockSize) : Reducer<K, V, U, W>(blockSize) {}

    void reduce(const K &key, const vector<V> &value) override
    {
        int sum = 0;
        for (const auto &val : value)
        {
            sum += val;
        }
        this->pair.setKey(key);
        this->pair.setValue(sum);
        this->pair.write();
    }
};

int main(int argc, char *argv[])
{
    if (argc < 4)
    {
        cout << "Usage: " << argv[0] << "<inputfile> <split_buffer_size> <mapreduce_buffer_size>" << endl;
        return 1;
    }
    string inputfile = argv[1]; // input/test.txt
    int splitBufferSize = stoi(argv[2]);
    int mapReduceBufferSize = stoi(argv[3]);

    // clean up all files in mapout, partition, sorted
    myutil::removeFiles("mapout");
    myutil::removeFiles("partition");
    myutil::removeFiles("sorted");
    myutil::removeFiles("reduceout");

    // wait 2 seconds
    // std::this_thread::sleep_for(std::chrono::seconds(20));

    // 실행시간 측정
    auto start = std::chrono::system_clock::now();

    RecordReader reader(inputfile, splitBufferSize);
    reader.readSplits();
    vector<string> Splits = reader.getSplits();

    std::vector<std::thread> threads;
    int count = 0;
    for (const auto &split : Splits)
    {
        auto wc = std::make_shared<WordCountMapper<MAPKEY_IN, MAPVALUE_IN, MAPKEY_OUT, MAPVALUE_OUT>>(mapReduceBufferSize);
        wc->setOutputPath("mapout/" + std::to_string(count));
        threads.emplace_back([wc, split, count]
                             {
        wc->map("", split);
        wc->flush(); });
        count++;
    }

    for (auto &thread : threads)
    {
        thread.join();
    }
    count--;

    // map 수행시간 출력
    std::cout << "map 수행시간: " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start).count() << "ms\n";

    // output all mapper files
    // while (count >= 0)
    // {
    //     myutil::readBinaryPair<MAPKEY_OUT, MAPVALUE_OUT>("mapout/" + to_string(count--));
    // }

    // map 파일들을 txt로 출력
    cout << "map 파일들을 txt로 출력하시겠습니까? (y/n)" << endl;
    char answer;
    cin >> answer;
    if (answer == 'y')
    {
        for (int i = 0; i < Splits.size(); i++)
        {
            // mapout/0 파일을 읽어서 mapout/0.txt로 출력
            ifstream inFile("mapout/" + to_string(i), ios::binary);
            ofstream outFile("mapout" + to_string(i) + ".txt");
            if (inFile.is_open() && outFile.is_open())
            {
                while (!inFile.eof())
                {
                    // Read the size of the key
                    size_t keySize;
                    inFile.read(reinterpret_cast<char *>(&keySize), sizeof(keySize));

                    // Read the key
                    char *keyBuffer = new char[keySize];
                    inFile.read(keyBuffer, keySize);
                    MAPKEY_OUT key(keyBuffer, keySize);
                    delete[] keyBuffer;

                    // Read the value
                    MAPVALUE_OUT value;
                    inFile.read(reinterpret_cast<char *>(&value), sizeof(value));
                    outFile << "Key: " << key << ", Value: " << value << endl;
                }
                inFile.close();
                outFile.close();
            }
            else
            {
                cout << "Failed to open file: "
                     << "mapout/" + to_string(i) << endl;
            }
        }
    }

    // partition 수행시간 측정
    start = std::chrono::system_clock::now();
    // partition
    Partitioner<MAPKEY_OUT, MAPVALUE_OUT> partitioner;
    partitioner.run();
    // partition 수행시간 출력
    std::cout << "partition 수행시간: " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start).count() << "ms\n";

    // cout << "partition 입니다." << '\n';
    // myutil::readBinaryMap<MAPVALUE_OUT>("./partition");

    // sort 수행시간 측정
    start = std::chrono::system_clock::now();
    // sort(external)
    Sorter sorter("./partition", mapReduceBufferSize);
    sorter.sort();
    // cout << "sorted 입니다." << '\n';
    // sorter.print();
    // sort 수행시간 출력
    std::cout << "sort 수행시간: " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start).count() << "ms\n";

    // reduce
    // reduce 수행시간 측정
    start = std::chrono::system_clock::now();
    std::ifstream keysFile("./partition/sorted_keys.txt");
    std::string key;
    std::vector<std::thread> reduceThreads;
    while (std::getline(keysFile, key))
    {
        reduceThreads.emplace_back([&, key]()
                                   {
        WordCountReducer<REDUCEKEY_IN, REDUCEVALUE_IN, REDUCEKEY_OUT, REDUCEVALUE_OUT> reducer(mapReduceBufferSize);
        reducer.setOutputPath("reduceout/result");
        std::ifstream inFile("./partition/" + key, std::ios::binary);
        std::vector<REDUCEVALUE_IN> values;
        REDUCEVALUE_IN value;
        while (inFile.read(reinterpret_cast<char*>(&value), sizeof(value))) {
            values.push_back(value);
        }
        reducer.reduce(key, values); });
    }

    for (auto &thread : reduceThreads)
    {
        thread.join();
    }
    // reduce 수행시간 출력
    std::cout << "reduce 수행시간: " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - start).count() << "ms\n";

    // // output reduceout file
    // myutil::readBinaryPair<REDUCEKEY_OUT, REDUCEVALUE_OUT>("reduceout/result");

    // 결과 파일을 txt로 출력하시겠습니까?
    cout << "결과 파일을 txt로 출력하시겠습니까? (y/n)" << endl;
    char answer2;
    cin >> answer2;
    if (answer2 == 'y')
    {
        // reduceout/result 파일을 읽어서 reduceout/result.txt로 출력
        ifstream inFile("reduceout/result", ios::binary);
        ofstream outFile("result.txt");
        if (inFile.is_open() && outFile.is_open())
        {
            while (!inFile.eof())
            {
                // Read the size of the key
                size_t keySize;
                inFile.read(reinterpret_cast<char *>(&keySize), sizeof(keySize));

                // Read the key
                char *keyBuffer = new char[keySize];
                inFile.read(keyBuffer, keySize);
                REDUCEKEY_OUT key(keyBuffer, keySize);
                delete[] keyBuffer;

                // Read the value
                REDUCEVALUE_OUT value;
                inFile.read(reinterpret_cast<char *>(&value), sizeof(value));

                outFile << "Key: " << key << ", Value: " << value << endl;
            }
            inFile.close();
            outFile.close();
        }
        else
        {
            cout << "Failed to open file: "
                 << "reduceout/result" << endl;
        }
    }

    std::cout << "DatabaseSystem Team Project";

    return 0;
}
