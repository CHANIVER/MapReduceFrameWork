/*
 * Project Name: MapReduceFramework
 * Project Start Date: 2023.11.08
 * Version: 1.0.0
 * Version Date: 2023.12.01
 * Author: Kwon Yechan, Park Solji, Yun Hongmin
 *
 * Project Description:
 *  - This project is a framework that implements the MapReduce algorithm in C++.
 *    Users can define map and reduce functions to perform data analysis tasks that can be parallelized.
 *
 * How to Execute:
 *  - Build: g++ -std=c++17 MapReduceFramework.cpp -o MapReduceFramework
 *  - Execute ./MapReduceFramework <inputfile> <split_buffer_size> <mapreduce_buffer_size>
 *  - Condition: input, output, mapout, partition, sorted, reduceout directories must exist in the directory.
 *
 * Argument Description:
 *  - <inputfile>: Path of the input file
 *  - <split_buffer_size>: Size of the split buffer
 *  - <mapreduce_buffer_size>: Size of the MapReduce buffer
 *
 * Development Environment:
 *  - Development Environment: Visual Studio Code
 *  - Operating System: macOS
 *  - Programming Language: C++17
 *
 * Execution Support Environment:
 *  - macOS is recommended. However, it can be executed on any platform that supports C++17.
 *
 * Dataset Support Information:
 *  - Alphabet: Supported
 *  - Korean: Partially Supported
 *  - Other: Partially Supported
 *
 * Key Type Support Information:
 *  - string: Supported
 *  - int: Partially Supported
 *  - float: Partially Supported
 *
 * Value Type Support Information:
 *  - int: Supported
 *  - float: Supported
 *  - string: Partially Supported
 */

/*
 * 프로젝트 이름: MapReduceFramework
 * 프로젝트 시작 일시: 2023.11.08
 * 버전: 1.0.0
 * 버전 날짜: 2023.12.01
 * 저자: Kwon Yechan, Park Solji, Yun Hongmin
 *
 * 프로젝트 설명:
 *  - 이 프로젝트는 MapReduce 알고리즘을 C++로 구현한 프레임워크입니다.
 *    사용자는 맵 함수와 리듀스 함수를 정의하여 병렬 처리가 가능한 데이터 분석 작업을 수행할 수 있습니다.
 *
 * 실행 방법:
 *  - 빌드: g++ -std=c++17 MapReduceFramework.cpp -o MapReduceFramework
 *  - 실행: ./MapReduceFramework <inputfile> <split_buffer_size> <mapreduce_buffer_size>
 *  - 조건: 디렉토리에 input, output, mapout, partition, sorted, reduceout 디렉토리가 존재해야 합니다.
 *
 *
 * 인자 설명:
 *  - <inputfile>: 입력 파일의 경로
 *  - <split_buffer_size>: 스플릿 버퍼의 크기
 *  - <mapreduce_buffer_size>: 맵리듀스 버퍼의 크기
 *
 * 개발 환경:
 *  - 개발 환경: Visual Studio Code
 *  - 운영 체제: macOS
 *  - 프로그래밍 언어: C++17
 *
 * 실행 지원 환경:
 *  - macOS를 추천합니다. 그러나 C++17을 지원하는 모든 플랫폼에서 실행이 가능합니다.
 *
 * 데이터셋 지원 정보:
 *  - 알파벳: 지원
 *  - 한글: 불완전 지원
 *  - 그외: 불완전 지원
 *
 * key 형식 지원 정보:
 *  - string: 지원
 *  - int: 불완전 지원
 *  - float: 불완전 지원
 *
 * value 형식 지원 정보:
 *  - int: 지원
 *  - float: 지원
 *  - string: 불완전 지원
 */

// ↓↓ Do not change this file ↓↓  ----------------------------------
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
#include "Reducer.cpp"
#include "ExternalSort.cpp"
// ↑↑ Do not change this file ↑↑----------------------------------

// ↓↓ You can change this parameters ↓↓  -------------------------------
#define MAPKEY_IN string
#define MAPVALUE_IN string
#define MAPKEY_OUT string
#define MAPVALUE_OUT int
#define REDUCEKEY_IN MAPKEY_OUT
#define REDUCEVALUE_IN MAPVALUE_OUT
#define REDUCEKEY_OUT string
#define REDUCEVALUE_OUT int
// ↑↑ You can change this parameters ↑↑ -------------------------------

// ↓↓ Do not change this code ↓↓  ----------------------------------
using namespace std;
namespace myutil
{
    // 이전 결과 파일들을 제거하는 함수 선언
    void removeFiles(const string &dirPath);

    // 이진 페어를 읽는 함수 템플릿 선언
    template <typename U, typename W>
    void readBinaryPair(const string &filename);

    // 이진 맵을 읽는 함수 템플릿 선언
    template <typename W>
    void readBinaryMap(const std::string &dirPath);

    // 문자열을 분리하는 함수 선언
    vector<string> split(const string &s, string delimiters);

    // mapout 파일들을 txt로 출력하는 함수 선언
    void printMapoutFiles(int count);

    // partition 파일들을 txt로 출력하는 함수 선언
    void printPartitionFiles();
}
// ↑↑ Do not change this code ↑↑ ----------------------------------

// ↓↓ You can change this code ↓↓  ----------------------------------
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
// ↑↑ You can change this code ↑↑ ----------------------------------

// ↓↓ Do not change this code ↓↓  ----------------------------------
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

    // 전체 실행시간 측정
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
    auto end = std::chrono::system_clock::now();
    std::cout << "map 수행시간: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms\n";

    // partition 수행시간 측정
    auto time = std::chrono::system_clock::now();
    Partitioner<MAPKEY_OUT, MAPVALUE_OUT> partitioner;
    partitioner.processDirectory("mapout/");
    end = std::chrono::system_clock::now();
    std::cout << "partition 수행시간: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - time).count() << "ms\n";

    // sort 수행시간 측정
    time = std::chrono::system_clock::now();
    Sorter<REDUCEKEY_IN> sorter(mapReduceBufferSize);
    vector<REDUCEKEY_IN> keys = partitioner.getKeys();
    sorter.externalSort(keys, "sorted/sorted_keys.txt");
    end = std::chrono::system_clock::now();
    std::cout << "sort 수행시간: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - time).count() << "ms\n";

    // reduce
    // reduce 수행시간 측정
    time = std::chrono::system_clock::now();
    std::ifstream keysFile("sorted/sorted_keys.txt");
    std::string key;
    std::vector<std::thread> reduceThreads;
    while (std::getline(keysFile, key))
    {
        reduceThreads.emplace_back([&, key]()
                                   {
        WordCountReducer<REDUCEKEY_IN, REDUCEVALUE_IN, REDUCEKEY_OUT, REDUCEVALUE_OUT> reducer(mapReduceBufferSize);
        reducer.setOutputPath("reduceout/result");
        string thisFilename= partitioner.getFilePath(key);
        std::ifstream inFile(thisFilename, std::ios::binary);
        //check if file is open
        if (!inFile.is_open())
        {
            std::cout << "Failed to open file: " << thisFilename << std::endl;
            return;
        }
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
    end = std::chrono::system_clock::now();
    std::cout << "reduce 수행시간: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - time).count() << "ms\n";

    // 전체 수행시간 출력
    std::cout << "전체 수행시간: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms\n";

    // map 파일들을 txt로 출력하시겠습니까?
    myutil::printMapoutFiles(count);

    // 입력 엔터 방지
    cin.ignore();

    // reduce 파일을 txt로 출력하시겠습니까?
    myutil::printPartitionFiles();

    std::cout << "DatabaseSystem Team Project Done";
    // clean up all files in mapout, partition, sorted
    myutil::removeFiles("mapout");
    myutil::removeFiles("partition");
    myutil::removeFiles("sorted");
    myutil::removeFiles("reduceout");

    return 0;
}
// ↑↑ Do not change this code ↑↑ ----------------------------------

// ↓↓ Do not change this code ↓↓  ----------------------------------
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

    // mapout 파일들을 txt로 출력하시겠습니까?
    void printMapoutFiles(int count)
    {
        cout << "map 파일들을 txt로 출력하시겠습니까? (y/n)\n 주의! 시간이 오래 걸릴 수 있습니다.\n";
        char answer;
        cin >> answer;
        if (answer == 'y')
        {
            for (int i = 0; i <= count; i++)
            {
                // mapout/0 파일을 읽어서 mapout/0.txt로 출력
                ifstream inFile("mapout/" + to_string(i), ios::binary);
                ofstream outFile("output/mapout" + to_string(i) + ".txt");
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
    }

    // partition 파일들을 txt로 출력하시겠습니까?
    void printPartitionFiles()
    {
        cout << "결과 파일을 txt로 출력하시겠습니까? (y/n)" << '\n';
        char answer2;
        cin >> answer2;
        if (answer2 == 'y')
        {
            // reduceout/result 파일을 읽어서 reduceout/result.txt로 출력
            ifstream inFile("reduceout/result", ios::binary);
            ofstream outFile("output/result.txt");
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
    }
}
// ↑↑ Do not change this code ↑↑ ----------------------------------