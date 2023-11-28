#pragma once
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <filesystem>
#include <fstream>
#include <thread>
#include <map>

using namespace std;

template <typename KeyType, typename ValueType>
class Pair
{
private:
    const int BUFFER_SIZE;                  // 버퍼 크기 설정
    map<KeyType, vector<ValueType>> buffer; // non-static 멤버로 변경
    int totalPairCount = 0;                 // non-static 멤버로 변경
    string filename = "/tmp";
    KeyType key;
    ValueType value;

public:
    Pair(int bufferSize) : BUFFER_SIZE(bufferSize) {} // 버퍼 크기를 설정하는 생성자

    ~Pair() { flush(); } // 소멸자에서 flush 호출

    void write()
    {
        // 버퍼에 추가
        if (buffer.find(key) == buffer.end())
        {
            buffer[key] = vector<ValueType>();
        }
        buffer[key].push_back(value);
        totalPairCount++;

        // 전체 키-값 쌍의 개수가 버퍼 크기를 초과하면 디스크에 쓰기
        if (totalPairCount >= BUFFER_SIZE)
        {
            flush();
        }
    }

    // flush 함수를 public으로 선언
    void flush()
    {
        ofstream outFile(filename, ios::app);

        if (outFile.is_open())
        {
            for (auto &pair : buffer)
            {
                KeyType key = pair.first;
                vector<ValueType> &values = pair.second;

                for (auto &value : values)
                {
                    outFile.write(reinterpret_cast<char *>(&key), sizeof(key));
                    outFile.write(reinterpret_cast<char *>(&value), sizeof(value));
                }
            }
            outFile.close();

            // 버퍼 비우기
            buffer.clear();
            totalPairCount = 0;
        }
    }

    KeyType getKey() { return key; }
    ValueType getValue() { return value; }
    void setKey(KeyType key) { this->key = key; }
    void setValue(ValueType value) { this->value = value; }
    void setOutputPath(string filename) { this->filename = filename; }
};
