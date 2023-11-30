#pragma once
#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <filesystem>
#include <fstream>
#include <thread>
#include <map>
#include <unordered_map>
using namespace std;

template <typename KeyType, typename ValueType>
class Pair
{
private:
    const int BUFFER_SIZE; // 버퍼 크기 설정
    // map<KeyType, vector<ValueType>> buffer; // non-static 멤버로 변경
    unordered_map<KeyType, vector<ValueType>> buffer; // non-static 멤버로 변경

    int totalPairCount = 0; // non-static 멤버로 변경
    string filename = "/tmp";
    KeyType key;
    ValueType value;

public:
    Pair(int bufferSize) : BUFFER_SIZE(bufferSize) {} // 버퍼 크기를 설정하는 생성자

    ~Pair() { flush(); } // 소멸자에서 flush 호출

    void write()
    {
        // Check if key exists before inserting
        auto it = buffer.find(key);
        if (it != buffer.end())
        {
            it->second.push_back(value);
        }
        else
        {
            buffer[key] = vector<ValueType>{value};
        }

        // If the total number of key-value pairs exceeds the buffer size, write to disk
        if (buffer.size() >= BUFFER_SIZE)
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
            for (const auto &pair : buffer)
            {
                const KeyType &key = pair.first;
                const vector<ValueType> &values = pair.second;

                for (auto &value : values)
                {
                    // Write the key
                    const char *keyData = key.c_str();
                    size_t keySize = key.size();
                    outFile.write(reinterpret_cast<const char *>(&keySize), sizeof(keySize));
                    outFile.write(keyData, keySize);

                    // Write the value
                    outFile.write(reinterpret_cast<const char *>(&value), sizeof(value));
                }
            }
            outFile.close();

            // 버퍼를 새로운 unordered_map 객체로 교체
            buffer = unordered_map<KeyType, vector<ValueType>>();
            totalPairCount = 0;
        }
        else
        {
            cout << "Failed to open file: " << filename << endl;
        }
    }

    KeyType getKey() { return key; }
    ValueType getValue() { return value; }
    void setKey(KeyType key) { this->key = key; }
    void setValue(ValueType value) { this->value = value; }
    void setOutputPath(string filename) { this->filename = filename; }
};
