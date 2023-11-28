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
    static const int BUFFER_SIZE = 10;             // 버퍼 크기 설정
    static map<KeyType, vector<ValueType>> buffer; // 버퍼 (키-값 쌍을 저장)
    static int totalPairCount;                     // 전체 키-값 쌍의 개수
    string filename = "/tmp";
    KeyType key;
    ValueType value;

public:
    void write()
    {
        // 버퍼에 추가
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

// 템플릿 클래스의 정적 데이터 멤버 초기화
template <typename KeyType, typename ValueType>
map<KeyType, vector<ValueType>> Pair<KeyType, ValueType>::buffer;
template <typename KeyType, typename ValueType>
int Pair<KeyType, ValueType>::totalPairCount = 0;