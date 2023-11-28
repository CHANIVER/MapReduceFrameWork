#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <cctype>
#include <algorithm>
#include <fstream>

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

/**
 * Mapper 클래스
 * Mapper 클래스는 map 함수를 가지고 있으며, map 함수를 통해 key, value를 받아서 Pair에 저장하고 write
 * input 형식 -> InKeyType, InValueType
 * output 형식 -> OutKeyType, OutValueType
 * 생성자 인자 splits
 */

template <typename InKeyType, typename InValueType, typename OutKeyType, typename OutValueType>
class Mapper
{

protected:
    vector<InValueType> splits;
    Pair<OutKeyType, OutValueType> pair;

public:
    Mapper(vector<InValueType> splits) : splits(splits), pair(Pair<OutKeyType, OutValueType>()) {}

    /**
     * map 함수
     * key, value를 받아서 Pair에 저장하고 write
     * Pair에 저장된 값들은 버퍼에 저장되고 버퍼가 가득 차면 flush
     * @param key key 값
     * @param value value 값
     * @return void
     * @see Pair
     */
    virtual void map(const InKeyType &key, const InValueType &value) = 0;
    void flush() { this->pair.flush(); }
    void setOutputPath(string outfilename) { this->pair.setOutputPath(outfilename); }
};

/*
write를 하면 파일에다가 이진 파일로 저장하게끔하고
나중에 읽어들이면서 key별로 정렬 및 sort하면 되지 않을까.
*/
