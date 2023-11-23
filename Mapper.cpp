#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cctype>
#include <algorithm>
#include <fstream>

using namespace std;

template <typename U, typename W>
class Pair
{
private:
    const string filename = "mapout/tmp";
    U key;
    W value;

public:
    void write()
    {
        ofstream outFile(filename, ios::app);

        if (outFile.is_open())
        {
            outFile.write(reinterpret_cast<char *>(&key), sizeof(key));
            outFile.write(reinterpret_cast<char *>(&value), sizeof(value));
            outFile.close();
        }
    }

    U getKey() { return key; }
    W getValue() { return value; }
    void setKey(U key) { this->key = key; }
    void setValue(W value) { this->value = value; }
};

/**
 * input 형식 -> K, V
 * output 형식 -> U, W
 * 생성자 인자 splits
 */
template <typename K, typename V, typename U, typename W>
class Mapper
{

protected:
    vector<V> splits;
    Pair<U, W> pair;

public:
    Mapper(vector<V> splits) : splits(splits), pair(Pair<U, W>()) {}

    /**
     * map 함수
     *
     */
    virtual void map(const K &key, const V &value) = 0;
};
/*
write를 하면 파일에다가 이진 파일로 저장하게끔하고
나중에 읽어들이면서 key별로 정렬 및 sort하면 되지 않을까.
*/
