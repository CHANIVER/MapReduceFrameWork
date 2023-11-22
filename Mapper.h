#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cctype>
#include <algorithm>
#include <fstream>

using namespace std;

template <typename K, typename V, typename U, typename W>
class Mapper
{
protected:
    Pair<U, W> pair;

public:
    virtual void map(const K &key, const V &value) = 0;
};

template <typename U, typename W>
class Pair
{
private:
    const string filename = "/tmp";
    const U key;
    const W value;

public:
    void write()
    {
        ofstream outFile(filename, ios::binary);

        if (outFile.is_open())
        {
            outFile.write(reinterpret_cast<char *>(&key), sizeof(key));
            outFile.write(reinterpret_cast<char *>(&value), sizeof(value));
            outFile.close();
        }
    }

    U getKey() { return key; }
    W getValue() { return value; }
    U setKey(U key) { this->key = key; }
    W setValue(W value) { this->value = value; }
};

/*
write를 하면 파일에다가 이진 파일로 저장하게끔하고
나중에 읽어들이면서 key별로 정렬 및 sort하면 되지 않을까.
*/
