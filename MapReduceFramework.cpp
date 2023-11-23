#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include "Mapper.cpp"
#include "RecordReader.cpp"
using namespace std;

namespace myutil
{
    template <typename U, typename W>
    void readBinaryFile(const string &filename)
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
    for (const auto &split : Splits)
    {
        wc.map("", split);
    }
    myutil::readBinaryFile<string, int>("mapout/tmp");
    cout << "DatabaseSystem Team Project";
    return 0;
}
