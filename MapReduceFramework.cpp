#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include "Mapper.cpp"
#include "RecordReader.cpp"
using namespace std;

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

template <typename K, typename V, typename U, typename W>
class WordCount : public Mapper<K, V, U, W>
{
public:
    WordCount(vector<V> splits) : Mapper<K, V, U, W>(splits) {}

    void map(const K &key, const V &value) override
    {
        istringstream iss(value);
        string word;
        while (iss >> word)
        {
            this->pair.setKey(word);
            this->pair.setValue(1);
            this->pair.write();
        }
    }
};

int main()
{
    RecordReader reader("input/test.txt", 2);
    reader.readSplits();
    vector<string> Splits = reader.getSplits();

    WordCount<string, string, string, int> wc(Splits);
    for (const auto &split : Splits)
    {
        wc.map("", split);
    }
    readBinaryFile<string, int>("mapout/tmp");
    cout << "DatabaseSystem Team Project";
    return 0;
}
