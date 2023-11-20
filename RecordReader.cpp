#include <iostream>

using namespace std;

#include <fstream>
#include <vector>
#include <string>
#include <iostream>
#include <cstdlib>

using namespace std;

class RecordReader
{
private:
    ifstream file;
    vector<string> splits;
    int splitSize;

public:
    RecordReader(const string &filename, int splitSize) : file(filename), splitSize(splitSize)
    {
        if (!file.is_open())
        {
            cerr << "파일을 열지 못했습니다: " << filename << endl;
            exit(1);
        }
        else
        {
            readSplits();
        }
    }

    void readSplits()
    {
        string line;
        vector<string> split;
        int lineCount = 0;
        while (getline(file, line))
        {
            if (!line.empty())
            { // 레코드가 비어있지 않을 때만 split에 추가
                split.push_back(line);
                lineCount++;
            }

            if (lineCount == splitSize)
            {
                splits.push_back(join(split, "\n"));
                split.clear();
                lineCount = 0;
            }
        }

        if (!split.empty())
        {
            splits.push_back(join(split, "\n"));
        }
    }

    string join(const vector<string> &vec, const string &delimiter)
    {
        string result;
        for (const auto &str : vec)
        {
            if (!result.empty())
            {
                result += delimiter;
            }
            result += str;
        }
        return result;
    }

    vector<string> getSplits() const
    {
        return splits;
    }
};

int main()
{
    RecordReader reader("input/test.txt", 2);
    vector<string> splits = reader.getSplits();

    for (int i = 0; i < splits.size(); i++)
    {
        cout << "Split " << i + 1 << ":\n"
             << splits[i] << endl;
    }

    return 0;
}
