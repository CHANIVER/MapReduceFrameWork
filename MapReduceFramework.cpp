#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>

using namespace std;

class ReduceTask
{
private:
    unordered_map<string, int> reducedValues;

public:
    void ReduceTaskFunction(vector<string>& keys, vector<vector<int>>& values)
    {
        // keys와 values의 크기가 같은지 확인
        if (keys.size() != values.size())
        {
            cerr << "Error: The sizes of keys and values are not the same." << endl;
            return;
        }

        // keys와 values를 순회하며 묶어서 더하기
        for (int i = 0; i < keys.size(); i++)
        {
            const string& key = keys[i];
            const vector<int>& value = values[i];

            // key가 이미 map에 존재하는 경우 해당 key의 value에 값을 추가
            if (reducedValues.find(key) != reducedValues.end())
            {
                for (int j = 0; j < value.size(); j++)
                {
                    reducedValues[key] += value[j];
                }
            }
            // key가 map에 없는 경우 새로운 key로 값을 추가
            else
            {
                reducedValues[key] = 0;
                for (int j = 0; j < value.size(); j++)
                {
                    reducedValues[key] += value[j];
                }
            }
        }

        // 결과를 다시 keys와 values에 할당 => 자료구조 하나씩 더 만들어야 할지 (?)
        keys.clear();
        values.clear();

        for (const auto& pair : reducedValues)
        {
            keys.push_back(pair.first);
            values.push_back({ pair.second });
        }
    }

    void PrintResults() const
    {
        for (const auto& pair : reducedValues)
        {
            cout << "word : " << pair.first << "  Frequency : " << pair.second << endl;
        }
    }

    void SaveResultsToFile(const string& filename) const
    {
        ofstream outputFile(filename);

        if (outputFile.is_open())
        {
            for (const auto& pair : reducedValues)
            {
                outputFile << "word : " << pair.first << "  Frequency : " << pair.second << endl;
            }

            outputFile.close();
            cout << "Results saved to " << filename << endl;
        }
        else
        {
            cerr << "Error: Unable to open the file " << filename << " for writing." << endl;
        }
    }
};

int main()
{
    ReduceTask reducer;

    vector<string> keys = { "apple", "banana", "apple", "orange" };
    vector<vector<int>> values = { {1}, {2}, {3}, {1} };

    reducer.ReduceTaskFunction(keys, values);
    reducer.PrintResults();
    reducer.SaveResultsToFile("output.txt");

    return 0;
}
