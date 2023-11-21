#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cctype>
#include <algorithm>

using namespace std;

class Mapper {
public:
    void map(vector<string>& keys, vector<vector<int>>& values, const vector<string>& records) {
        unordered_map<string, vector<int>> wordCount;

        for (const string& record : records) {
            istringstream iss(record);
            string word;

            while (iss >> word) {
                word.erase(remove_if(word.begin(), word.end(), [](char c) { return !isalnum(c) && c != '\''; }), word.end());

                if (!word.empty()) {
                    if (wordCount[word].empty() || wordCount[word].back() == 0) {
                        keys.push_back(word);
                        values.push_back({ 1 });
                    }
                    else {
                        wordCount[word].back()++;
                    }
                }
            }
        }
    }
};

void partitionAndSort(vector<string>& keys, vector<vector<int>>& values) {
    // Partitioning
    unordered_map<string, int> partitionIndices;
    int currentPartition = 0;

    for (size_t i = 0; i < keys.size(); ++i) {
        if (partitionIndices.find(keys[i]) == partitionIndices.end()) {
            partitionIndices[keys[i]] = currentPartition++;
        }
    }

    vector<vector<string>> partitionedKeys(currentPartition);
    vector<vector<vector<int>>> partitionedValues(currentPartition);

    for (size_t i = 0; i < keys.size(); ++i) {
        int partitionIndex = partitionIndices[keys[i]];
        partitionedKeys[partitionIndex].push_back(keys[i]);
        partitionedValues[partitionIndex].push_back(values[i]);
    }

    // 각 파티션 내에서 정렬
    for (int i = 0; i < currentPartition; ++i) {
        // 키와 값들을 함께 정렬
        vector<pair<string, vector<int>>> keyValuePairs;

        // 중복된 키를 처리하기 위한 맵
        unordered_map<string, vector<int>> uniqueKeys;

        // 중복된 키에 대한 값을 리스트로 합치기
        for (size_t j = 0; j < partitionedKeys[i].size(); ++j) {
            uniqueKeys[partitionedKeys[i][j]].insert(uniqueKeys[partitionedKeys[i][j]].end(), partitionedValues[i][j].begin(), partitionedValues[i][j].end());
        }

        // 중복된 키와 해당 키에 대한 값들을 keyValuePairs에 추가
        for (const auto& entry : uniqueKeys) {
            keyValuePairs.push_back({ entry.first, entry.second });
        }

        // partitionedKeys와 partitionedValues를 업데이트
        partitionedKeys[i].clear();
        partitionedValues[i].clear();

        // 정렬된 keyValuePairs로 partitionedKeys와 partitionedValues를 갱신
        for (size_t j = 0; j < keyValuePairs.size(); ++j) {
            partitionedKeys[i].push_back(keyValuePairs[j].first);
            partitionedValues[i].push_back(keyValuePairs[j].second);
        }
    }



    // Flatten the partitions
    keys.clear();
    values.clear();

    for (int i = 0; i < currentPartition; ++i) {
        keys.insert(keys.end(), partitionedKeys[i].begin(), partitionedKeys[i].end());
        values.insert(values.end(), partitionedValues[i].begin(), partitionedValues[i].end());
    }
}

int main() {
    Mapper mapper;
    vector<string> keys;
    vector<vector<int>> values;
    vector<string> records = { "Hello, my name is solji.", "How are you, today?", "I'm from Korea.", "How about you?", "Hi, solji" };

    mapper.map(keys, values, records);
    partitionAndSort(keys, values);

    for (size_t i = 0; i < keys.size(); ++i) {
        cout << keys[i] << " ";
        for (int val : values[i]) {
            cout << val << " ";
        }
        cout << endl;
    }

    return 0;
}







/*
int main() {
    Mapper mapper;
    vector<string> keys;
    vector<vector<int>> values;
    vector<string> records = { "Hello, my name is solji.", "How are you, today?", "I'm from Korea.", "How about you?", "Hi, Solji" };

    // Map 함수 호출
    mapper.map(keys, values, records);

    // 결과 출력
    for (size_t i = 0; i < keys.size(); ++i) {
        cout << keys[i] << ": ";
        for (int val : values[i]) {
            cout << val << " ";
        }
        cout << endl;
    }

    return 0;
}

*/





/*
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cctype>

using namespace std;

class Mapper {
public:
    void map(vector<string>& keys, vector<vector<int>>& values, const vector<string>& records) {
        unordered_map<string, int> wordCount;

        for (const string& record : records) {
            istringstream iss(record);
            string word;

            // 한 라인을 공백을 기준으로 분리하여 단어 추출
            while (iss >> word) {
                // 소문자로 변환
                for (char& c : word) {
                    c = tolower(c);  // 소문자로 변환
                }

                // 특수 문자 제거
                word.erase(remove_if(word.begin(), word.end(), [](char c) { return !isalnum(c) && c != '\''; }), word.end());

                // 빈 문자열이 아니면 빈도수 증가
                if (!word.empty()) {
                    // 단어를 key로, 빈도수를 value로 저장
                    wordCount[word]++;
                }
            }
        }

        // 결과 출력
        for (const auto& pair : wordCount) {
            // key와 value를 출력
            keys.push_back(pair.first);
            values.push_back({ pair.second });
        }
    }
};

int main() {
    Mapper mapper;
    vector<string> keys;
    vector<vector<int>> values;
    vector<string> records = { "Hello, my name is solji.", "How are you, today?", "I'm from Korea.", "How about you?", "Hi, Solji" ,"You are solji.","Hello", "Hello," };

    // Map 함수 호출
    mapper.map(keys, values, records);

    // 결과 출력
    for (size_t i = 0; i < keys.size(); ++i) {
        cout << keys[i] << ": ";
        for (int val : values[i]) {
            cout << val << " ";
        }
        cout << endl;
    }

    return 0;
}
*/