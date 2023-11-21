#include "mapper.h"

void Mapper::map(vector<string>& keys, vector<vector<int>>& values, const vector<string>& records) {
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