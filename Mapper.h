#pragma once

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
    void map(vector<string>& keys, vector<vector<int>>& values, const vector<string>& records);
};

void partitionAndSort(vector<string>& keys, vector<vector<int>>& values);
