#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cctype>

using namespace std;

class MyMapper {
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
/*
int main() {
    MyMapper mapper;
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