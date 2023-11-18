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

            // �� ������ ������ �������� �и��Ͽ� �ܾ� ����
            while (iss >> word) {
                // �ҹ��ڷ� ��ȯ
                for (char& c : word) {
                    c = tolower(c);  // �ҹ��ڷ� ��ȯ
                }

                // Ư�� ���� ����
                word.erase(remove_if(word.begin(), word.end(), [](char c) { return !isalnum(c) && c != '\''; }), word.end());

                // �� ���ڿ��� �ƴϸ� �󵵼� ����
                if (!word.empty()) {
                    // �ܾ key��, �󵵼��� value�� ����
                    wordCount[word]++;
                }
            }
        }

        // ��� ���
        for (const auto& pair : wordCount) {
            // key�� value�� ���
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

    // Map �Լ� ȣ��
    mapper.map(keys, values, records);

    // ��� ���
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