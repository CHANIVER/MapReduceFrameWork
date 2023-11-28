#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <cctype>
#include <algorithm>
#include <fstream>
#include "Pair.h"

using namespace std;

/**
 * Mapper 클래스
 * Mapper 클래스는 map 함수를 가지고 있으며, map 함수를 통해 key, value를 받아서 Pair에 저장하고 write
 * input 형식 -> InKeyType, InValueType
 * output 형식 -> OutKeyType, OutValueType
 * 생성자 인자 splits
 */

template <typename InKeyType, typename InValueType, typename OutKeyType, typename OutValueType>
class Mapper
{

protected:
    vector<InValueType> splits;
    Pair<OutKeyType, OutValueType> pair;

public:
    Mapper(vector<InValueType> splits) : splits(splits), pair(Pair<OutKeyType, OutValueType>()) {}

    /**
     * map 함수
     * key, value를 받아서 Pair에 저장하고 write
     * Pair에 저장된 값들은 버퍼에 저장되고 버퍼가 가득 차면 flush
     * @param key key 값
     * @param value value 값
     * @return void
     * @see Pair
     */
    virtual void map(const InKeyType &key, const InValueType &value) = 0;
    void flush() { this->pair.flush(); }
    void setOutputPath(string outfilename) { this->pair.setOutputPath(outfilename); }
};

/*
write를 하면 파일에다가 이진 파일로 저장하게끔하고
나중에 읽어들이면서 key별로 정렬 및 sort하면 되지 않을까.
*/
