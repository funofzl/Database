#include <vector>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <json/json.h>
#include "bpt.h"

using namespace std;

void trim(string& s){
    int al_left_index = s.find_first_not_of(" ");
    int al_right_index = s.find_last_not_of(" ");
    s = s.substr(al_left_index, al_right_index);
}

void split(char * s, vector<string>& result){
    char * temp = strtok(s, " ");
    while(temp != NULL){
        string t(temp);
        result.push_back(t);
        temp = strtok(NULL, " ");
    }
}

void split(string s, char dims, vector<string>& result){
    istringstream ss(s);
    string temp;
    while(getline(ss, temp, dims)){
        result.push_back(temp);
    }
}

// detect the string is contained of digits
bool is_digit_str(string s){
    int i = 0;
    while(i < s.size()){
        if(!isdigit(s[i])){
            return false;
        }
    }
    return true;
}


int startsWith(string s, string sub)
{
	return s.find(sub) == 0 ? 1 : 0;
}

int endsWith(string s, string sub)
{
	return s.rfind(sub) == (s.length() - sub.length()) ? 1 : 0;
}

string map2jsonstr(const map<string, string>& map_info)
{
	Json::Value jObject;
	for (map<string, string>::const_iterator iter = map_info.begin(); iter != map_info.end(); ++iter)
	{
		jObject[iter->first] = iter->second;
	}
	return jObject.toStyledString();
}

