#include <iostream>
#include <string>
#include <vector>
#include <set>
#include <algorithm>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include "bpt.h"

using namespace std;
using namespace bpt;

#define USAGE "must begin with valid operations"
std::set<std::string> OP_ = {"select", "insert", "update", "delete", "create", "drop"};
enum OP{CREATE, DROP, SELECT, INSERT, UPDATE, DELETE};

struct query{
    int type;
    vector<string> query;
};

void cli_parse(char * argv[]);

int main(int argc, char * argv[]){
    if(argc >= 3){
        cli_parse(argv);
    }else{
        std::cerr<<USAGE<<endl;
        exit(0);
    }
}



void cli_parse(int argc, char * argv[]){
    // 此处应该有小写转换  留坑
    //...
    query q;
    std::set<string>::iterator iter = OP_.find(argv[1]);
    if(iter == OP_.end()){
        std::cout<<USAGE<<endl;
        exit(0);   
    }
    string op = *iter;
    if(op == "create"){
        if(argc != 4 || strcmp(argv[2], "table") != 0){
            std::cout<<USAGE<<endl;
            exit(0);  
        }
        q.type = CREATE;
    }else if(op == "drop"){
        if(argc < 4 || strcmp(argv[2], "table") != 0){
            std::cout<<USAGE<<endl;
            exit(0);  
        }
        q.type = DROP;
    }else if(op == "select"){
        if(argc < 5){
            std::cout<<USAGE<<endl;
            exit(0);  
        }
        q.type = SELECT;
    }else if(op == "insert"){
        if(argc < 4){
            std::cout<<USAGE<<endl;
            exit(0);  
        }
        q.type = INSERT;
    }else if(op == "update"){
        if(argc < 5){
            std::cout<<USAGE<<endl;
            exit(0);  
        }
        q.type = INSERT;
    }else if(op == "delete"){
        if(argc < 4){
            std::cout<<USAGE<<endl;
            exit(0);  
        }
        q.type = INSERT;
    }
    int i = 1;
    while(i < argc){
        q.query.push_back(argv[i]);
        i++;
    }
}
