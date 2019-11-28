#include <vector>
#include <map>
#include <string>

#include "bpt.h"
#include "utils.hpp"

using namespace bpt;
using std::string;
using std::vector;
using std::map;

// Cache object count (which one is two pages)
const int PAGE_N = 4;

// There are PAGE_N*2 cache pages;
const int PAGE_C = PAGE_N * 2;

// There are only TASK_C in running.
const int TASK_C = PAGE_N;

// max page cache when there are not so many tasks
const int TASK_PAGE_C = 4;

enum LOCK_TYPE{NONE, READ, WRITE};
// 操作表的键类型
enum INDEX_TYPE{NON_K, PRI_K, COM_K};

enum OP{CREATE, DROP, SELECT, INSERT, UPDATE, DELETE};

enum FIELD_TYPE{INT, CHAR, VARCHAR, ENUM, TEXT};

void map_type(string real_type, int & type){
    
}

struct Task
{
    int want_lock = NONE;
    int real_lock = NONE; 
    int index_type = PRI_K;
    string table_obj;
    string row_object;
};


class Database
{
public:
    Database();
    ~Database();
    void QueryParse(int type, const vector<string> query);
    void Create(map<string, string>& fields_result);
    void Drop();
    void Insert();
    void Delete();
    void Update();
    void Select();
    void Error(string err_str);
protected:
    vector<bpt::bplus_tree> Trees;
    vector<string> Cache_table;         // Store tables which this page store
    vector<bpt::page_t> Cache_pages;    // Store cache pages
    vector<Task> Tasks;                 // Store current waiting tasks 
    map<string, vector<int>> Locks;  // Store current Lock information
protected:
    void create_parse(const int & type, const vector<string>& query, map<string, string>& fields_result);
};


Database::Database(){
    Trees.reserve(TASK_C);
    Cache_table.reserve(TASK_C);
    Cache_pages.reserve(PAGE_C);
    
}

void Database::Error(string err_str){
    cout<<err_str<<endl;
    exit(0);
}

// 请求解析(重点)
void Database::QueryParse(int type, const vector<string> query)
{
    // create table
    // create table table_name(id int, name char(10), primary key(id), index(name));
     if(type == CREATE){
        map<string, string> fields_result;
        create_parse(type, query, fields_result);
        Create(fields_result);
    }
}

// create parse
 void Database::create_parse(const int & type, const vector<string>& query, map<string, string>& fields_result){
    int i = 0;
    string qs;
    while(i < query.size()){
        qs += query[i];
        i++;
    }
    // find the left and right bracket after "create table tablename(...)"
    int bracket_left = qs.find_first_of("(");
    if(bracket_left < 0){
        Error("syntax error around tablenam");
    }
    int bracket_right = qs.find_last_of(")");
    if(bracket_right < 0){
        Error("syntax error around tablename");
    }
    /* First substr the tablename */
    string table_name = qs.substr(0, bracket_left);
    qs = qs.substr(bracket_left, bracket_right);

    /* Second parse the fields */
    vector<string> fields;
    // split the sql with comma-","
    int comma_pos = comma_pos = qs.find_first_of(",");
    while(comma_pos >= 0){
        fields.push_back(qs.substr(0, comma_pos));
        qs = qs.substr(comma_pos+1, qs.size());
        comma_pos = qs.find_first_of(",");
    }
    if(fields.size < 1){
        Error("There is not any field!");
    }
    // trim " filed type " to "field type"
    for(i = 0;i<fields.size();i++){
        trim(fields[i]);
    }
    // field_result to store the result of parsing fields
    int j;
    vector<string> temp;
    for(i = 0;i<fields.size();i++){
        // 1.find the "()" of "name cahr(10)" or "primary key()"
        bracket_left = fields[i].find_first_of("(");
        bracket_right = fields[i].find_last_of(")");
        if(bracket_left * bracket_right < 0){       // there are only one side can match
            Error("brackets match error");
        }

        split(fields[i], ' ', temp);
        // there is not any brackets, so just like "id int"
        if(bracket_left < 0){   
            temp.clear();
            trim(temp[1]);
            fields_result[temp[0]] = temp[1];
            continue;
        }
        // there are some brackets
        // three conditions
        // 1. filed type contains () like char varcahr enum 
        int brack_pos_l = temp[1].find_first_of("(");
        int brack_pos_r = temp[1].find_last_of(")");
        // (var)char
        int pos = fields[i].find("char");
        if(pos != fields[i].npos){
            if(!is_digit_str(temp[1].substr(brack_pos_l+1, brack_pos_r-1))){
                Error("char length error");
            }
            if(pos > 2 && fields[i].substr(pos-3,pos) == "var"){
                fields_result[temp[0]] = "varchar " + temp[1].substr(brack_pos_l+1, brack_pos_r-1);
            }else{
                fields_result[temp[0]] = "char " + temp[1].substr(brack_pos_l+1, brack_pos_r-1);
            }
            continue;
        }
        // enum
        pos = fields[i].find("enum");
        if(pos != fields[i].npos){
            string t = fields[i].substr(brack_pos_l+1, brack_pos_r-1);
            vector<string> ts;
            split(t, ',', ts);
            string type_all;
            j = 0;
            while(j < ts.size()){
                trim(ts[j]);
                ts[j] = ts[j].substr(1, ts[j].size()-1);
                type_all += (" " + ts[j]);
                j++;
            } 
            fields_result[temp[0]] = "enum" + type_all;
            continue;
        }
        // primary
        pos = fields[i].find("primary");
        if(pos != fields[i].npos){
            trim(fields[1]);
            int pos_small_l = fields[1].find_first_of("(");
            int pos_small_r = fields[1].find_first_of(")");
            string key_name = fields[1].substr(0, pos_small_l-1);
            string field_name =  fields[1].substr(pos_small_l+1, pos_small_r-1);
            if(fields_result.count(field_name) < 1){
                Error("primary key error");
            }
            fields_result[key_name] = "primary " + field_name;
            continue;
        }
        // index
        pos = fields[i].find("index");
        if(pos != fields[i].npos){
            trim(fields[1]);
            int pos_small_l = fields[1].find_first_of("(");
            int pos_small_r = fields[1].find_first_of(")");
            string key_name = fields[1].substr(0, pos_small_l-1);
            string field_name =  fields[1].substr(pos_small_l+1, pos_small_r-1);
            vector<string> ts;
            split(field_name, ',', ts);
            field_name = "";
            j = 0;
            while(j < ts.size()){
                trim(ts[j]);
                if(fields_result.count(ts[j]) < 1){
                    Error("index key error");
                }
                field_name += ( " " + ts[j] );
            }
            fields_result[key_name] = "index " + field_name;
            continue;
        }
    }
}
   
// 以下几个操作注意约束和限制条件(留坑完事)
// 插入
void Database::Insert(){
    
}

// create table
void Create(map<string, string>& fields_result)
{
    /* create table information file */
    // srtucture
    // db file name
    // fields
    // keys
}

// 删表
void Database::Drop()
{

}

// delete query
void Database::Delete()
{

}

// update query
void Database::Update()
{

}

// selectc query
void Database::Select()
{

}