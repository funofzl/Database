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

// There are PAGE_N*4 cache pages;
const int PAGE_C = PAGE_N * 4;   

// There are only TASK_C in running.
const int TASK_C = PAGE_N;

// max page cache when there are not so many tasks
const int PAGE_PER_TASK = 4 * 2;

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


struct {

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
    void Database::CreateFile(string table_name, map<string, string> fields_result);
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

