#include <vector>
#include <map>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <direct.h>

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


// max opened table
const int MAX_OPEN_TABLE = 20;

enum LOCK_TYPE{NONE, READ, WRITE};

// 操作表的键类型
enum INDEX_TYPE{NON_K, PRI_K, COM_K};

enum OP{CREATE, DROP, SELECT, INSERT, UPDATE, DELETE};

enum FIELD_TYPE{INT, CHAR, VARCHAR, ENUM, TEXT, PRIMARY, INDEX, ERROR};

const int MAX_FIELD_C = 100;
const int MAX_INDEX_C = 10;

const map<string, int> Field_t{
    {"int", 0}, {"char", 1}, {"varchar", 2}, {"enum", 3}, {"text", 4},
    {"primary", 5}, {"index", 6}
};


// field string convert to FIELD_TYPE(int)
int parse_type(const string &t)
{   
    map<string, int>::const_iterator iter;
    iter = Field_t.find(t);
    if(iter == Field_t.cend()){
        cout<<"FIELD_TYPE Error. Unknown type"<<endl;
        return FIELD_TYPE::ERROR;
    }
    return iter->second;
}

string get_type_str(int tp){
    map<string, int>::const_iterator iter = Field_t.cbegin();
    while(iter != Field_t.cend()){
        if(iter->second == tp){
            return iter->first;
        }
    }
    return "error";
}


// Task struct (which will be executed)
struct Task
{
    int want_lock = NONE; 
    int real_lock = NONE; 
    int index_type = PRI_K;
    char table_obj[20];
    size_t row_object;      // data id which store in data(.db) file to parse to find the position of the data
 };

/* sizeof(8 + 40 + 1240 +2800) = 4088 (Bytes) */
struct table_meta_t{
    //8
    int fields_count = 0;   
    int index_count = 0;
    // 40
    char pri_name[20];  // primary key name
    char pri_field_name[20];
    int pri_max_len;
    //1240
    char indexs[MAX_INDEX_C][20];       // index key name
    char idx_field_name[MAX_INDEX_C][MAX_FIELD_C];  // the fields for per index contrains
    int indexs_max_len[MAX_INDEX_C];        //  max length of index key value
    //2800
    int fields_type[MAX_FIELD_C];       // fields type
    char fields_name[MAX_FIELD_C][20];  // fields name
    int fields_len[MAX_FIELD_C];    // Bytes count
};
using table_meta_header = table_meta_t;

class Database
{
public:
    Database();
    ~Database();
    void QueryParse(int type, const vector<string> query);
    void Create(const vector<string> query);
    void Drop();
    void Insert();
    void Delete();
    void Update();
    void Select();
    void Error(string err_str);
protected:
    vector<Tree> Trees;
    char * memCache[TASK_C];    // addres of Load pages
    int table_opened;
    char * tableMeta[MAX_OPEN_TABLE];   // opened table's table_meta
    map<string, map<string, int>> tab_key_fd;    // open(table key) ==> fd



    vector<string> Cache_table;         // Store all table_name which this page store

    vector<Task> Tasks;                 // Store current waiting tasks 
    map<string, vector<int>> Locks;  // Store current Lock information
protected:
    void create_parse(const int & type, const vector<string>& query, string & table_name, map<string, string>& fields_result);
    void CreateFile(string table_name, map<string, string> fields_result);
    void LoadTable(string & table_name);
    bool table_exists(string table_name);
};


Database::Database(){
    Cache_table.reserve(TASK_C);
    Cache_pages.reserve(PAGE_C);
    
}

void Database::Error(string err_str){
    cout<<err_str<<endl;
    exit(0);
}

// 请求解析(重点)
void Database::QueryParse(int typ, const vector<string> query)
{
    // create table
    // create table table_name(id int, name char(10), primary key(id), index(name));
    if((int)typ == CREATE){
        Create(query);
    }
}



