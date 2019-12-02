#include <vector>
#include <map>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <direct.h>
#include <mutex>

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

struct cachePage_t{
    string table_name;  // which table_name the cache store
    size_t pageId;      // which page the cache store
    int type;
};

// Task struct (which will be executed)
struct Task
{
    int want_lock = NONE; 
    int real_lock = NONE; 
    int index_type = PRI_K;
    char table_obj[20];
    size_t row_object;      // data id which store in data(.db) file to parse to find the position of the data
 };

/* table information */
struct table_meta_t{
    //8
    int fields_count = 0;   
    int index_count = 0;
    // 48
    char pri_name[20];  // primary key name
    char pri_field_name[20];
    int pri_max_len;
    int pri_field_idx;
    //1240
    char indexs[MAX_INDEX_C][20];       // index key name
    char idx_field_name[MAX_INDEX_C][MAX_FIELD_C];  // the fields for per index contrains
    int indexs_max_len[MAX_INDEX_C];        //  max length of index key value
    //2800
    int fields_type[MAX_FIELD_C];       // fields type
    char fields_name[MAX_FIELD_C][20];  // fields name
    int fields_len[MAX_FIELD_C];    // Bytes count

    int min_data_len = 0;           // min data length (used for space reuse )
};
using table_meta_header = table_meta_t;

class Database
{
public:
    Database();
    ~Database();
    void QueryParse(int type, const vector<string> query);
    void Create(const vector<string> query);
    void Drop(const string& table_name);
    void Insert(const vector<string>& query);
    void Delete();
    void Update();
    void Select();
    void Error(string err_str);
protected:
    vector<Tree> Trees;
    char * memCache[TASK_C];    // addres of Load pages
    // Store all table_name which this page store and the PAGE_TYPE of page
    vector<vector<cachePage_t>> pageType;    // task(thread)->pageId

    int table_opened;                   // table count opened
    std::mutex table_count_mutex;       // mutex for open table

    int table_bit_map;                  // the opened bit map
    map<string, int> table_name_idx;    // table_name ==> index(used for follow)
    
    char * tableMeta[MAX_OPEN_TABLE];   // opened table's table_meta(get by mmap)
    data_meta_header dataMeta[MAX_OPEN_TABLE];      // table data meta information
    key_meta_header tablePriMeta[MAX_OPEN_TABLE];    // opened table's primary key meta 
    vector<pair<string, key_meta_header>> indexs_meta;  // store all index meta information for table
    
    int tableFd[MAX_OPEN_TABLE];    // open table's table_meta fd
    int tableData[MAX_OPEN_TABLE];   // opened table's table_data(fd object wait for mmap)
    map<string, map<string, int>> tab_key_fd;    // open(table key) ==> fd (wait for mmap)

    vector<map<string, Tree>> Trees(MAX_OPEN_TABLE);

    vector<Task> Tasks;                 // Store current waiting tasks 
    map<string, vector<int>> Locks;  // Store current Lock information
protected:
    void create_parse(const int & type, const vector<string>& query, string & table_name, map<string, string>& fields_result);
    void CreateFile(string table_name, map<string, string> fields_result);
    void LoadTable(string & table_name);
    void LoadTableIndex(string& table_name, string& key_name);
    bool LoadPage(string & table_name, int memCacheId, int posId, size_t pageId);
    
    void insert_parse(const vector<string>& query, string& table_name, vector<string>& field_values);

    bool table_exists(string table_name);
    bool page_exists(size_t pageId);
    int get_next_table_pos();
    size_t get_next_data_off(const strnig & table_name);
    inline void Database::set_next_table_pos(int i);
    int getNextRepPage(int taskId);
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
void Database::QueryParse(int type, const vector<string> query)
{
    // create table
    // create table table_name(id int, name char(10), primary key(id), index(name));

    switch((int)type){
        case CREATE:
            Create(query);
        break;
        case INSERT:
            Insert(query);
        break;
        default:
            Error("Operation type error");
        break;
    }
}



int Database::get_next_table_pos(){
    int i = 0;
    while(i < MAX_OPEN_TABLE){
        if((1 << i) & table_bit_map != 0){
            return i;
        }
    }
}

inline void Database::set_next_table_pos(int i){
    table_bit_map |= (1 <<< i);
} 

bool Database::table_exists(string table_name){
    fstream fs("table_name.txt", ios::out);
    char buffer[20];
	while (!fs.eof()) {
		fs.getline(buffer, 20);
		if (strcmp(buffer, table_name.c_str()) == 0) {
			return true;
		}
	}
    return false;
}



size_t Database::get_next_dir_off(const strnig & table_name){
    data_meta_header * data_meta_p = &dataMeta[table_name_idx[table_name]];
    data_meta_p->
}

bool Database::page_exists(size_t){

};

int Database::getNextRepPage(int taskId){

}