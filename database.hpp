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

// page count for per extend(which is the 单位 of data file add)
const int EXTEND_SIZE = 4;  


// max opened table
const int MAX_OPEN_TABLE = 20;

enum LOCK_TYPE{NONE, READ, WRITE};

// 操作表的键类型
enum INDEX_TYPE{DATA, PRI_K, COM_K};

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
    char type_name[20]; // which type this page store   contains(data, peimay key, index key)
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
using table_meta_header = struct table_meta_t;

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
    vector<vector<cachePage_t> > pagetypes;    // task(thread)->pageId
    int ReQueue[TASK_C][PAGE_N];               // log the replace sequence of pages
    

    int table_opened;                   // table count opened
    std::mutex table_count_mutex;       // mutex for open table

    int table_bit_map;                  // the opened bit map
    map<string, int> table_name_idx;    // table_name ==> index(used for follow)
    
    char * tableMeta[MAX_OPEN_TABLE];   // opened table's table_meta(get by mmap)
    data_meta_header dataMeta[MAX_OPEN_TABLE];      // table data meta information
    key_meta_header tablePriMeta[MAX_OPEN_TABLE];    // opened table's primary key meta 
    vector<map<string, key_meta_header>> indexs_meta;  // store all index meta information for table
    
    int tableFd[MAX_OPEN_TABLE];    // open table's table_meta fd
    int tableData[MAX_OPEN_TABLE];   // opened table's table_data(fd object wait for mmap)
    map<string, map<string, int>> tab_key_fd;    // open(table key) ==> fd (wait for mmap)

    vector<map<string, Tree*> > Trees;   

    vector<Task> Tasks;                 // Store current waiting tasks 
    map<string, vector<int>> Locks;  // Store current Lock information
protected:
    void create_parse(const int & type, const vector<string>& query, string & table_name, map<string, string>& fields_result);
    void CreateFile(string table_name, map<string, string> fields_result);
    int LoadTable(string & table_name);
    void LoadTableIndex(string& table_name, string& key_name);
    bool LoadPage(string & table_name, int memCacheId, int posId, size_t pageId);
    
    void insert_parse(const vector<string>& query, string& table_name, vector<string>& field_values);

    void expandExtend(string& table_name, char * expand_ob);
    
    // table
    int get_next_table_pos();
    inline void set_next_table_pos(int i);
    // data
    size_t get_next_dir_off(int t_idx, int length, int& from_where);
    int getNextRepPage(int taskId);

    void set_last_next(int t_idx, size_t dir_off);
    void set_dir_next(size_t prv_dic, size_t next_dic_off);
    size_t get_dir_next(size_t dir_off);

    // 因为控制相连块的row_dic不一定相连(两块合并后就有这种情况)
    size_t get_next_row_dic_by_data(size_t from_dic, short row_off){
        while(((row_dic*)from_dic)->row_off != row_off){
            from_dic += sizeof(row_dic);
        }
        return from_dic;
    }
    
    //page
    int get_page_count(string & table_name, char * expand_type);

    bool table_exists(string & table_name);
    int page_exists(const string &table_name, const char *tp, size_t pageId);
};


Database::Database(){
   
    
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
    table_bit_map |= (1 << i);
} 




// get the next row_dic to store the row_data
size_t Database::get_next_dir_off(int t_idx, int target_len, int & from_where){
    string table_name;
    map<string, int>::const_iterator iter = table_name_idx.cbegin();
    while (iter != table_name_idx.cend())
    {
        if (iter->second == t_idx)
        {
            table_name = iter->first;
        }
    }
    // 大小比较
    data_meta_header * data_meta_p = &dataMeta[t_idx];
    size_t target_off;
    if(data_meta_p->max_size > target_len){
        target_off = data_meta_p->max_unsorted;
        int max_len = 0;
        // row_dic data_dic_p = get_dir_off(data_meta_p->unsorted);
        if(data_meta_p->un_count > 1){
            size_t cur_p = data_meta_p->unsorted, prv_p;
            for(int i=0;i<data_meta_p->un_count;i++){
                if(i != 0){
                    cur_p = get_dir_next(prv_p);
                }
                if(cur_p == data_meta_p->max_unsorted){
                    if(i == 0){
                        data_meta_p->unsorted = get_dir_next(cur_p);
                    }else{
                        set_dir_next(prv_p, get_dir_next(cur_p));
                    }
                }
                prv_p = cur_p;
            }
        }else{
            data_meta_p->unsorted = 0;
            data_meta_p->max_size = 0;
        }
        data_meta_p->un_count -= 1;
    }else{
        // there are no proper empty unsorted block
        // wwe have to ccreate new block
        if(data_meta_p->slot == 0){
            expandExtend(table_name, "data");
        }
        target_off = data_meta_p->slot;
        from_where = 0;
    }

    return target_off;
    // if the free_ratio is too large
    // if(((page_header*)(memCache[0]+4096 * PagePos))->free_ratio > 80){
    //     if(dataMeta[t_idx].page_count == pageId + 1){
    //         expandExtend(table_name);
    //     }
    //     pageId += 1;
    // }

}



int Database::getNextRepPage(int taskId){
    int i=0;
    for(;i<PAGE_N;i++){
        if(ReQueue[taskId][i] == 0){
            return i;
        }
    }
    int target = ReQueue[taskId][i-1];
    memmove(&ReQueue[taskId][1], &ReQueue[taskId][0], PAGE_N);
    ReQueue[taskId][0] = target;
    return target;
}




void Database::set_last_next(int t_idx, size_t dir_off){
    size_t last_off = ((data_meta_header*)&dataMeta[t_idx])->last;
    set_dir_next(last_off, dir_off);
    dataMeta[t_idx].last = dir_off;
}
    
void Database::set_dir_next(size_t prv_dic, size_t next_dic_off){
    size_t pageId = (prv_dic >> 8) << 8;
    int page_in_off = 4096 - (1 + (prv_dic & 255)) * sizeof(row_dic);
    int off = (pageId + 1) * 4096 + page_in_off;
    void *buf = mmap(NULL, sizeof(row_dic), PROT_READ | PROT_WRITE, MAP_SHARED, tableData[t_idx], off);
    if (buf == MAP_FAILED)
    {
        Error("Set last next : mmap error");
    }
    // update
    ((row_dic *)buf)->next = next_dic_off;
    // mmap同步
    msync(buf, sizeof(row_dic), MS_SYNC);   // 同步后返回
    munmap(buf, sizeof(row_dic), MS_SYNC);
}

size_t Database::get_dir_next(size_t prv_dic){
    size_t pageId = (prv_dic >> 8) << 8;
    int page_in_off = 4096 - (1 + (prv_dic & 255)) * sizeof(row_dic);
    int off = (pageId + 1) * 4096 + page_in_off;
    void *buf = mmap(NULL, sizeof(row_dic), PROT_READ | PROT_WRITE, MAP_SHARED, tableData[t_idx], off);
    if (buf == MAP_FAILED)
    {
        Error("Set last next : mmap error");
    }
    // get
    return ((row_dic *)buf)->next;
}

int Database::get_page_count(string & table_name, char * expand_ob){
    int t_idx = table_name_idx[table_name];
    map<string, key_meta_header>::const_iterator iter = indexs_meta[t_idx].find(string(expand_ob));
    if(iter == indexs_meta[t_idx].cend()){
        LoadTableIndex(table_name, string(expand_ob));
    }
    return indexs_meta[t_idx][string(expand_ob)].page_count;
}


// ************* exists *****************//

bool Database::table_exists(string & table_name)
{
    fstream fs("table_name.txt", ios::out);
    char buffer[20];
    while (!fs.eof())
    {
        fs.getline(buffer, 20);
        if (strcmp(buffer, table_name.c_str()) == 0)
        {
            return true;
        }
    }
    return false;
}

int Database::page_exists(const string &table_name, const char *tp, size_t pageId)
{
    int i = 0, j = 0;
    if(pagetypes.size() > 0){
        for (i = 0; i < pagetypes[0].size(); i++)
        {
            if (pagetypes[0][i].table_name == table_name)
            {
                if (pagetypes[0][i].pageId == pageId)
                {
                    if (strcmp(pagetypes[0][i].type_name, tp) == 0)
                    {
                        return i;
                    }
                }
            }
        }
    }
    return -1;
};
