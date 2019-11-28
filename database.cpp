#include "database.hpp"

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

// File structure
// table_name
// fields_count
// 



struct table_file {
	char table_name[20];
	int field_count;
	map<string, string> fields;
};
using Table_Meta = struct table_file;

void Database::CreateFile(string table_name, map<string, string> fields_result) {
	Table_Meta t_m;
	fstream tb_f("table_name");
	char buffer[20];
	while (!tb_f.eof()) {
		tb_f.getline(buffer, 20);
		if (strcmp(buffer, table_name.c_str()) == 0) {
			Error("Table name exists");
		}
	}
	if (table_name.size() > 20)
	{
		Error("Table name length too long!");
	}
	tb_f << table_name << endl;


	t_m.field_count = fields_result.size();
	strcpy(t_m.table_name, table_name.c_str());
	t_m.fields = fields_result;
	ifstream d_f(table_name + ".db");
	ifstream k_f(table_name + ".key");
	ifstream d_f(table_name + ".idx");
	

}