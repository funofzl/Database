#ifndef BPT_H
#define BPT_H

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

// #ifndef UNIT_TEST
// #include "predefined.h"
// #else
// #include "util/unit_test_predefined.h"
// #endif

namespace bpt {

/* offsets */
#define OFFSET_META 0
#define OFFSET_BLOCK ((OFFSET_META + sizeof(key_meta_t)) / 4096 + 1) * 4096
#define SIZE_NO_CHILDREN sizeof(leaf_node_t<key_t>) - BP_ORDER * sizeof(record_t)

#define BP_ORDER 20

enum record_type{PRIMARY_KEY, INDEX_KEY, DATA};

/* meta information of B+ tree */

// two kinds of meta header ((index)key and data) 
/* contains node(internal and leaf node) */
/* standord size(72) Bytes */
struct key_meta_t{
    int height;            /* height of tree (exclude leafs) */
    int key_size;   /* size of per key */
    int internal_node_num; /* how many internal nodes */
    int leaf_node_num;     /* how many leafs */
    unsigned int page_count; /* how many pages */
    unsigned int un_count;  /* unsorted_map link area count  */
    unsigned int max_size;  /* max_unsorted size */
    size_t  unsorted;    /* empty key area lnk (when insert few data it will will be used) */ 
    size_t  max_unsorted;    /* max size unsorted (56 + 8 ==> data_dir_off)*/

    size_t  slot;        /* where to store new block */

    size_t  root_offset; /* where is the root of internal nodes */
    size_t  leaf_offset; /* where is the first leaf */
};
using key_meta_header = struct key_meta_t;

/* standord size(40) Bytes */
struct data_meta_t{
    unsigned int data_count;  /* data count */
    unsigned int page_count;    /* page count */
    size_t  begin_offset; /* where is the root of internal nodes */
    size_t  slot;        /* where to store new block pageId + offset */ 
    size_t  unsorted;    /* empty key area lnk (when insert few data it will will be used) */ 
    size_t  max_unsorted;    /* max size unsorted */
    unsigned int un_count;  /* unsorted_map link area count  */
    unsigned int max_size;  /* max_unsorted size */
    size_t last = 0;        /* store thee last data_dir offset(56 + 8) */
};
using data_meta_header = struct data_meta_t;


/* page header size(16) Bytes*/
struct page_t{
    char page_id;   /* page_id */
    char record_type;  /* 记录的数据类型(data  (index)key)   from enum record_type */ 

    short free_space;   /* free space */
    short row_space;    /* row data space */
    short row_dic_size; /* row dic space */

    short data_offset;
    float free_ratio;
};
using page_header = struct page_t;


/* row header (standord (16 Bytes) + data_len)*/
struct row_t{
    char status;    /* 0碎片，1普通记录, 2他处数据引用(data_len可用，其他无用) */
    char dic_off;   /* to find the offset of the row_dic (which is sorted) */
    unsigned short data_len;    /* store the real length of row_data */
    unsigned short null_map_size; /* field */
    short null_map; // a part of null_map if sizeof(null_map) > 16 there will be new null_map after row_t but before data
    size_t  next;  /* --row_id-- like pointer point to next raw  (64 = 50 + 12 ) 
                    50 stand for page_id and 12 stand for the address of a row in per page */  
    /* null_map */
    /* data */
};
using row_header = struct row_t;



/* row directory */
// 即使删除 空间不会回收 所以不需要标记id, 顺着来就行, 中间空间合并后后续顺序也不变，因为id只是个标志而已, 不一定有顺序
struct row_direc_t{
    short row_len;  // 第一位为1时是负数，表示此块被弃用, 表示被前或后合并掉了
                    // 平常>0 因为需要存储所指向的row_data的可用大小
    short row_off;
    size_t next;    // 56 + 8  (8 stand for position in page, 56->pageId)
};
using row_dic = struct row_direc_t;








/* internal nodes' index segment */
template<class key_t>
struct index_t {
    key_t key;      /* according to the condition, it will be string, int */
    size_t  child; /* child's offset */
};

/***
 * internal node block
 ***/
template<class key_t>
struct internal_node_t {
    typedef index_t<key_t> * child_t;

    size_t  parent; /* parent node offset */
    size_t  next;
    size_t  prev;
    size_t  n; /* how many children */
    index_t<key_t> children[BP_ORDER];
};


// the address of data (not directly store data)
// typedef size_t  value_t;


/* the final record of value */
template<class key_t, class value_t>
class record_t {
	key_t key;
    value_t value;
};

/* leaf node block */
template<class key_t, class value_t>
struct leaf_node_t {
    typedef record_t<key_t, value_t> *child_t;

    size_t  parent; /* parent node offset */
    size_t  next;
    size_t  prev;
    size_t  n;
    record_t<key_t, value_t> children[BP_ORDER];
};






class Tree(){};

/* the encapulated B+ tree */
template<class key_t, class value_t>
class bplus_tree : public Tree{
public:
    bplus_tree(const char *path, bool force_empty = false);

    
    /* abstract operations */
    int search(const key_t & key, value_t *value) const;
    int search_range(key_t *left, const key_t &right,
                     value_t *values, size_t  max, bool *next = NULL) const;
    int remove(const key_t& key);
    int insert(const key_t& key, value_t value);
    int update(const key_t& key, value_t value);
    key_meta_header get_meta() const
    {
        return meta;
    };

#ifndef UNIT_TEST
private:
#else
public:
#endif
    char path[512];
    key_meta_header meta;

    /* init empty tree */
    // void init_from_empty();
    static void init_from_empty();

    /* find index */
    size_t search_index(const key_t &key) const;

    /* find leaf */
    size_t search_leaf(size_t index, const key_t &key) const;
    size_t search_leaf(const key_t &key) const
    {
        return search_leaf(search_index(key), key);
    }

    /* remove internal node */
    void remove_from_index(size_t offset, internal_node_t<key_t> &node,
                           const key_t &key);

    /* borrow one key from other internal node */
    bool borrow_key(bool from_right, internal_node_t<key_t> &borrower,
                    size_t offset);

    /* borrow one record from other leaf */
    bool borrow_key(bool from_right, leaf_node_t<key_t> &borrower);

    /* change one's parent key to another key */
    void change_parent_child(size_t parent, const key_t &o, const key_t &n);

    /* merge right leaf to left leaf */
    void merge_leafs(leaf_node_t<key_t> *left, leaf_node_t<key_t> *right);

    void merge_keys(index_t<key_t> *where, internal_node_t<key_t> &left,
                    internal_node_t<key_t> &right, bool change_where_key = false);

    /* insert into leaf without split */
    void insert_record_no_split(leaf_node_t<key_t> *leaf,
                                const key_t &key, const value_t &value);

    /* add key to the internal node */
    void insert_key_to_index(size_t offset, const key_t &key,
                             size_t value, size_t after);
    void insert_key_to_index_no_split(internal_node_t<key_t> &node, const key_t &key,
                                      size_t value);

    /* change children's parent */
    void reset_index_children_parent(index_t<key_t> *begin, index_t<key_t> *end,
                                     size_t parent);

    template<class T>
    void node_create(size_t offset, T *node, T *next);

    template<class T>
    void node_remove(T *prev, T *node);

    /* multi-level file open/close */
    mutable FILE *fp;
    mutable int fp_level;
    void open_file(const char *mode = "rb+") const
    {
        // `rb+` will make sure we can write everywhere without truncating file
        if (fp_level == 0)
            fp = fopen(path, mode);

        ++fp_level;
    }

    void close_file() const
    {
        if (fp_level == 1)
            fclose(fp);

        --fp_level;
    }

    /* alloc from disk */
    size_t alloc(size_t  size)
    {
        size_t slot = meta.slot;
        meta.slot += size;
        return slot;
    }

    size_t alloc(leaf_node_t<key_t> *leaf)
    {
        leaf->n = 0;
        meta.leaf_node_num++;
        return alloc(sizeof(leaf_node_t<key_t>));
    }

    size_t alloc(internal_node_t<key_t> *node)
    {
        node->n = 1;
        meta.internal_node_num++;
        return alloc(sizeof(internal_node_t<key_t>));
    }

    void unalloc(leaf_node_t<key_t> *leaf, size_t offset)
    {
        --meta.leaf_node_num;
    }

    void unalloc(internal_node_t<key_t> *node, size_t offset)
    {
        --meta.internal_node_num;
    }

    /* read block from disk */
    int map(void *block, size_t offset, size_t  size) const
    {
        open_file();
        fseek(fp, offset, SEEK_SET);
        size_t  rd = fread(block, size, 1, fp);
        close_file();

        return rd - 1;
    }

    template<class T>
    int map(T *block, size_t offset) const
    {
        return map(block, offset, sizeof(T));
    }

    /* write block to disk */
    int unmap(void *block, size_t offset, size_t  size) const
    {
        open_file();
        fseek(fp, offset, SEEK_SET);
        size_t wd = fwrite(block, size, 1, fp);
        close_file();

        return wd - 1;
    }

    template<class T>
    int unmap(T *block, size_t offset) const
    {
        return unmap(block, offset, sizeof(T));
    }

    int add_extent(char * table_name, int page_type){
        strcpy(path, table_name);
        open_file();
        char buffer[1024] = {'\0'};
        size_t page_id = meta.page_count;
        meta.page_count += 1;
        sprintf(buffer, "%d", page_id);
        int temp = (unsigned int)(page_type<<16)+(unsigned int)(1024-sizeof(page_t);
        // strcpy(buffer, "%d",temp);
        fwrite(buffer, 4096*2, 1, fp);
    }
};

// template<class key_t>
// inline int keycmp(const key_t &a, const key_t &b) {
//     int x = strlen(a.k) - strlen(b.k);
//     return x == 0 ? strcmp(a.k, b.k) : x;
// }


// template<class key_t, class type>
// bool operator< (const key_t &l, const type &r) {
//     return keycmp(l, r.key) < 0;
// }
// template<class key_t, class type>
// bool operator< (const type &l, const key_t &r) {
//     return keycmp(l.key, r) < 0;
// }
// template<class key_t, class type>
// bool operator== (const key_t &l, const type &r) {
//     return keycmp(l, r.key) == 0;
// }
// template<class key_t, class type>
// bool operator== (const type &l, const key_t &r) {
//     return keycmp(l.key, r) == 0;
// }







} /* namespace bpt */

#endif /* end of BPT_H */


;
