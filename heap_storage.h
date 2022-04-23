/**
 * @file heap_storage.h - Implementation of storage_engine with a heap file structure.
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2022"
 */
#pragma once

// added these to support std::memcpy
#include <stdio.h>
#include <stdlib.h>
#include<cstring>

// comes with milestone 1 starter files
#include "db_cxx.h"
#include "storage_engine.h"

using namespace std;
extern DbEnv *_DB_ENV;

/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 *      Manage a database block that contains several records.
        Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.

        Record id are handed out sequentially starting with 1 as records are added with add().
        Each record has a header which is a fixed offset from the beginning of the block:
            Bytes 0x00 - Ox01: number of records
            Bytes 0x02 - 0x03: offset to end of free space
            Bytes 0x04 - 0x05: size of record 1
            Bytes 0x06 - 0x07: offset to record 1
            etc.
 *
 */
class SlottedPage : public DbBlock {
public:
    // constuctor
    SlottedPage(Dbt &block, BlockID block_id, bool is_new = false);

    // Big 5 - we only need the destructor, copy-ctor, move-ctor, and op= are unnecessary
    // but we delete them explicitly just to make sure we don't use them accidentally

    // destructor
    virtual ~SlottedPage() {}

    // copy-ctor -> don't need implementation
    SlottedPage(const SlottedPage &other) = delete;

    //  move-ctor -> don't need implementation
    SlottedPage(SlottedPage &&temp) = delete;

    // op= -> don't need implementation
    SlottedPage &operator=(const SlottedPage &other) = delete;

    // op= -> don't need implementation
    SlottedPage &operator=(SlottedPage &temp) = delete;

    /**
     * adds a new record to the block, assumes that the record itself has been
     * marshaled into the memory at data. Returns an id suitable for fetching
     * it back later with get().
     * @param data content
     * @return an corresponding id for data
     */
    virtual RecordID add(const Dbt *data);

    /**
     * get a record's data for a given record id. It will have to
     * be unmarshaled by the client code (since only the client
     * knows how it was marshaled to store it) to expose the individual fields.
     * @param record_id corresponding record id
     * @return record's data
     */
    virtual Dbt *get(RecordID record_id);

    /**
     * updates the record's data or, for some file organizations
     * like add, but where we already know the record id.
     * we compute the record id based on the record's fields.
     * @param record_id record id
     * @param data data content
     */
    virtual void put(RecordID record_id, const Dbt &data);

    /**
    * remove a record from the block (by record id).
    * @param record record's id
    */
    virtual void del(RecordID record_id);

    /**
    * iterate through all the record ids in this block.
    * @return an array of all records' id
    */
    virtual RecordIDs *ids(void);

protected:
    u_int16_t num_records; // the number of records
    u_int16_t end_free; // address of the last free byte

    /**
    * Get the size and offset for given id. For id of zero, it is the block header. The opposite of put()
    * @param size size of the id
    * @param loc offect of the id
    * @param id record id
    */
    virtual void get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0);

    /**
    * Store the size and offset for given id. For id of zero, store the block header(meta header for the block).
    * @param size size of the id; for meta header, it is number of the records
    * @param loc offect of the id; for meta header, it is the last free byte
    * @param id record id
    */
    virtual void put_header(RecordID id = 0, u_int16_t size = 0, u_int16_t loc = 0);

    /**
    * has enough extra room to store data
    * @param size of data, number of recordrs
    * @return if there is equal or more than number of "size" records available in the current block
    */
    virtual bool has_room(u_int16_t size);

    /**
     * slide a record in the block, and thus slide free space and all records on its left
     * p.s. locationi of a data record means location of the first byte of the record
     * @param start location of data record before sliding
     * @param end location of data record after sliding
     */
    virtual void slide(u_int16_t start, u_int16_t end);

    /**
     * get data given the offset
     * @param offset record's offset from the first byte in the block
     * @return address of the record
     */
    virtual u_int16_t get_n(u_int16_t offset);

    /**
     * change data give the offset, it does not change the size of the data(only content)
     * @param offset record's offset from the first byte in the block
     * @return address of the record
     */
    virtual void put_n(u_int16_t offset, u_int16_t n);

    /**
     * get a records's physical address
     * @param offset from the 1st record in the block
     * @return physical address of the record
     */
    virtual void *address(u_int16_t offset);
};

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
 */
class HeapFile : public DbFile {
public:
    /**
     * constructor
     * @param name of the db file
     */
    HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {}

    // not implemented
    virtual ~HeapFile() {}

    // not implemented
    HeapFile(const HeapFile &other) = delete;

    // not implemented
    HeapFile(HeapFile &&temp) = delete;

    // not implemented
    HeapFile &operator=(const HeapFile &other) = delete;

    // not implemented
    HeapFile &operator=(HeapFile &&temp) = delete;

    /**
     * create the database file that will store the blocks for this relation.
     */
    virtual void create(void);

    /**
     * delete() in python code. Delete the physical file.
     */
    virtual void drop(void);

    /**
     * open the database file.
     */
    virtual void open(void);

    /** close the database file.
     */
    virtual void close(void);

    /**
     * create a new empty block and add it to the database file.
     * @return a new block to be modified by the client via the DbBlock interface.
     */
    virtual SlottedPage *get_new(void);

    /**
     * get a block from the database file (via the buffer manager, presumably) for a given block id.
     * The client code can then read or modify the block via the DbBlock interface.
     * @param block_id  which block to get
     * @returns pointer to the DbBlock (freed by caller)
     */
    virtual SlottedPage *get(BlockID block_id);

    /**
     * write a block to the file. Presumably the client has made modifications in the block that
     * he would like to save. Typically, it's up to the buffer manager exactly when the block is
     * actually written out to disk.
     * @param block the block to write (overwrites existing block on disk)
     */
    virtual void put(DbBlock *block);

    /** iterate through all the block ids in the file.
     * @return  @returns  a pointer to vector of BlockIDs (freed by caller), includes deleted blocks)
     */
    virtual BlockIDs *block_ids();

    /**
     * get the last block's id
     * @return the last block's id
     */
    virtual u_int32_t get_last_block_id() { return last; }

protected:
    std::string dbfilename; // db file name
    u_int32_t last; // last block's id
    bool closed; // db file is close or not(can't open a closed file)
    Db db; // db's physical environment

    /** Wrapper for Berkeley DB open, which does both open and creation.
     * @param flags flag for the DbEnv class to open a BerkleyDB
     */
    virtual void db_open(uint flags = 0);
};

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */
class HeapTable : public DbRelation {
public:
    /**
     * ctor
     * takes the name of the relation, the columns (in order), and all
     * the column attributes (e.g., it's data type, any constraints, is
     * it allowed to be null, etc.) It's not the job of DbRelation to track
     * all this information. That's done by the schema storage.
     * @param table_name name of the table
     * @param coumns_names an array of column names
     * @param column_attributes an array of actual data for each column
     */
    HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes);


    // dtor -> not implemented
    virtual ~HeapTable() {}

    // not implemented
    HeapTable(const HeapTable &other) = delete;

    // not implemented
    HeapTable(HeapTable &&temp) = delete;

    // not implemented
    HeapTable &operator=(const HeapTable &other) = delete;

    // not implemented
    HeapTable &operator=(HeapTable &&temp) = delete;

    /**
     * Execute: CREATE TABLE <table_name> ( <columns> )
     * Assumes the metadata and validation are already done.
     * corresponds to the SQL command CREATE TABLE. At minimum, it presumably
     * sets up the DbFile and calls its create method.
     */
    virtual void create();

    /**
     * corresponds to the SQL command CREATE TABLE IF NOT EXISTS. Whereas
     * create will throw an exception if the table already exists, this method
     * will just open the table if it already exists.
     */
    virtual void create_if_not_exists();

    /**
     * Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
     * Assumes the metadata and validate are already done.
     * corresponds to the SQL command DROP TABLE. Deletes the underlying DbFile.
     */
    virtual void drop();

    /**
     * opens the table for insert, update, delete, select, and project methods
     */
    virtual void open();

    /**
     * closes the table, temporarily disabling insert, update, delete, select, and project methods.
     */
    virtual void close();

    /**
     * corresponds to the SQL command INSERT INTO TABLE. Takes a proposed row and adds
     * it to the table. This is the method that determines the block to write it to
     * and marshals the data and writes it to the block. It is also responsible for
     * handling any constraints, applying defaults, etc.
     * but we'll only handle two data types for now, INTEGER (or INT) and TEXT.
     * We won't handle any other column attributes or any NULL values.
     * @param row row's data (an array of pair of column name and its value)
     * @return the location of the new row (a pair of BlockID and RecordID)
     */
    virtual Handle insert(const ValueDict *row);

    /**
     * corresponds to the SQL command UPDATE. Like insert, but only applies specific
     * field changes, keeping other fields as they were before. Same logic as insert
     * for constraints, defaults, etc. The client needs to first obtain a handle to
     * the row that is meant to be updated either from insert or from select.
     * @param handle the location of the new row(a pair of BlockID and RecordID)
     * @param new_values row's data (an array of pair of column name and its value)
     */
    virtual void update(const Handle handle, const ValueDict *new_values);

    /**
     * corresponds to the SQL command DELETE FROM. Deletes a row for a given
     * row handle (obtained from insert or select).
     * @param handle the location of the row
     */
    virtual void del(const Handle handle);

    /**
     * Return an array of all records' handle
     * corresponds to the SQL query SELECT * FROM...WHERE. Returns handles to the matching rows.
     * For select we'll ignore any WHERE or GROUP BY or LIMIT criteria. We will support the project method.
     * We won't yet support update or delete.
     * @return a array of all rows' handle
     */
    virtual Handles *select();

    // WHERE... is not supported for this sprint
    virtual Handles *select(const ValueDict *where);

    /**
     * extracts a row from the table (a projection).
     * @param handle locatiton of the row
     */
    virtual ValueDict *project(Handle handle);

    /**
     * extracts specific fields from a row handle (a projection).
     * @param handle locatiton of the row
     * @param column_names fields to extract
     */
    virtual ValueDict *project(Handle handle, const ColumnNames *column_names);

    /**
     * test unmarshall()
     * developer's own unit test
     */
    virtual bool test_unmarshal();
protected:
    HeapFile file;

    /**
     * validate the content of the row.
     * prints out error message in the console if error occurs
     * @param row the location of the row
     */
    virtual ValueDict *validate(const ValueDict *row);

    /**
     * appends one more row in the table
     * @param row the location of the new row
     * @return the handle of the new row
     */
    virtual Handle append(const ValueDict *row);

    /**
     * return the bits to go into the file.
     * caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
     * @return the address of the Dbt
     */
    virtual Dbt *marshal(const ValueDict *row);

    /**
     * decode the content in Dbt and return ValueDict
     * @param data address of the data
     * @return content of the row
     */
    virtual ValueDict *unmarshal(Dbt *data);
};

/**
 * Professor's test
 */
bool test_heap_storage();

