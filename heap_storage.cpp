#include <stdexcept>
#include "heap_storage.h"
/*
* Naive Test from Kevin
*/
bool test_heap_storage() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    return table1.test_unmarshal();
    // table1.create();
    // std::cout << "create ok" << std::endl;
    // table1.drop();  // drop makes the object unusable because of BerkeleyDB
    //                 //restriction -- maybe want to fix this some day
    // std::cout << "drop ok" << std::endl;
    // HeapTable table("_test_data_cpp", column_names, column_attributes);
    // table.create_if_not_exists();
    // std::cout << "create_if_not_exsts ok" << std::endl;
    // ValueDict row;
    // row["a"] = Value(12);
    // row["b"] = Value("Hello!");
    // std::cout << "try insert" << std::endl;
    // table.insert(&row);
    // std::cout << "insert ok" << std::endl;
    // Handles* handles = table.select();
    // std::cout << "select ok " << handles->size() << std::endl;
    // ValueDict *result = table.project((*handles)[0]);
    // std::cout << "project ok" << std::endl;
    // Value value = (*result)["a"];
    // if (value.n != 12)
    //     return false;
    // value = (*result)["b"];
    // if (value.s != "Hello!")
    //     return false;
    // table.drop();
    // return true;
}

// copied from instructor's code
typedef u_int16_t u16;

const char *ENV_DIR = "cpsc5300/data"; // the db dir

/* -------------SlotttedPage::DbBlock-------------*/
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

/**
 *  adds a new record to the block, assumes that the record itself has been
 * marshaled into the memory at data. Returns an id suitable for fetching
 * it back later with get().
 */
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    // memory-to-memory copy
    std::memcpy(this->address(loc), data->get_data(), size);
    return id;
}

/**
 * get a record's data for a given record id. It will have to
 * be unmarshaled by the client code (since only the client
 * knows how it was marshaled to store it) to expose the individual fields.
 */
Dbt* SlottedPage::get(RecordID record_id) {
    u16 size = get_n(4 * record_id);
    u16 loc = get_n(4 * record_id + 2);
    return new Dbt(this->address(loc), size);
}

/**
 * like add, but where we already know the record id. Could be used
 * to update the record's data or, for some file organizations, we
 * compute the record id based on the record's fields.
 */
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 curr_size, curr_loc;
    get_header(curr_size, curr_loc, record_id);
    u16 new_size = data.get_size();
    // if new_size is greater, need to make space by sliding records
    if(new_size > curr_size) {
        // if needs more space, do we have room?
        if(!has_room(new_size - curr_size)) {
            throw new DbBlockNoRoomError("Not enough room in block");
        }
        // slide right to left
        // update block
    }
    else {
        // update block
        // slide from left to right
    }
    // slide function updates headers, so grab header info again
    get_header(curr_size, curr_loc, record_id);
    put_header(record_id, new_size, curr_loc);
}

// remove a record from the block (by record id).
void SlottedPage::del(RecordID record_id) {
    // similar to put (slide stuff)
    // first check if id exists
    // put zeroes in the header of id = record_id
    // if yes, slide stuff over the record
    // slide(loc, loc + size)
}

// iterate through all the record ids in this block.
RecordIDs* SlottedPage::ids(void) {
    // we know the number of records
    // this->num_records
    // from 1 to num_records] we add those ids to the vector
    // RecordIDs and return it
    return 0;
}

// SlottedPage protected
// Get the size and offset for given id. For id of zero, it is the block header.
// The opposite of put
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

bool SlottedPage::has_room(u_int16_t size) {
    u16 free_space = this->end_free - (this->num_records + 1) * 4;
    return (size + 4) <= free_space;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    u16 shift = end - start;

    if(shift == 0) {
        return;
    }
    // if shift is negative, we need more space
    // slide records from right to left

    // if shift is positive, we have space left-over
    // slide records from left to right

    // update headers
    // loop through all ids
    // for ids loc <= start, we add the shift

    // update end_free
    this->end_free += shift;
    // update header for id=0
    this->put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

/* -------------HeapFile::DbFile-------------*/
// public
// HeapFile::HeapFile(std::string name){} // re-definittion

void HeapFile::create(void) {
    DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
    try {
	    env.open(ENV_DIR, DB_CREATE | DB_INIT_MPOOL, 0);
    } catch (DbException &exc) {
        std::cerr << "(sql5300: " << exc.what() << ")";
        env.close(0);
        exit(1);
    }
    _DB_ENV = &env;
}

void HeapFile::drop(void) {

}

void HeapFile::open(void) {
	_DB_ENV->open(ENV_DIR, 0, 0);
}

void HeapFile::close(void) {

}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

SlottedPage* HeapFile::get(BlockID block_id){
    return 0;
}

void HeapFile::put(DbBlock *block) {

}

BlockIDs* HeapFile::block_ids() {
    return 0;
}

// don't need re-definition
// u_int32_t HeapFile::get_last_block_id() {
//     return last;
// }

// protected
void HeapFile::db_open(uint flags) {

}

/* -------------HeapTable::DbRelation-------------*/
// I think we can use the DbRelation aliases
// More type aliases
// typedef std::string Identifier;
// typedef std::vector<Identifier> ColumnNames;
// typedef std::vector<ColumnAttribute> ColumnAttributes;
// typedef std::pair<BlockID, RecordID> Handle;
// typedef std::vector<Handle> Handles;  // FIXME: will need to turn this into an iterator at some point
// typedef std::map<Identifier, Value> ValueDict;

// Public
/**
 * takes the name of the relation, the columns (in order), and all
 * the column attributes (e.g., it's data type, any constraints, is
 * it allowed to be null, etc.) It's not the job of DbRelation to track
 * all this information. That's done by the schema storage.
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), file(table_name) {}

/**
 * corresponds to the SQL command CREATE TABLE. At minimum, it presumably
 * sets up the DbFile and calls its create method.
 */
void HeapTable::create() {
    try {
        file.create();
    }
    catch (DbRelationError e) {
        std::cerr << e.what() << std::endl;
    }
}

/**
 * corresponds to the SQL command CREATE TABLE IF NOT EXISTS. Whereas
 * create will throw an exception if the table already exists, this method
 * will just open the table if it already exists.
 */
void HeapTable::create_if_not_exists() {
    try {
        file.open();
    }
    catch (DbRelationError e) { // bdb.DBNoSuckFileError
        file.create();
    }
}

/**
 * corresponds to the SQL command DROP TABLE. Deletes the underlying DbFile.
 */
void HeapTable::drop() {
    file.drop();
}

/**
 * opens the table for insert, update, delete, select, and project methods
 */
void HeapTable::open() {
    file.open();
}

/**
 * closes the table, temporarily disabling insert, update, delete, select, and project methods.
 */
void HeapTable::close() {
    file.close();
}

/**
 * corresponds to the SQL command INSERT INTO TABLE. Takes a proposed row and adds
 * it to the table. This is the method that determines the block to write it to
 * and marshals the data and writes it to the block. It is also responsible for
 * handling any constraints, applying defaults, etc.
 * but we'll only handle two data types for now, INTEGER (or INT) and TEXT.
 * We won't handle any other column attributes or any NULL values.
 */
Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    return this->append(this->validate(row));
}

/**
 * corresponds to the SQL command UPDATE. Like insert, but only applies specific
 * field changes, keeping other fields as they were before. Same logic as insert
 * for constraints, defaults, etc. The client needs to first obtain a handle to
 * the row that is meant to be updated either from insert or from select.
 */
void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw std::logic_error("HeapTable::update method not implemented");
}

/**
 * corresponds to the SQL command DELETE FROM. Deletes a row for a given
 * row handle (obtained from insert or select).
 */
void HeapTable::del(const Handle handle) {
    throw std::logic_error("HeapTable::del method not implemented");
}

/**
 * corresponds to the SQL query SELECT * FROM...WHERE. Returns handles to the matching rows.
 * For select we'll ignore any WHERE or GROUP BY or LIMIT criteria. We will support the project method.
 * We won't yet support update or delete.
 */
Handles* HeapTable::select() {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles* HeapTable::select(const ValueDict* where) {
    throw std::logic_error("HeapTable::select method with where clause not implemented");
}

/**
 * extracts specific fields from a row handle (a projection).
 */
ValueDict* HeapTable::project(Handle handle) {
    // get recordID and blockID from handle
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    // use file to get block from blockID
    SlottedPage* block = file.get(block_id);
    // use block to get data from recordID
    Dbt* data = block->get(record_id);
    // unmarshal data to get a row
    ValueDict* row = HeapTable::unmarshal(data);
    // return row
    return row;
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
    ValueDict* row = HeapTable::project(handle);
    // it is the same with HeapTable::project(Handle handle), until here
    // return the whole row if column_names does not exist
    if ( column_names == nullptr) {
        return row;
    }
    // if there is column_names, loop through column_names and return the value
    // in that column_name
    ValueDict* new_row = new std::map<Identifier, Value>;
    for (auto const& column_name: *column_names) {
        new_row->insert(std::pair<Identifier, Value>(column_name,row->at(column_name)));
    }
    return new_row;
}

// protected
ValueDict* HeapTable::validate(const ValueDict *row) {
    ValueDict* full_row = new ValueDict;
    for (auto const& column_name: this->column_names) {
        ValueDict::const_iterator column = row->find(column_name);
        if(column == row->end()) {
            throw new DbInvalidRowError("Row missing column name " + column_name);
        }
        Value value = column->second;
        full_row->insert(std::pair<Identifier, Value>(column_name, value));
    }
    return full_row;
}

Handle HeapTable::append(const ValueDict *row) {
    // marshals the row into data -> binary representation
    Dbt *data = HeapTable::marshal(row);
    // find where to put that new data by geting the last block in the file
    SlottedPage *block = file.get(file.get_last_block_id());
    // in try, add data to the block and return the recordID returned
    RecordID record_id = 0;
    try {
        record_id = block->add(data);
    } // if there's a ValueError exception, block is full, so get new block
    catch (DbBlockNoRoomError e) {
        block = file.get_new();
        record_id = block->add(data);
    }
    // put the block back in the file
    file.put(block);
    // return a pair file.last, recordID // u_int32_t, u_int16_t
    return std::pair<unsigned int, short unsigned int>(file.get_last_block_id(),record_id);
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict* HeapTable::unmarshal(Dbt *data) {
    ValueDict *row = new ValueDict;
    char *output_data = (char *)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            int32_t n;
            memcpy( &n, (output_data + offset), sizeof(int32_t));
            Value val(n);
            row->insert(std::pair<Identifier, Value>(column_name, val));
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16 *)(output_data + offset); // size is in the first byte
            offset += sizeof(u16);
            std::string s(output_data + offset, size);
            Value val(s);
            row->insert(std::pair<Identifier, Value>(column_name, val));
            offset += size;
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
    }
    return row;
}

bool HeapTable::test_unmarshal() {
    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    Dbt *data = marshal(&row);
    ValueDict *result = unmarshal(data);
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    return true;
}