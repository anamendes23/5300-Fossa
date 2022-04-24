/**
 * @file heap_storage.cpp - Heap Storage Engine implementation
 * @author Ana Carolina de Souza Mendes, MSCS
 * @author Fangsheng Xu, MSCS
 * @see "Seattle University, CPSC5300, Spring 2022"
 * This is free and unencumbered software released into the public domain.
 */
#include <stdexcept>
#include <bitset>
#include "heap_storage.h"
/*
* Naive Test from Kevin
*/
bool test_heap_storage() {
    std::cout << "start test" << std::endl;
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;
    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;
    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12) {
        delete handles;
        delete result;
        return false;
    }
    value = (*result)["b"];
    if (value.s != "Hello!") {
        delete handles;
        delete result;
        return false;
    }
    table.drop();
    delete handles;
    delete result;
    return true;
}

// copied from instructor's code
typedef u_int16_t u16;

const char *ENV_DIR = "cpsc5300/data"; // the db dir

/* -------------SlotttedPage::DbBlock-------------*/
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id) {
    // Constructor provided by professor Lundeen
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

RecordID SlottedPage::add(const Dbt* data) {
    // Function provided by professor Lundeen
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

Dbt* SlottedPage::get(RecordID record_id) {
    u16 size = get_n(4 * record_id);
    u16 loc = get_n(4 * record_id + 2);

    // from Remi - fixed memory issue
    char* data = new char[size];
    memcpy(data, this->address(loc), size);
    // ----

    return new Dbt(data, size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 curr_size, curr_loc;
    get_header(curr_size, curr_loc, record_id);
    u16 new_size = data.get_size();
    // if new_size is greater, need to make space by sliding records
    if(new_size > curr_size) {
        // extra space need
        u16 extra_space = new_size - curr_size;
        // if needs more space, do we have room?
        if(!has_room(extra_space)) {
            throw new DbBlockNoRoomError("Not enough room in block");
        }
        // handle loc = 0, when the record had been deleted
        // get the virtual curr_loc based on record id that comes after that id
        // loop through all ids
        if (curr_loc == 0) {
            for (auto const& id: *ids()) {
                if (id > record_id) {
                    u16 temp_size, temp_loc;
                    get_header(temp_size, temp_loc, id);
                    if (temp_size > 0) {
                        curr_loc = temp_loc + temp_size;
                        break;
                    }
                }
            }
        }

        // what if all records were deleted? get end_free + 1
        if (curr_loc == 0) {
            curr_loc = end_free + 1;
        }

        // slide right to left
        slide(curr_loc, curr_loc - extra_space);
        // update block
        std::memcpy(this->address(curr_loc - extra_space), data.get_data(), new_size);
        curr_loc = curr_loc - extra_space;
    }
    else { // new_size is smaller
        // update block
        u16 new_loc = curr_loc + curr_size - new_size;
        std::memcpy(this->address(new_loc), data.get_data(), new_size);
        // slide from left to right
        slide(curr_loc, curr_loc + (curr_size - new_size));
        curr_loc = curr_loc + (curr_size - new_size);
    }
    // slide function updates headers
    put_header(record_id, new_size, curr_loc);
}

void SlottedPage::del(RecordID record_id) {
    // first check if id exists
    if (record_id > num_records) {
        throw new DbRecordIdNotFound("Record id does not exist: " + record_id);
    }
    // assign current size and current location of the record
    u16 curr_size, curr_loc;
    get_header(curr_size, curr_loc, record_id);
    // put zeroes in the header of id = record_id, size = 0, loc = 0
    put_header(record_id, 0, 0);
    // slide remaining records over the unused space
    slide(curr_loc, curr_loc + curr_size);
}

RecordIDs* SlottedPage::ids(void) {
    RecordIDs *record_ids = new RecordIDs;
    // we know the number of records
    // from 1 to num_records] we add those ids to the vector
    for(int i = 1; i <= this->num_records; i++) {
        record_ids->push_back(i);
    }
    return record_ids;
}

// SlottedPage protected
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    size = get_n(4 * id); // 2 bytes
    loc = get_n(4 * id + 2); // 2 bytes
}

void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    // Function provided by professor Lundeen
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size); // 2 bytes
    put_n(4 * id + 2, loc); // 2 bytes
}

bool SlottedPage::has_room(u_int16_t size) {
    u16 free_space = this->end_free - (this->num_records + 1) * 4;
    return (size + 4) <= free_space;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    int shift = end - start;

    if(shift == 0) {
        return;
    }
    // if shift is negative, we need more space
    // slide records from right to left
    // if shift is positive, we have space left-over
    // slide records from left to right
    // move_loc is where the last record starts
    u16 move_loc = this->end_free + 1;
    u16 move_size = start - move_loc;
    u16 new_loc = move_loc + shift;
    // current records;
    Dbt temp_data(this->address(move_loc), move_size);
    std::memcpy(this->address(new_loc), this->address(move_loc), move_size);

    // update headers
    u16 size, loc;
    // loop through all ids
    for (auto const& record_id: *ids()) {
        get_header(size, loc, record_id);
        // for ids loc <= start, we add the shift
        if(loc != 0 && loc <= start) {
            loc += shift;
            put_header(record_id, size, loc);
        }
    }
    // update end_free
    this->end_free += shift;
    // update header for id=0
    this->put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    // Function provided by professor Lundeen
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    // Function provided by professor Lundeen
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    // Function provided by professor Lundeen
    return (void*)((char*)this->block.get_data() + offset);
}

/* -------------HeapFile::DbFile-------------*/
// public
void HeapFile::create(void) {
    this->db_open(DB_CREATE | DB_TRUNCATE);
    // get a new block and put it in the file
    SlottedPage* block = this->get_new();
    this->put(block);

    delete block;
}

void HeapFile::drop(void) {
    this->close();
    // delete database file
    const char **homep = new const char*[255];
    _DB_ENV->get_home(homep);
    std::string path = std::string(*homep) + + "/" + dbfilename;
    int delete_result = std::remove(path.c_str());
    delete homep;
    if (delete_result != 0) {
        throw FailToRemoveDbfile ("failed to remove the physical file " + path);
    }
}

void HeapFile::open(void) {
    // call dp_open without flags
    this->db_open();
}

void HeapFile::close(void) {
    this->db.close(0);
    // helpful for checking if the db is closed
    this->closed = true;
}

SlottedPage* HeapFile::get_new(void) {
    // Function provided by professor Lundeen
    // minor changes to fix functionality
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, DbBlock::BLOCK_SZ);
    Dbt data(block, DbBlock::BLOCK_SZ);

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    Dbt slottedPageData(block, DbBlock::BLOCK_SZ);
    this->db.get(nullptr, &key, &slottedPageData, 0);

    SlottedPage* page = new SlottedPage(slottedPageData, this->last, true);
    return page;
}

SlottedPage* HeapFile::get(BlockID block_id) {
    // allocate an empty block
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));
    Dbt key(&block_id, sizeof(block_id));
    // get data from Berkley DB and store in empty block
    this->db.get(nullptr, &key, &data, 0);
    // create slotted page from that block
    SlottedPage* page = new SlottedPage(data, block_id, false);
    return page;
}

void HeapFile::put(DbBlock *block) {
    BlockID block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    // &data should be the same thing as block->get_block()
    this->db.put(NULL, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids() {
    // BlockIDs is a vector<BlockID>
    // BlockID is a u_int32_t type
    BlockIDs* block_ids = new BlockIDs;
    // loop through all the block ids and return the vector
    for (u_int32_t i = 1; i <= this->last; i++) {
        block_ids->push_back(i);
    }
    return block_ids;
}

// protected
void HeapFile::db_open(uint flags) {
    // check if closed/exist
    if(this->closed) {
        db.set_message_stream(_DB_ENV->get_message_stream());
        db.set_error_stream(_DB_ENV->get_error_stream());
        // set the record leng (db.set_re_len) as the block_size
        db.set_re_len(DbBlock::BLOCK_SZ);

        this->dbfilename = this->name + ".db";
        // dbtype is DB_RECNO
        int result = db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);
        if(result != 0) {
            // if opening doesn't sucessed, we close
            this->close();
            this->closed = true;
        }
        this->closed = false;
    }
}

/* -------------HeapTable::DbRelation-------------*/
// Public
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
    : DbRelation(table_name, column_names, column_attributes), file(table_name) {}

void HeapTable::create() {
    try {
        file.create();
    }
    catch (DbRelationError &e) {
        std::cerr << e.what() << std::endl;
    }
}

void HeapTable::create_if_not_exists() {
    try {
        file.create();
    }
    catch (DbRelationError &e) {
        file.open();
    }
}

void HeapTable::drop() {
    file.drop();
}

void HeapTable::open() {
    file.open();
}

void HeapTable::close() {
    file.close();
}

Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    ValueDict *full_row = this->validate(row);
    Handle handle = this->append(full_row);
    delete full_row;
    return handle;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    throw std::logic_error("HeapTable::update method not implemented");
}

void HeapTable::del(const Handle handle) {
    throw std::logic_error("HeapTable::del method not implemented");
}

Handles* HeapTable::select() {
    // Function provided by professor Lundeen
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(std::make_pair(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles* HeapTable::select(const ValueDict* where) {
    throw std::logic_error("HeapTable::select method with where clause not implemented");
}

ValueDict* HeapTable::project(Handle handle) {
    // get recordID and blockID from handle
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    // use file to get block from blockID
    SlottedPage* block = file.get(block_id);
    // use block to get data from recordID
    Dbt* data = block->get(record_id);
    // unmarshal data to get a row
    ValueDict* row = this->unmarshal(data);
    // deallocate memory
    delete block;
    delete data;
    // return row
    return row;
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
    ValueDict* row = this->project(handle);
    // it is the same with HeapTable::project(Handle handle), until here
    // return the whole row if column_names does not exist
    if ( column_names == nullptr) {
        return row;
    }
    // if there is column_names, loop through column_names
    // and return the value in that column_name
    ValueDict* new_row = new std::map<Identifier, Value>;
    for (auto const& column_name: *column_names) {
        new_row->insert(std::make_pair(column_name,row->at(column_name)));
    }
    delete row;

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
        full_row->insert(std::make_pair(column_name, value));
    }
    return full_row;
}

Handle HeapTable::append(const ValueDict *row) {
    // marshals the row into data -> binary representation
    Dbt *data = this->marshal(row);
    // find where to put that new data by geting the last block in the file
    SlottedPage *block = file.get(file.get_last_block_id());
    // in try, add data to the block and return the recordID returned
    RecordID record_id = 0;
    try {
        record_id = block->add(data);
    } // if there's a ValueError exception, block is full, so get new block
    catch (DbBlockNoRoomError &e) {
        block = file.get_new();
        record_id = block->add(data);
    }
    // put the block back in the file
    file.put(block);
    delete data;
    delete block;
    // return a pair file.last, recordID
    return std::make_pair(file.get_last_block_id(),record_id);
}

Dbt* HeapTable::marshal(const ValueDict* row) {
    // Function provided by professor Lundeen
    // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    char *bytes = new char[DbBlock::BLOCK_SZ];
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
    Dbt *data = new Dbt(right_size_bytes, offset);
    delete[] bytes;
    delete[] right_size_bytes;
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
            row->insert(std::make_pair(column_name, val));
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16 *)(output_data + offset); // size is in the first byte
            offset += sizeof(u16);
            std::string s(output_data + offset, size);
            Value val(s);
            row->insert(std::make_pair(column_name, val));
            offset += size;
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
    }
    delete[] output_data;
    return row;
}

bool HeapTable::test_unmarshal() {
    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    Dbt *data = marshal(&row);
    ValueDict *result = unmarshal(data);
    Value value = (*result)["a"];
    if (value.n != 12) {
        delete data;
        delete result;
        return false;
    }
    value = (*result)["b"];
    if (value.s != "Hello!") {
        delete data;
        delete result;
        return false;
    }
    delete data;
    delete result;
    return true;
}
