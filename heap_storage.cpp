#include "heap_storage.h"
/*
* Naive Test from Kevin
*/
bool test_heap_storage();

// copied from instructor's code
typedef u_int16_t u16;

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

// not sure if we need it
// SlottedPage::~SlottedPage() {
// }

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

Dbt* SlottedPage::get(RecordID record_id) {
    return 0;
}


void SlottedPage::put(RecordID record_id, const Dbt &data) {
    // nothing
}

void SlottedPage::del(RecordID record_id) {
    //
}

RecordIDs* SlottedPage::ids(void) {
    return 0;
}

// SlottedPage protected
// typedef u_int16_t RecordID;
// typedef u_int32_t BlockID;
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {

}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

bool SlottedPage::has_room(u_int16_t size) {
    return false;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {

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

}

void HeapFile::drop(void) {

}

void HeapFile::open(void) {

}

void HeapFile::close(void) {

}

SlottedPage* HeapFile::get_new(void) {
    return 0;
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

// More type aliases
typedef std::string Identifier;
typedef std::vector<Identifier> ColumnNames;
typedef std::vector<ColumnAttribute> ColumnAttributes;
typedef std::pair<BlockID, RecordID> Handle;
typedef std::vector<Handle> Handles;  // FIXME: will need to turn this into an iterator at some point
typedef std::map<Identifier, Value> ValueDict;

// Public

// HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) {

// }

void HeapTable::create() {

}

void HeapTable::create_if_not_exists() {

}

void HeapTable::drop() {

}

void HeapTable::open() {

}

void HeapTable::close() {

}

Handle HeapTable::insert(const ValueDict *row) {
    return std::pair<unsigned int, short unsigned int>(0,0);
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {

}

void HeapTable::del(const Handle handle) {

}

Handles* HeapTable::select() {
    return nullptr;
}

Handles* HeapTable::select(const ValueDict* where) {
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

ValueDict* HeapTable::project(Handle handle) {
    return nullptr;
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
    return nullptr;
}

// protected
ValueDict* HeapTable::validate(const ValueDict *row) {
    return nullptr;
}

Handle HeapTable::append(const ValueDict *row) {
    return std::pair<unsigned int, short unsigned int>(0,0);
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
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
    return nullptr;
}


/* -------------Naive Test-------------*/
bool test_heap_storage() {
    return true;
}



