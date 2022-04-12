#include "heap_storage.h"
#include "storage_engine.h"
bool test_heap_storage() {return true;}

/*
 * Convenient aliases for types
 */
typedef u_int16_t RecordID;
typedef u_int32_t BlockID;
typedef std::vector<RecordID> RecordIDs;
typedef std::length_error DbBlockNoRoomError;

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id) {
}

// SlottedPage::~SlottedPage() {
// }

RecordID SlottedPage:: add(const Dbt *data) {
    return 0;
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

void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {

}

bool SlottedPage::has_room(u_int16_t size) {
    return false;
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {

}

u_int16_t SlottedPage::get_n(u_int16_t offset) {
    return 0;
}

void SlottedPage::put_n(u_int16_t offset, u_int16_t n) {

}

void* SlottedPage::address(u_int16_t offset) {
    return 0;
};
