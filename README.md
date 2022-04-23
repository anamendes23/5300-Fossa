# 5300-Fossa

## Sprint Verano

The goal for Spring Verano was to build a skeleton for our Relation Manager project.
We started by building a SQL shell in Milestone 1, which simply reads a SQL statement, parses it, and prints it back to the user or indicates if it is not a valid query.
In Milestone 2, we implemented a rudimentary Storage Engine using a Heap data structure. More details in the sections below.

Team: Ana Mendes, Fangsheng Xu

### **Set up**

#### **Build:**
- Update the INCLUDE and LIBRARY variables in the Makefile to match the locations of Berkley DB and SQLParser
- Run `$ make` to compile the solution

#### **Usage:**
After compiling, run the following command to start the SQL shell:
`$ ./sql5300 [PATH]/data`
To test the storage engine, use the `test` command:
`$ SQL> test`
To exit the SQL shell, use the `quit` command:
`$ SQL> quit`

### **Milestone 1**

SQL shell that runs on an instance of a Berkley DB environment and parses SQL statements input by user.

#### **Features**
* Supports CREATE statement
* Supports SELECT statements with:
<br />&nbsp;- Multiple FROM tables
<br />&nbsp;- JOIN clauses
<br />&nbsp;- WHERE clauses


### **Milestone 2**

Rudimentary storage engine built using a Heap data structure. The heap storage engine is composed of SlottedPage, HeapFile, and HeapTable components.

#### **Features**

#### Heap Table:
* Engine's top layer, represents the logical view of a Database table
* Supports create, create if not exists, drop, insert, update, delete, select, and project functionalities

#### Heap File:
* Engine's middle layer, which handles a collection of blocks
* Supports create, open, close, and delete database functionalities
* Supports get, get new, put, and block_ids block manipulation functionalities

#### Slotted Page:
* Engine's bottom layer, stores records in blocks in Slotted Page architecture
* Utilizes Berkeley DB's buffer manager for handling reading from and writing to the disk
* Supports add, get, delete, and put data manipulation in blocks functionalities

#### **Testing**

We used [heap_storage_test.cpp](https://github.com/klundeen/5300-Mink/blob/main/heap_storage_test.cpp) test file shared by amazing team Mink to validate our implementation.
All tests passed, including professor Lundeen's `test_heap_storage()` function.

### **Hand-off**

To assist the next team working on project Fossa, we recorded a walk-through video to show the parts of our code.
You can find watch the [video here](https://seattleu.instructuremedia.com/embed/62564f66-0f00-4f63-a7c1-cfc6f2666fde).

In addition to the video, the files contain comprehensive documentation.
