#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"

int main(int argc, char** argv)
{
    if (argc != 2)
    {
        std::cout << " Usage: " << argv[0] << " [path to a writable directory]" << std::endl;
        return -1;
    }
    const char *HOME = argv[1];

    // TODO: add print message with location of database environment

    // TODO: prompt user for a SQL> query
    
    // TODO: after parsing, print the message parsed

    // TODO: Add support for create table

    // TODO: add support to select from

    // TODO: add support to join

    // TODO: add support to where clause

    // TODO: add support to alias

    // TODO: print message for invalid sql statements

    // TODO: use quit to exit

    std::cout << "Hello" << std::endl;
    return EXIT_SUCCESS;
}