
CCFLAGS = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb # added -ggdb
COURSE = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR = $(COURSE)/lib

OBJ = sql5300.o

%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"

sql5300: $(OBJ)
	g++ -L$(LIB_DIR) -o $@ $< -ldb_cxx -lsqlparser

clean:
	rm -f sql5300 *.o


# sql5300: sql5300.o
# 	g++ -L/usr/local/db6/lib -o $@ $< -ldb_cxx -lsqlparser

# sql5300.o : sql5300.cpp
# 	g++ -I/usr/local/db6/include -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11 -c -ggdb -o $@ $<

# clean :
# 	rm -f sql5300.o sql5300