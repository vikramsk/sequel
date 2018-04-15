CC = g++ -std=c++1z -Iinclude -Isrc -O2 -w -pthread
#-Wno-deprecated 

debug: CC += -DDEBUG -g
debug: main

platform=$(shell uname)

ifeq ($(platform),Linux)
	tag = 'sed -n build/src/y.tab.c -e "s/  _attribute_ ((_unused))$$/# ifndef __cplusplus\n  __attribute_ ((_unused_));\n# endif/"'
else
	tag = 'sed -i "" "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" build/src/y.tab.c'
endif

ifeq ($(platform),Linux)
	tag1 = 'sed -n build/src/yyfunc.tab.c -e "s/  _attribute_ ((_unused))$$/# ifndef __cplusplus\n  __attribute_ ((_unused_));\n# endif/"'
else
	tag1 = 'sed -i "" "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" build/src/yyfunc.tab.c'
endif

BUILD_DIR ?= build
SRC_DIR ?= src
TEST_DIR ?= tests
SRC := $(shell find $(SRC_DIR) -name *.cc)
TEST_SRC := $(shell find $(TEST_DIR) -name *.cc)
OBJS := $(SRC:%=$(BUILD_DIR)/%.o)
TEST_OBJS := $(TEST_SRC:%=$(BUILD_DIR)/%.o)

MKDIR_P ?= mkdir -p

$(BUILD_DIR)/%.cc.o: %.cc
	$(MKDIR_P) $(dir $@)
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

all: main test.out test1.out test2.out test3.out testsuite

main: $(OBJS) build/src/yysql.tab.o build/src/lex.yysql.o build/src/main.o
	$(CC) $(CXXFLAGS) $(OBJS) build/src/main.o  build/src/yysql.tab.o build/src/lex.yysql.o -o build/$@

test.out: $(OBJS) build/src/y.tab.o build/src/yyfunc.tab.o build/src/lex.yy.o build/src/lex.yyfunc.o build/src/test.o dbfolder
	$(CC) $(CXXFLAGS) $(OBJS) build/src/test.o build/src/y.tab.o build/src/yyfunc.tab.o build/src/lex.yy.o build/src/lex.yyfunc.o -o build/test.out  -ll

test1.out: $(OBJS) build/src/y.tab.o build/src/yyfunc.tab.o build/src/lex.yy.o build/src/lex.yyfunc.o build/src/test1.o dbfolder
	$(CC) $(CXXFLAGS) $(OBJS) build/src/test1.o build/src/y.tab.o build/src/yyfunc.tab.o build/src/lex.yy.o build/src/lex.yyfunc.o -o build/test1.out  -ll

test2.out: $(OBJS) build/src/y.tab.o build/src/yyfunc.tab.o build/src/lex.yy.o build/src/lex.yyfunc.o build/src/test2.o dbfolder
	$(CC) $(CXXFLAGS) $(OBJS) build/src/test2.o build/src/y.tab.o build/src/yyfunc.tab.o build/src/lex.yy.o build/src/lex.yyfunc.o -o build/test2.out  -ll

test3.out: $(OBJS) build/src/y.tab.o build/src/yyfunc.tab.o build/src/lex.yy.o build/src/lex.yyfunc.o build/src/test3.o dbfolder
	$(CC) $(CXXFLAGS) $(OBJS) build/src/test3.o build/src/y.tab.o build/src/yyfunc.tab.o build/src/lex.yy.o build/src/lex.yyfunc.o -o build/test3.out  -ll

build/src/main.o: src/main.cpp
	$(CC) -g -c src/main.cpp -o $@
	
build/src/test.o: build/src/y.tab.o src/test.cpp
	$(CC) -Ibuild/src -g -c src/test.cpp -o $@
	
build/src/test1.o: src/test1.cpp
	$(CC) -g -c src/test1.cpp -o $@

build/src/test2.o: src/test2.cpp
	$(CC) -g -c src/test2.cpp -o $@

build/src/test3.o: src/test3.cpp
	$(CC) -g -c src/test3.cpp -o $@

build/src/yysql.tab.o: src/ParserSQL.y 
	yacc -p "yysql" -b "yysql" -d src/ParserSQL.y -o build/src/yysql.tab.c
	#sed -i build/src/yysql.tab.h -e "/int yyparse (void);/d" || true # Hack for continuing execution on MAC
	$(shell $(tag))
	g++ -w -c -Isrc/ build/src/yysql.tab.c 
	mv yysql.tab.o build/src/

build/src/y.tab.o: src/Parser.y 
	yacc -d src/Parser.y -o build/src/y.tab.c
	sed -i build/src/y.tab.h -e "/int yyparse (void);/d" || true # Hack for continuing execution on MAC
	$(shell $(tag))
	g++ -w -c -Isrc/ build/src/y.tab.c 
	mv y.tab.o build/src/

build/src/yyfunc.tab.o: src/ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d src/ParserFunc.y -o build/src/yyfunc.tab.c
	$(shell $(tag1))
	g++ -w -c -Isrc/ build/src/yyfunc.tab.c
	mv yyfunc.tab.o build/src/

build/src/lex.yyfunc.o: src/LexerFunc.l
	lex -Pyyfunc src/LexerFunc.l
	mv lex.yyfunc.c build/src/
	gcc -w -c -Isrc/ build/src/lex.yyfunc.c
	mv lex.yyfunc.o build/src/

build/src/lex.yysql.o: src/LexerSQL.l
	lex  -Pyysql src/LexerSQL.l 
	mv lex.yysql.c build/src/
	gcc -w -c -Isrc/ build/src/lex.yysql.c
	mv lex.yysql.o build/src/

build/src/lex.yy.o: src/Lexer.l
	lex  src/Lexer.l 
	mv lex.yy.c build/src/
	gcc -w -c -Isrc/ build/src/lex.yy.c
	mv lex.yy.o build/src/

dbfolder: 
	mkdir -p build/dbfiles

GTEST_DIR = include/googletest

# Where to find user code.
USER_DIR = tests

# Flags passed to the preprocessor.
# Set Google Test's header directory as a system directory, such that
# the compiler doesn't generate warnings in Google Test headers.
CPPFLAGS += -isystem $(GTEST_DIR)/include

# Flags passed to the C++ compiler.
CXXFLAGS += -g -pthread
#-Wall -Wextra -pthread

# All Google Test headers.  Usually you shouldn't change this
# definition.
GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h


# Builds gtest.a and gtest_main.a.

# Usually you shouldn't tweak such internal variables, indicated by a
# trailing _.
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)

# For simplicity and to avoid depending on Google Test's
# implementation details, the dependencies specified below are
# conservative and not optimized.  This is fine as Google Test
# compiles fast and for ordinary users its source rarely changes.
gtest-all.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest-all.cc

gtest_main.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest_main.cc

gtest.a : gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

gtest_main.a : gtest-all.o gtest_main.o
	$(AR) $(ARFLAGS) $@ $^

testsuite: $(OBJS) $(TEST_OBJS) build/src/y.tab.o build/src/yyfunc.tab.o build/src/lex.yy.o build/src/lex.yyfunc.o gtest.a dbfolder
	$(CC) $(CXXFLAGS) -Isrc $(OBJS) $(TEST_OBJS) build/src/y.tab.o build/src/lex.yy.o build/src/yyfunc.tab.o build/src/lex.yyfunc.o gtest.a -o $@
	mv $@ build

.PHONY: clean

clean:
	$(RM) -r $(BUILD_DIR)
	rm -f  gtest.a gtest_main.a *.o Statistics.txt
