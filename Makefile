CC = g++ -std=c++1z -Iinclude/ -O2 -Wno-deprecated 

debug: CC += -DDEBUG -g
debug: main

tag = -i

ifdef linux
tag = -n
endif

main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -ll

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o y.tab.o lex.yy.o test.o -ll
	
test.o: src/test.cc
	$(CC) -g -c src/test.cc

main.o: src/main.cc
	$(CC) -g -c src/main.cc
	
Comparison.o: src/Comparison.cc
	$(CC) -g -c src/Comparison.cc
	
ComparisonEngine.o: src/ComparisonEngine.cc
	$(CC) -g -c src/ComparisonEngine.cc
	
DBFile.o: src/DBFile.cc
	$(CC) -g -c src/DBFile.cc

File.o: src/File.cc
	$(CC) -g -c src/File.cc

Record.o: src/Record.cc
	$(CC) -g -c src/Record.cc

Schema.o: src/Schema.cc
	$(CC) -g -c src/Schema.cc
	
y.tab.o: src/Parser.y src/ParseTree.h
	yacc -d src/Parser.y 
	sed $(tag) "" "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c 
	g++ -c -Isrc/ y.tab.c 

lex.yy.o: src/Lexer.l
	lex  src/Lexer.l
	gcc  -c -Isrc/ lex.yy.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
