#ifndef REL_OP_H
#define REL_OP_H

#include <string>
#include <thread>
#include "DBFile.h"
#include "Function.h"
#include "Pipe.h"
#include "Record.h"

class RelationalOp {
   protected:
    pthread_t thread;

   public:
    // blocks the caller until the particular relational operator
    // has run to completion
    virtual void WaitUntilDone() = 0;

    // tell us how much internal memory the operation can use
    virtual void Use_n_Pages(int n) = 0;
};

class SelectFile : public RelationalOp {
   private:
    struct selectArgs {
        DBFile &inFile;
        Pipe &out;
        CNF &cnf;
        Record &literal;
    };

    static void *selectFileWorker(void *args);

   public:
    void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone();
    void Use_n_Pages(int n) {}
};

class SelectPipe : public RelationalOp {
   private:
    struct selectArgs {
        Pipe &in;
        Pipe &out;
        CNF &cnf;
        Record &literal;
    };

    static void *selectPipeWorker(void *args);

   public:
    void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone();
    void Use_n_Pages(int n) {}
};

class Project : public RelationalOp {
   private:
    struct projectArgs {
        Pipe &in;
        Pipe &out;
        int *keepMe;
        int numAttsInput;
        int numAttsOutput;
    };

    static void *projectWorker(void *args);

   public:
    void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
             int numAttsOutput);
    void WaitUntilDone();
    void Use_n_Pages(int n) {}
};

class Join : public RelationalOp {
   private:
    struct joinArgs {
        Pipe &inL;
        Pipe &inR;
        Pipe &out;
        CNF &cnf;
        Record &literal;
    };

    static void *joinWorker(void *args);

   public:
    void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp,
             Record &literal);
    void WaitUntilDone();
    void Use_n_Pages(int n) {}
};

class DuplicateRemoval : public RelationalOp {
   private:
    Pipe *in;
    Pipe *out;
    Schema *schema;

    static void *bigQDuplicateRemoval(void *voidArgs);

   public:
    void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
    void WaitUntilDone();
    void Use_n_Pages(int n) {}
};

class Sum : public RelationalOp {
   private:
    Pipe *in;
    Pipe *out;
    Function *func;

    static void *computeSum(void *voidArgs);

   public:
    void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
    void WaitUntilDone();
    // TODO: Check how to use a buffer for computing sum
    void Use_n_Pages(int n) {}
};

class GroupBy : public RelationalOp {
   private:
    Pipe *in;
    Pipe *out;
    OrderMaker *groupingAttributes;
    Function *func;

    static void *performGrouping(void *voidArgs);

   public:
    void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
             Function &computeMe);
    void WaitUntilDone();
    void Use_n_Pages(int n) {}
};

class WriteOut : public RelationalOp {
   private:
    std::string buffer;
    Pipe *in;
    FILE *outputFile;
    Schema *schema;

    static void *writeTextFile(void *voidArgs);

   public:
    void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};
#endif
