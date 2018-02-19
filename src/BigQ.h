#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include "File.h"
#include "Pipe.h"
#include "Record.h"

using namespace std;

class BigQ {
   public:
    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    ~BigQ();
    pthread_t worker;

   private:
    File runs;

    // runHeads records the page offsets for
    // each run in the file.
    vector<off_t> runHeads;

    static void *sortRecords(void *voidArgs);
    void mergeRunsAndWrite(Pipe &out, OrderMaker &sortOrder);
    off_t appendRunToFile(off_t currRunHead, vector<Record> &singleRun);
    void createRuns(Pipe &in, OrderMaker &sortOrder, int runlen);
};

#endif
