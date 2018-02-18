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

   private:
    pthread_t worker;

    File runs;

    // runHeads records the page offsets for
    // each run in the file.
    vector<off_t> runHeads;

    static void *sortRecords(void *voidArgs);
    void mergeRunsAndWrite(Pipe &out, OrderMaker &sortOrder);
    void createRuns(Pipe &in, OrderMaker &sortOrder, int runlen);
};

#endif
