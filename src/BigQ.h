#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
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
    static void *sortRecords(void *voidArgs);
    void mergeRunsAndWrite(Pipe &out, OrderMaker &sortOrder);
    void createRuns(Pipe &in, OrderMaker &sortOrder, int runlen);
};

#endif
