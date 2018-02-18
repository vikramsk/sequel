#include "BigQ.h"
#include <pthread.h>

struct workerArgs {
    BigQ *instance;
    Pipe &in;
    Pipe &out;
    OrderMaker &sortOrder;
    int runlen;

    workerArgs(BigQ *instance, Pipe &in, Pipe &out, OrderMaker &sortOrder,
               int runlen)
        : instance(instance),
          in(in),
          out(out),
          sortOrder(sortOrder),
          runlen(runlen) {
        // in = in;
        // out = out;
        // sortOrder = sortOrder;
        // runlen = runlen;
    }
};

void BigQ::mergeRunsAndWrite(Pipe &out, OrderMaker &sortOrder){};

void BigQ::createRuns(Pipe &in, OrderMaker &sortOrder, int runlen){};

// void BigQ::*sortRecords(void *voidArgs) {
//    workerArgs *args = (workerArgs *)voidArgs;
//    args->instance->createRuns(args->in, args->sortOrder, args->runlen);
//    args->instance->mergeRunsAndWrite(args->out, args->sortOrder);
//}

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    // workerArgs args(this, in, out, sortorder, runlen);
    // pthread_create(&worker, NULL, &BigQ::sortRecords, &args);
    // read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data
    // into the out pipe

    // finally shut down the out pipe
    out.ShutDown();
}

BigQ::~BigQ() {}
