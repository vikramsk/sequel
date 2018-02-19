#include "BigQ.h"
#include <pthread.h>
#include "cpptoml.h"

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
          runlen(runlen) {}
};

void BigQ::mergeRunsAndWrite(Pipe &out, OrderMaker &sortOrder){};

off_t BigQ::appendRunToFile(off_t currRunHead, vector<Record> &singleRun) {
    Page buffer;
    off_t pageIndex = currRunHead; //append current run from given page index onwards
    
    for (vector<Record>::iterator it = singleRun.begin(); it != singleRun.end(); ++it) {
        int appendResult = buffer.Append(&*it); //dereferencing the pointer
        if (appendResult == 0) {  // indicates that the page is full
            runs.AddPage(&buffer,pageIndex++);  // write loaded buffer to file
            buffer.EmptyItOut();
            buffer.Append(&*it);
        }
    }
    runs.AddPage(&buffer,pageIndex++);  // write remaining records to file
    return pageIndex; // return start page of the next run
}

void BigQ::createRuns(Pipe &in, OrderMaker &sortOrder, int runlen){
    Record temp;
    vector<Record> singleRun(runlen);
    int runLimit = runlen;
    off_t currRunHead = 0;
    
    //Initialize runs file
    //TODO: Pull from config file
    //char* runsFile = config->get_as<string>("dbfiles")+"tpmms_runs.bin";
    runs.Open(0, "build/dbfiles/tpmms_runs.bin");
    
    while(in.Remove(&temp)) {
        if(runLimit <= 0) {
            
            //sort the current records in singleRun
            
            //write it out to file
            runHeads.push_back(currRunHead);
            currRunHead = appendRunToFile(currRunHead, singleRun);
            singleRun.clear();
            runLimit = runlen;
        }
        singleRun.push_back(temp);
        runLimit--;
    }
    runHeads.push_back(currRunHead);
    appendRunToFile(currRunHead, singleRun);
    runs.Close();
}

void *BigQ::sortRecords(void *voidArgs) {
    workerArgs *args = (workerArgs *)voidArgs;
    args->instance->createRuns(args->in, args->sortOrder, args->runlen);
    args->instance->mergeRunsAndWrite(args->out, args->sortOrder);
    pthread_exit(NULL);
}

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    workerArgs args(this, in, out, sortorder, runlen);
    pthread_create(&worker, NULL, BigQ::sortRecords, &args);
    // read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data
    // into the out pipe

    // finally shut down the out pipe
    out.ShutDown();
}

BigQ::~BigQ() {}
