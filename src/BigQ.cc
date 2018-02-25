#include "BigQ.h"
#include <pthread.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include "cpptoml.h"

struct SortHelper {
    ComparisonEngine comp;
    OrderMaker *ordering;
    SortHelper(OrderMaker *ordering) { 
        this->ordering = ordering;
    }
    bool operator () (Record *a, Record *b) {
        return comp.Compare(a, b,ordering)<0; //Arranges in ascending order
    }
};

void BigQ::mergeRunsAndWrite(Pipe *out, OrderMaker *sortOrder){}

off_t appendRunToFile(vector<Record *> &singleRun, File &runs, off_t pageIndex) {
    Page buffer;
    Record *temp;
    int appendResult = 0;
    for (vector<Record *>::iterator it = singleRun.begin(); it != singleRun.end(); ++it) {
        temp = *it;
        //temp->Print(&mySchema);
        appendResult = buffer.Append(temp);
        if (appendResult == 0) {  // indicates that the page is full
            runs.AddPage(&buffer,pageIndex++);  // write loaded buffer to file
            buffer.EmptyItOut();
            buffer.Append(temp);
        }
    }
    runs.AddPage(&buffer,pageIndex++);  // write remaining records to file
    return pageIndex;
}

void BigQ::createRuns(Pipe *in, OrderMaker *sortOrder, int runlen) {
    int pagesLeft = runlen;
    vector<Record *> singleRun;
    off_t currRunHead = 0;

    //Initialize runs file
    //TODO: Pull from config file
    //char* runsFile = config->get_as<string>("dbfiles")+"tpmms_runs.bin";
    runs.Open(0, "build/dbfiles/tpmms_runs.bin");
    
    Record rec;
    Page buffer;
    int appendResult = 0;
    Record *temp = new Record();
    Schema mySchema("data/catalog", "lineitem");
    while(in->Remove(&rec)) {
        if(pagesLeft <= 0) {
            //sort the current records in singleRun
            sort(singleRun.begin(), singleRun.end(), SortHelper(sortOrder));
            //update vector to track head of new run
            runHeads.push_back(currRunHead);
            //write it out to file
            currRunHead = appendRunToFile(singleRun,runs,currRunHead);
            singleRun.clear();
            pagesLeft = runlen;
        }
        appendResult = buffer.Append(&rec);
        if (appendResult == 0) {  // indicates that the page is full
            // move loaded buffer to vector
            while(buffer.GetFirst(temp)!=0) {
                singleRun.push_back(temp);
                temp = new Record();
            }
            pagesLeft--;
            buffer.Append(&rec);
        }
    }
    
    while(buffer.GetFirst(temp)!=0) {
        singleRun.push_back(temp);
        temp = new Record();
    }
    free(temp);

    //sort the current records in singleRun
    sort(singleRun.begin(), singleRun.end(), SortHelper(sortOrder));
    //update vector to track head of new run
    runHeads.push_back(currRunHead);
    //write it out to file
    appendRunToFile(singleRun,runs,currRunHead);
    runs.Close();
}

void *BigQ::sortRecords(void *voidArgs) {
    BigQ *args = (BigQ *)voidArgs;
    // read data from in pipe sort them into runlen pages
    args->createRuns(args->inPipe, args->sortOrdering, args->runlength);
    // construct priority queue over sorted runs and dump sorted data
    // into the out pipe
    args->mergeRunsAndWrite(args->outPipe, args->sortOrdering);
    // finally shut down the out pipe
    args->outPipe->ShutDown();
    pthread_exit(NULL);
}

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    inPipe = &in;
    outPipe = &out;
    sortOrdering = &sortorder;
    runlength = runlen;

    /* Initialize and set thread detached attribute */
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&worker, NULL, &BigQ::sortRecords, this);
    pthread_attr_destroy(&attr);
}

BigQ::~BigQ() {}
