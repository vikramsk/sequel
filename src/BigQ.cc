#include "BigQ.h"
#include "Defs.h"
#include <pthread.h>
#include <iostream>
#include "cpptoml.h"

void BigQ::mergeRunsAndWrite(Pipe *out, OrderMaker *sortOrder){}

void BigQ::createRuns(Pipe *in, OrderMaker *sortOrder, int runlen) {
    Record temp;
    int runLimit = runlen*PAGE_SIZE;
    //vector<Record> singleRun;
    //off_t currRunHead = 0;
    //Initialize runs file
    //TODO: Pull from config file
    //char* runsFile = config->get_as<string>("dbfiles")+"tpmms_runs.bin";
    
    //runs.Open(1, "build/dbfiles/tpmms_runs.bin");
    //cout << "Successfully opened file!" << endl;
    
    Schema mySchema("data/catalog", "lineitem");
    while(in->Remove(&temp)) {
        if(runLimit <= 0) {
            //sort the current records in singleRun

            //write it out to file
            //runHeads.push_back(currRunHead);
            //Page buffer;
            //off_t pageIndex = currRunHead; //append current run from given page index onwards
            //cout<< "In Append" <<endl;
            //for (vector<Record>::iterator it = singleRun.begin(); it != singleRun.end(); ++it) {
            //    int appendResult = buffer.Append(&*it); //dereferencing the pointer
            //    if (appendResult == 0) {  // indicates that the page is full
            //        runs.AddPage(&buffer,pageIndex++);  // write loaded buffer to file
            //        buffer.EmptyItOut();
            //        buffer.Append(&*it);
            //    }
            //}
            //runs.AddPage(&buffer,pageIndex++);  // write remaining records to file
            //currRunHead = pageIndex; // set start page of the next run
            //singleRun.clear();
            runLimit = runlen;
        }
        //cout<< "Outside while" <<endl;
        //singleRun.push_back(temp);
        temp.Print(&mySchema);
        runLimit--;
    }
    //runHeads.push_back(currRunHead);
    //Page buffer;
    //off_t pageIndex = currRunHead; //append current run from given page index onwards
    //cout<< "Out Append" <<endl;
    //for (vector<Record>::iterator it = singleRun.begin(); it != singleRun.end(); ++it) {
        //temp = *it;
        //int appendResult = buffer.Append(&temp); //dereferencing the pointer
        //if (appendResult == 0) {  // indicates that the page is full
        //    runs.AddPage(&buffer,pageIndex++);  // write loaded buffer to file
        //    buffer.EmptyItOut();
        //    buffer.Append(&temp);
        //}
    //}
    //runs.AddPage(&buffer,pageIndex);  // write remaining records to file
    //runs.Close();
}

void *BigQ::sortRecords(void *voidArgs) {
    BigQ *args = (BigQ *)voidArgs;
    args->createRuns(args->inPipe, args->sortOrdering, args->runlength);
    args->mergeRunsAndWrite(args->outPipe, args->sortOrdering);
    pthread_exit(NULL);
}

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    inPipe = &in;
    outPipe = &out;
    sortOrdering = &sortorder;
    runlength = runlen;

    //runs.Open(0, "build/dbfiles/tpmms_runs.bin"); //create file
    //runs.Close();

    /* Initialize and set thread detached attribute */
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&worker, NULL, &BigQ::sortRecords, this);
    pthread_attr_destroy(&attr);

    // read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data
    // into the out pipe

    // finally shut down the out pipe
    out.ShutDown();
}

BigQ::~BigQ() {}
