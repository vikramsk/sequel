#include "BigQ.h"
#include <pthread.h>
#include <iostream>
#include <vector>
#include "cpptoml.h"

void BigQ::mergeRunsAndWrite(Pipe *out, OrderMaker *sortOrder){}

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

            //write it out to file
            runHeads.push_back(currRunHead);
            Page writeBuffer;
            Record *writeTemp;
            cout<< singleRun.size() <<endl;
            for (vector<Record *>::iterator it = singleRun.begin(); it != singleRun.end(); ++it) {
                writeTemp = *it;
                //writeTemp->Print(&mySchema);
                appendResult = writeBuffer.Append(writeTemp);
                if (appendResult == 0) {  // indicates that the page is full
                    runs.AddPage(&writeBuffer,currRunHead++);  // write loaded buffer to file
                    writeBuffer.EmptyItOut();
                    writeBuffer.Append(writeTemp);
                }
            }
            runs.AddPage(&writeBuffer,currRunHead++);  // write remaining records to file
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

    runHeads.push_back(currRunHead);

    for (vector<Record *>::iterator it = singleRun.begin(); it != singleRun.end(); ++it) {
        temp = *it;
        //temp->Print(&mySchema);
        appendResult = buffer.Append(temp);
        if (appendResult == 0) {  // indicates that the page is full
            runs.AddPage(&buffer,currRunHead++);  // write loaded buffer to file
            buffer.EmptyItOut();
            buffer.Append(temp);
        }
    }
    runs.AddPage(&buffer,currRunHead);  // write remaining records to file
    runs.Close();
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
