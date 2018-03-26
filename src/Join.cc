#include <cstring>
#include <iostream>
#include "RecordBufferManager.h"
#include "RelOp.h"

void Join::WaitUntilDone() { pthread_join(thread, NULL); }

void mergeEqualRecords(Record &recLeft, Record &recRight, Pipe &sortedL,
                       Pipe &sortedR, Pipe &outPipe, OrderMaker &orderL,
                       OrderMaker &orderR) {
    Record result;
    ComparisonEngine comp;
    int nl = recLeft.NumAtts();
    int nr = recRight.NumAtts();
    int attsToKeep[nl + nr];
    for (int i = 0; i < nl; i++) {
        attsToKeep[i] = i;
    }
    for (int i = 0; i < nr; i++) {
        attsToKeep[i + nl] = i;
    }

    // std::string filePath =
    //    "build/dbfiles/rec_manager_" + to_string(rand() % 10000) + ".bin";
    // char *fileName = new char[filePath.length() + 1];
    // strcpy(fileName, filePath.c_str());

    // DBFile file;
    // file.Create(fileName, heap, NULL);
    RecordBufferManager buff;

    // write all matches to temp file.
    while (comp.Compare(&recLeft, &orderL, &recRight, &orderR) == 0) {
        buff.AddRecord(recRight);
        if (!sortedR.Remove(&recRight)) {
            recRight.bits = NULL;
            break;
        }
    }
    while (recLeft.bits) {
        buff.MoveFirst();
        buff.GetNext(recRight);
        int compResult = comp.Compare(&recLeft, &orderL, &recRight, &orderR);
        if (compResult != 0) {
            break;
        }
        do {
            result.MergeRecords(&recLeft, &recRight, nl, nr, attsToKeep,
                                nl + nr, nl);
            outPipe.Insert(&result);
        } while (buff.GetNext(recRight));

        if (!sortedL.Remove(&recLeft)) {
            recLeft.bits = NULL;
        };
    }
    if (!sortedR.Remove(&recRight)) {
        recRight.bits = NULL;
    };
    // file.Delete();
}

void mergeSortJoin(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe,
                   OrderMaker &orderL, OrderMaker &orderR) {
    Pipe sortedL(100), sortedR(100);
    BigQ qLeft(inPipeL, sortedL, orderL, 100);
    BigQ qRight(inPipeR, sortedR, orderR, 100);

    ComparisonEngine comp;
    Record recLeft;
    Record recRight;

    sortedL.Remove(&recLeft);
    sortedR.Remove(&recRight);

    while (recLeft.bits && recRight.bits) {
        int compResult = comp.Compare(&recLeft, &orderL, &recRight, &orderR);
        if (compResult == 0) {
            mergeEqualRecords(recLeft, recRight, sortedL, sortedR, outPipe,
                              orderL, orderR);
        } else if (compResult < 0) {
            if (!sortedL.Remove(&recLeft)) {
                recLeft.bits = NULL;
            };
        } else {
            if (!sortedR.Remove(&recRight)) {
                recRight.bits = NULL;
            }
        }
    }
    while (recLeft.bits) {
        if (!sortedL.Remove(&recLeft)) {
            recLeft.bits = NULL;
        };
    }
    while (recRight.bits) {
        if (!sortedR.Remove(&recRight)) {
            recRight.bits = NULL;
        }
    }

    outPipe.ShutDown();
}

void blockNestedLoopJoin(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe,
                         CNF &selOp, Record &literal) {}

void performJoin(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp,
                 Record &literal) {
    OrderMaker orderLeft, orderRight;
    if (selOp.GetSortOrders(orderLeft, orderRight)) {
        mergeSortJoin(inPipeL, inPipeR, outPipe, orderLeft, orderRight);
    } else {
        blockNestedLoopJoin(inPipeL, inPipeR, outPipe, selOp, literal);
    }
    outPipe.ShutDown();
}

void *Join::joinWorker(void *voidArgs) {
    joinArgs *args = (joinArgs *)voidArgs;
    performJoin(args->inL, args->inR, args->out, args->cnf, args->literal);
    pthread_exit(NULL);
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp,
               Record &literal) {
    joinArgs *jArgs = new joinArgs{inPipeL, inPipeR, outPipe, selOp, literal};

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&thread, NULL, &Join::joinWorker, (void *)jArgs);
    pthread_attr_destroy(&attr);
}
