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

    RecordBufferManager buff;

    // write all matches to temp file.
    while (comp.Compare(&recLeft, &orderL, &recRight, &orderR) == 0) {
        buff.AddRecord(recRight);
        if (!sortedR.Remove(&recRight)) {
            recRight.bits = NULL;
            break;
        }
    }
    Record rightIterator;
    while (recLeft.bits) {
        buff.MoveFirst();
        buff.GetNext(rightIterator);
        int compResult =
            comp.Compare(&recLeft, &orderL, &rightIterator, &orderR);
        if (compResult != 0) {
            break;
        }
        do {
            result.MergeRecords(&recLeft, &rightIterator, nl, nr, attsToKeep,
                                nl + nr, nl);
            outPipe.Insert(&result);
        } while (buff.GetNext(rightIterator));

        if (!sortedL.Remove(&recLeft)) {
            recLeft.bits = NULL;
        };
    }
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
