#include <iostream>
#include "RelOp.h"

void *GroupBy::performGrouping(void *voidArgs) {
    GroupBy *args = (GroupBy *)voidArgs;
    Pipe groupedOutput(100);
    BigQ bigQInstance(*(args->in), groupedOutput, *(args->groupingAttributes),
                      1);

    Record currentRec;
    Record previousRec;
    if (groupedOutput.Remove(&currentRec)) {
        previousRec.Consume(&currentRec);
    }

    // if there are no records in the input pipe, return
    if (!previousRec.bits) {
        args->out->ShutDown();
        pthread_exit(NULL);
    }

    int grpAttrCount = args->groupingAttributes->getNumAttributes();
    int attsToKeep[1 + grpAttrCount];
    attsToKeep[0] = 0;
    int *groupAttrList = args->groupingAttributes->getAttributes();
    std::copy(groupAttrList, groupAttrList + grpAttrCount, attsToKeep + 1);
    ComparisonEngine comp;
    Pipe *currentGroup = new Pipe(100);
    Pipe *groupSum = new Pipe(1);
    Sum S;
    Record summedResult;
    S.Run(*currentGroup, *groupSum, *(args->func));

    while (groupedOutput.Remove(&currentRec)) {
        if (comp.Compare(&currentRec, &previousRec, args->groupingAttributes) ==
            0) {
            currentGroup->Insert(&previousRec);
        } else {
            Record prevRecCopy;
            prevRecCopy.Copy(&previousRec);

            currentGroup->Insert(&previousRec);
            currentGroup->ShutDown();
            S.WaitUntilDone();
            Record sumRec;
            groupSum->Remove(&sumRec);

            // Merge the two records
            summedResult.MergeRecords(&sumRec, &prevRecCopy, 1, grpAttrCount,
                                      attsToKeep, 1 + grpAttrCount, 1);
            args->out->Insert(&summedResult);
            free(currentGroup);
            free(groupSum);
            currentGroup = new Pipe(100);
            groupSum = new Pipe(1);
            S.Run(*currentGroup, *groupSum, *(args->func));
        }
        previousRec.Consume(&currentRec);
    }

    Record prevRecCopy;
    prevRecCopy.Copy(&previousRec);

    currentGroup->Insert(&previousRec);
    currentGroup->ShutDown();
    S.WaitUntilDone();
    Record sumRec;
    groupSum->Remove(&sumRec);

    // Merge the two records
    summedResult.MergeRecords(&sumRec, &prevRecCopy, 1, grpAttrCount,
                              attsToKeep, 1 + grpAttrCount, 1);
    args->out->Insert(&summedResult);
    free(currentGroup);
    free(groupSum);
    args->out->ShutDown();
    pthread_exit(NULL);
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
                  Function &computeMe) {
    in = &inPipe;
    out = &outPipe;
    groupingAttributes = &groupAtts;
    func = &computeMe;

    /* Initialize and set thread detached attribute */
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&thread, NULL, &GroupBy::performGrouping, this);
    pthread_attr_destroy(&attr);
}

void GroupBy::WaitUntilDone() { pthread_join(thread, NULL); }