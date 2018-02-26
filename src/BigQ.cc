#include "BigQ.h"
#include <pthread.h>
#include <algorithm>
#include <iostream>
#include <queue>
#include <vector>
#include "cpptoml.h"

struct SortHelper {
    ComparisonEngine comp;
    OrderMaker *ordering;
    SortHelper(OrderMaker *ordering) { this->ordering = ordering; }
    bool operator()(Record *a, Record *b) {
        return comp.Compare(a, b, ordering) < 0;  // Arranges in ascending order
    }
};

struct recordTracker {
    size_t runTrackerIndex;
    Record *record;
    recordTracker(size_t pi, Record *r) : runTrackerIndex(pi), record(r) {}
};

struct runTracker {
    off_t pageIndex;
    Page *page;
    runTracker(off_t pi, Page *p) : pageIndex(pi), page(p) {}
};

struct MergeSortHelper {
    ComparisonEngine comp;
    OrderMaker *ordering;
    bool operator()(recordTracker *a, recordTracker *b) {
        return comp.Compare(a->record, b->record, ordering) < 0;
    }

   public:
    MergeSortHelper(OrderMaker *ordering) : ordering(ordering) {}
};

// creates a vector of pages consisting of the first page of each run.
vector<runTracker *> createRunTrackers(File &runs, vector<off_t> &runHeads,
                                       vector<bool> &runStatus) {
    vector<runTracker *> runTrackers;
    Page *runPage = new Page();
    for (vector<off_t>::iterator it = runHeads.begin(); it != runHeads.end();
         ++it) {
        runs.GetPage(runPage, *it);
        runTrackers.push_back(new runTracker(*it, runPage));
        runStatus.push_back(false);
        runPage = new Page();
    }
    free(runPage);
    return runTrackers;
}

std::priority_queue<recordTracker *, std::vector<recordTracker *>,
                    MergeSortHelper>
initPriorityQueue(vector<runTracker *> &runTrackers, OrderMaker *sortOrder) {
    MergeSortHelper cmp(sortOrder);
    std::priority_queue<recordTracker *, std::vector<recordTracker *>,
                        MergeSortHelper>
        pq(cmp);
    // Assume each page has at least one record.
    // This should be true because it's the first page of each run.
    Record *rec = new Record();
    for (std::size_t i = 0; i != runTrackers.size(); i++) {
        runTrackers[i]->page->GetFirst(rec);
        pq.push(new recordTracker(i, rec));
        rec = new Record();
    }
    free(rec);
    return pq;
}

runTracker *createRunTracker(File &runs, off_t pageIndex) {
    Page *p = new Page();
    runs.GetPage(p, pageIndex);
    return new runTracker(pageIndex, p);
}

// getNextRecord fetches the next record using the parameters
// and stores it in rec.
// it returns the runTracker index of the returned record.
// if no record is stored, it returns -1.
bool getNextRecordInRun(Record *rec, vector<runTracker *> &rts, File &runs,
                        vector<off_t> &runHeads, size_t runTrackerIndex) {
    int status = rts[runTrackerIndex]->page->GetFirst(rec);
    if (status) {
        return true;
    }

    // if the current page is from the last run.
    if (runTrackerIndex == runHeads.size() - 1) {
        if (++rts[runTrackerIndex]->pageIndex < runs.GetLength() - 1) {
            rts[runTrackerIndex] =
                createRunTracker(runs, rts[runTrackerIndex]->pageIndex);
            rts[runTrackerIndex]->page->GetFirst(rec);
            return true;
        } else {
            // last run is processed.
            return false;
        }
    }

    // if the selected run isn't the last and pageIndex + 1 for the
    // current run isn't the start index of the next run.
    if (++rts[runTrackerIndex]->pageIndex < runHeads[runTrackerIndex + 1]) {
        rts[runTrackerIndex] =
            createRunTracker(runs, rts[runTrackerIndex]->pageIndex);
        rts[runTrackerIndex]->page->GetFirst(rec);
        return true;
    }
    return false;
}

// getNextRecord loads the next record for the selected run.
// if no more records are available in the current run, it fetches
// a record from another run.
// it returns the runIndex for the record and returns -1 if no more
// records are available for processing in any run.
off_t getNextRecord(Record *rec, vector<runTracker *> &rts, File &runs,
                    vector<off_t> &runHeads, vector<bool> &runStatus,
                    size_t runTrackerIndex) {
    bool status;
    if (!runStatus[runTrackerIndex]) {
        status = getNextRecordInRun(rec, rts, runs, runHeads, runTrackerIndex);
        if (status) {
            return runTrackerIndex;
        } else {
            runStatus[runTrackerIndex] = true;
        }
    }

    for (size_t i = 0; i != runStatus.size(); i++) {
        if (runStatus[i]) continue;
        status = getNextRecordInRun(rec, rts, runs, runHeads, i);

        if (status) return i;
        runStatus[i] = true;
    }
    return -1;
}

void BigQ::mergeRunsAndWrite(Pipe *out, OrderMaker *sortOrder) {
    runs.Open(1, "build/dbfiles/tpmms_runs.bin");

    vector<bool> runStatus;
    auto runTrackers = createRunTrackers(runs, runHeads, runStatus);

    auto recordPQ = initPriorityQueue(runTrackers, sortOrder);
    bool runsEmpty = false;
    while (!recordPQ.empty()) {
        auto rec = recordPQ.top();
        recordPQ.pop();

        Record *r = new Record();
        recordTracker *rt = new recordTracker(0, r);
        if (!runsEmpty) {
            auto runTrackerIndex =
                getNextRecord(r, runTrackers, runs, runHeads, runStatus,
                              rec->runTrackerIndex);
            if (runTrackerIndex == -1) {
                runsEmpty = true;
            } else {
                recordPQ.push(new recordTracker(runTrackerIndex, r));
            }
        }
        out->Insert(rec->record);
    }
    runs.Close();
    out->ShutDown();
}

off_t appendRunToFile(vector<Record *> &singleRun, File &runs,
                      off_t pageIndex) {
    Page buffer;
    Record *temp;
    int appendResult = 0;
    for (vector<Record *>::iterator it = singleRun.begin();
         it != singleRun.end(); ++it) {
        temp = *it;
        // temp->Print(&mySchema);
        appendResult = buffer.Append(temp);
        if (appendResult == 0) {  // indicates that the page is full
            runs.AddPage(&buffer, pageIndex++);  // write loaded buffer to file
            buffer.EmptyItOut();
            buffer.Append(temp);
        }
    }
    runs.AddPage(&buffer, pageIndex++);  // write remaining records to file
    return pageIndex;
}

void BigQ::createRuns(Pipe *in, OrderMaker *sortOrder, int runlen) {
    int pagesLeft = runlen;
    vector<Record *> singleRun;
    off_t currRunHead = 0;

    // Initialize runs file
    // TODO: Pull from config file
    // char* runsFile = config->get_as<string>("dbfiles")+"tpmms_runs.bin";
    runs.Open(0, "build/dbfiles/tpmms_runs.bin");

    Record rec;
    Page buffer;
    int appendResult = 0;
    Record *temp = new Record();
    Schema mySchema("data/catalog", "lineitem");
    while (in->Remove(&rec)) {
        if (pagesLeft <= 0) {
            // sort the current records in singleRun
            sort(singleRun.begin(), singleRun.end(), SortHelper(sortOrder));
            // update vector to track head of new run
            runHeads.push_back(currRunHead);
            // write it out to file
            currRunHead = appendRunToFile(singleRun, runs, currRunHead);
            singleRun.clear();
            pagesLeft = runlen;
        }
        appendResult = buffer.Append(&rec);
        if (appendResult == 0) {  // indicates that the page is full
            // move loaded buffer to vector
            while (buffer.GetFirst(temp) != 0) {
                singleRun.push_back(temp);
                temp = new Record();
            }
            pagesLeft--;
            buffer.Append(&rec);
        }
    }

    while (buffer.GetFirst(temp) != 0) {
        singleRun.push_back(temp);
        temp = new Record();
    }
    free(temp);

    // sort the current records in singleRun
    sort(singleRun.begin(), singleRun.end(), SortHelper(sortOrder));
    // update vector to track head of new run
    runHeads.push_back(currRunHead);
    // write it out to file
    appendRunToFile(singleRun, runs, currRunHead);
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

BigQ::~BigQ() { remove("build/dbfiles/tpmms_runs.bin"); }
