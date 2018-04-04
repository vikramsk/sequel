#include "RecordBufferManager.h"
#include <stdlib.h>
#include <time.h>
#include <cstring>
#include <iostream>
#include <random>

RecordBufferManager::RecordBufferManager() {
    srand(time(NULL));
    pageIndex = 0;
    std::string fileName =
        "build/dbfiles/rec_manager_" + to_string(rand() % 10000) + ".bin";
    filePath = new char[fileName.length() + 1];
    strcpy(filePath, fileName.c_str());
    file.Open(0, filePath);
    mode = WRITE;
}

RecordBufferManager::~RecordBufferManager() {
    close();
    remove(filePath);
}

void RecordBufferManager::close() { file.Close(); }

bool RecordBufferManager::bufferAppend(Record *rec, bool createNewPage) {
    int appendResult = buffer.Append(rec);

    if (appendResult) return true;

    if (!createNewPage) return false;

    file.AddPage(&buffer,
                 pageIndex++);  // write loaded buffer to file
    buffer.EmptyItOut();
    buffer.Append(rec);
    return true;
}

void RecordBufferManager::flushBuffer() {
    file.AddPage(&buffer,
                 pageIndex);  // write remaining records in buffer to file
    buffer.EmptyItOut();
    pageIndex = 0;
}

bool RecordBufferManager::AddRecord(Record *rec) { bufferAppend(rec, true); }

void RecordBufferManager::ClearBuffer() { buffer.EmptyItOut(); }

bool RecordBufferManager::AddRecordBlock(Pipe &pipe) {
    mode = WRITE;
    Record rec;
    int pipeResult = pipe.Remove(&rec);

    // no record in pipe
    if (!pipeResult) return false;
    int ans = 0;
    while (pipeResult) {
        ans++;
        bool result = bufferAppend(&rec, false);
        if (!result) {
            loadRecordsInBuffer();
            Record *rCopy = new Record();
            rCopy->Copy(&rec);
            currentPage.push_back(rCopy);
            return true;
        }
        pipeResult = pipe.Remove(&rec);
    }

    // will reach here only if pipe gets empty before
    // page size is hit. In that case, load the records
    // in the buffer.
    loadRecordsInBuffer();
    return true;
}

void RecordBufferManager::loadRecordsInBuffer() {
    currentPage.clear();
    while (true) {
        Record *rec = new Record();
        int result = buffer.GetFirst(rec);
        if (!result) break;
        currentPage.push_back(rec);
    }
    currentPageRecordIndex = 0;
    mode = READ;
}

void RecordBufferManager::MoveFirstInBlock() { currentPageRecordIndex = 0; }

void RecordBufferManager::MoveFirst() {
    if (mode == READ) {
        currentPageRecordIndex = 0;
        return;
    }

    if (pageIndex == 0) {
        loadRecordsInBuffer();
        return;
    }

    flushBuffer();
    pageIndex = 0;
    file.GetPage(&buffer, pageIndex);
    loadRecordsInBuffer();
}

bool RecordBufferManager::GetNext(Record &rec) {
    if (currentPageRecordIndex == currentPage.size()) {
        if (file.GetLength() <= pageIndex + 2) return false;
        file.GetPage(&buffer, ++pageIndex);
        loadRecordsInBuffer();
    }
    rec.Copy(currentPage.at(currentPageRecordIndex));
    currentPageRecordIndex++;
    return true;
}

bool RecordBufferManager::GetNextInBlock(Record &rec) {
    if (currentPageRecordIndex == currentPage.size()) {
        return false;
    }
    rec.Copy(currentPage.at(currentPageRecordIndex));
    currentPageRecordIndex++;
    return true;
}
