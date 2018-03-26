#include "RecordBufferManager.h"
#include <cstring>
#include <iostream>

RecordBufferManager::RecordBufferManager(bool sp) {
    pageIndex = 0;
    singlePage = sp;
    std::string fileName =
        "build/dbfiles/rec_manager_" + to_string(rand() % 10000) + ".bin";
    filePath = new char[fileName.length() + 1];
    strcpy(filePath, fileName.c_str());
}

RecordBufferManager::~RecordBufferManager() {
    close();
    remove(filePath);
}

void RecordBufferManager::close() { file.Close(); }

bool RecordBufferManager::bufferAppend(Record *rec) {
    int appendResult = buffer.Append(rec);

    if (appendResult == 0 && !singlePage) {  // indicates that the page is full
        file.AddPage(&buffer,
                     pageIndex++);  // write loaded buffer to file
        buffer.EmptyItOut();
        buffer.Append(rec);
    }
    return appendResult;
}

void RecordBufferManager::flushBuffer() {
    file.AddPage(&buffer,
                 pageIndex);  // write remaining records in buffer to file
    buffer.EmptyItOut();
    pageIndex = 0;
}

bool RecordBufferManager::AddRecord(Record &rec) { bufferAppend(&rec); }

bool RecordBufferManager::AddRecordBlock(Pipe &pipe) {
    bool result = true;
    Record *rec = new Record();
    int pipeResult = pipe.Remove(rec);
    while (pipeResult) {
        result = bufferAppend(rec);
        if (!result) {
            // insert the extra record in the block
            loadRecordsInBuffer();
            currentPage.push_back(rec);
            return true;
        }
        pipeResult = pipe.Remove(rec);
    }
    return pipeResult;
}

void RecordBufferManager::loadRecordsInBuffer() {
    currentPage.clear();
    Record *rec = new Record();
    while (buffer.GetFirst(rec)) {
        currentPage.push_back(rec);
    }
    currentPageRecordIndex = 0;
}

void RecordBufferManager::MoveFirst() {
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
