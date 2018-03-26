#include "RecordBufferManager.h"
#include <cstring>
#include <iostream>

RecordBufferManager::RecordBufferManager() {
    pageIndex = 0;
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

void RecordBufferManager::bufferAppend(Record *rec) {
    int appendResult = buffer.Append(rec);
    if (appendResult == 0) {  // indicates that the page is full
        file.AddPage(&buffer,
                     pageIndex++);  // write loaded buffer to file
        buffer.EmptyItOut();
        buffer.Append(rec);
    }
}

void RecordBufferManager::flushBuffer() {
    file.AddPage(&buffer,
                 pageIndex);  // write remaining records in buffer to file
    buffer.EmptyItOut();
    pageIndex = 0;
}

bool RecordBufferManager::AddRecord(Record &rec) { bufferAppend(&rec); }

void RecordBufferManager::loadRecordsInBuffer() {
    currentPage.clear();
    Record *rec = new Record();
    while (buffer.GetFirst(rec)) {
        currentPage.push_back(rec);
    }
    currentPageRecordIndex = 0;
    // currPageIt = currentPage.begin();
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
    // cout << currPageIt - currentPage.begin() << endl;
    rec.Copy(currentPage.at(currentPageRecordIndex));
    currentPageRecordIndex++;
    return true;
}
