#include <vector>
#include "File.h"
#include "Record.h"

class RecordBufferManager {
   private:
    File file;
    char *filePath;
    Page buffer;
    off_t pageIndex;
    vector<Record *> currentPage;
    size_t currentPageRecordIndex;
    vector<Record *>::iterator currPageIt;

    void flushBuffer();
    void bufferAppend(Record *rec);
    void close();
    void loadRecordsInBuffer();

   public:
    RecordBufferManager();
    ~RecordBufferManager();
    bool AddRecord(Record &rec);
    void MoveFirst();
    bool GetNext(Record &rec);
};
