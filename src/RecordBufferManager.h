#include <vector>
#include "File.h"
#include "Pipe.h"
#include "Record.h"

class RecordBufferManager {
   private:
    File file;
    enum { READ, WRITE } mode;
    char *filePath;
    Page buffer;
    bool singlePage;
    bool bufferFlushed;
    off_t pageIndex;
    vector<Record *> currentPage;
    size_t currentPageRecordIndex;

    void flushBuffer();
    bool bufferAppend(Record *rec, bool creatNewPage);
    void close();
    void loadRecordsInBuffer();

   public:
    RecordBufferManager();
    ~RecordBufferManager();
    bool AddRecord(Record *rec);
    bool AddRecordBlock(Pipe &pipe);
    void ClearBuffer();
    void MoveFirst();
    void MoveFirstInBlock();
    bool GetNext(Record &rec);
    bool GetNextInBlock(Record &rec);
};
