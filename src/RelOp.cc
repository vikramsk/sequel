#include "RelOp.h"

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
}

void SelectFile::WaitUntilDone () {
	// pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}

void *WriteOut::writeTextFile(void *voidArgs) {
	WriteOut *args = (WriteOut *)voidArgs;
	Record rec;
	while (args->in->Remove(&rec)) {
		
		// Get text version of rec
		std::string record = rec.GetTextVersion(args->schema);
		
		// Check size of rec string
		// if space remaining in buffer, append
		// else write buffer to file and then append
		if (record.size()+args->buffer.size() > args->buffer.capacity()) {
			fputs(args->buffer.c_str(),args->outputFile);
			args->buffer.clear();
		}
		args->buffer += record;
	}
	fputs(args->buffer.c_str(),args->outputFile);
	fclose(args->outputFile);
	pthread_exit(NULL);
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 
	// Set default capacity of buffer to 1 page
	if(buffer.capacity() < PAGE_SIZE)	Use_n_Pages(1);
	in = &inPipe;
	outputFile = outFile;
	schema = &mySchema;
	
	/* Initialize and set thread detached attribute */
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&thread, NULL, &WriteOut::writeTextFile, this);
    pthread_attr_destroy(&attr);
	
}
void WriteOut::WaitUntilDone () { 
	pthread_join(thread, NULL);
}
void WriteOut::Use_n_Pages (int n) { 
	buffer.reserve(n*PAGE_SIZE);
}
