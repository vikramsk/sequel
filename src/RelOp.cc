#include "RelOp.h"
#include <future>
#include <thread>

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    worker = std::async([&, this] {
        ComparisonEngine comp;
        Record rec;
        while (inPipe.Remove(&rec)) {
            if (comp.Compare(&rec, &literal, &selOp)) {
                outPipe.Insert(&rec);
            }
        }
        outPipe.ShutDown();
    });
}

void SelectPipe::WaitUntilDone() { worker.wait(); }

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp,
                     Record &literal) {
    worker = std::async(std::launch::async, [&, this] {
        ComparisonEngine comp;
        Record rec;
		inFile.MoveFirst();
        while (inFile.GetNext(rec,selOp,literal)) {
                outPipe.Insert(&rec);
        }
        outPipe.ShutDown();
    });
}

void SelectFile::WaitUntilDone() { worker.wait(); }

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
                  int numAttsOutput) {
    worker = std::async(std::launch::async, [&, this] {
        Record rec;
        while (inPipe.Remove(&rec)) {
            rec.Project(keepMe, numAttsOutput, numAttsInput);
            outPipe.Insert(&rec);
        }
        outPipe.ShutDown();
    });
}

void Project::WaitUntilDone() { worker.wait(); }

void *DuplicateRemoval::bigQDuplicateRemoval(void *voidArgs) {
	DuplicateRemoval *args = (DuplicateRemoval *)voidArgs;
	Pipe sortedOutput(100); //this buffer size is based on the test input
	OrderMaker om(args->schema);
	BigQ bigQInstance(*(args->in), sortedOutput, om, 1);
	// TODO: make sure that the input pipe is shutdown in its predecessor 
	
	Record currentRec;
	Record previousRec;
	if(sortedOutput.Remove(&currentRec)) {
		previousRec.Consume(&currentRec);
	}
	
	ComparisonEngine comp;
	while (sortedOutput.Remove(&currentRec)) {
		//Compare current record to previous
		//If equal, skip current record
		if(comp.Compare(&currentRec, &previousRec, &om) == 0) {
			continue;
		}
		//Else write previous record to output and store current in previous
		args->out->Insert(&previousRec);
		previousRec.Consume(&currentRec);
	}
	
	if(previousRec.bits) {
		//write previous record to output
		args->out->Insert(&previousRec);
	}
	args->out->ShutDown();
	pthread_exit(NULL);
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    in = &inPipe;
    out = &outPipe;
    schema = &mySchema;

    /* Initialize and set thread detached attribute */
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&thread, NULL, &DuplicateRemoval::bigQDuplicateRemoval,
                   this);
    pthread_attr_destroy(&attr);
}

void DuplicateRemoval::WaitUntilDone() { pthread_join(thread, NULL); }

void *Sum::computeSum(void *voidArgs) {
    Sum *args = (Sum *)voidArgs;
    Record rec;
    double sum = 0;
    while (args->in->Remove(&rec)) {
        int ival = 0;
        double dval = 0;
        args->func->Apply(rec, ival, dval);
        sum += (ival + dval);
    }
    Attribute sumType = {"double", Double};
    Schema out_schema("out_schema", 1, &sumType);
    Record sumRec;
    std::string val = std::to_string(sum) + "|";
    sumRec.ComposeRecord(&out_schema, val.c_str());
    args->out->Insert(&sumRec);
    args->out->ShutDown();
    pthread_exit(NULL);
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    in = &inPipe;
    out = &outPipe;
    func = &computeMe;

    /* Initialize and set thread detached attribute */
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    pthread_create(&thread, NULL, &Sum::computeSum, this);
    pthread_attr_destroy(&attr);
}

void Sum::WaitUntilDone() { pthread_join(thread, NULL); }

void *GroupBy::performGrouping(void *voidArgs) {
	GroupBy *args = (GroupBy *)voidArgs;
	// TODO: Determine how to fix buffer size
	Pipe groupedOutput(100);
	BigQ bigQInstance(*(args->in), groupedOutput, *(args->groupingAttributes), 1);

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
	int attsToKeep[1+grpAttrCount];
	attsToKeep[0] = 0;
	int *groupAttrList = args->groupingAttributes->getAttributes();
	std::copy(groupAttrList, groupAttrList + grpAttrCount, attsToKeep + 1);
	ComparisonEngine comp;
	Pipe *currentGroup = new Pipe(100);
	Pipe *groupSum = new Pipe(1);
	Sum S;
	Record summedResult;
	S.Run(*currentGroup,*groupSum,*(args->func));
		
	while (groupedOutput.Remove(&currentRec)) {
		if (comp.Compare(&currentRec, &previousRec, args->groupingAttributes) == 0) {
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
			summedResult.MergeRecords (&sumRec, &prevRecCopy, 1, grpAttrCount, attsToKeep, 1+grpAttrCount, 1); 
			args->out->Insert(&summedResult);
			free(currentGroup);
			free(groupSum);
			currentGroup = new Pipe(100);
			groupSum = new Pipe(1);
			S.Run(*currentGroup,*groupSum,*(args->func));
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
	summedResult.MergeRecords (&sumRec, &prevRecCopy, 1, grpAttrCount, attsToKeep, 1+grpAttrCount, 1); 
	args->out->Insert(&summedResult);
	free(currentGroup);
	free(groupSum);
	args->out->ShutDown();
	pthread_exit(NULL);
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
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

void GroupBy::WaitUntilDone () { 
	pthread_join (thread, NULL);
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
        if (record.size() + args->buffer.size() > args->buffer.capacity()) {
            fputs(args->buffer.c_str(), args->outputFile);
            args->buffer.clear();
        }
        args->buffer += record;
    }
    fputs(args->buffer.c_str(), args->outputFile);
    fclose(args->outputFile);
    pthread_exit(NULL);
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    // Set default capacity of buffer to 1 page
    if (buffer.capacity() < PAGE_SIZE) Use_n_Pages(1);
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
void WriteOut::WaitUntilDone() { pthread_join(thread, NULL); }
void WriteOut::Use_n_Pages(int n) { buffer.reserve(n * PAGE_SIZE); }
