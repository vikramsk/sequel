#include <iostream>
#include "cpptoml.h"
#include "QueryPlanner.h"

int pipeSize = 100;    // buffer sz allowed for each pipe
int bufferSize = 100;  // pages of memory allowed for operations

void QueryPlanner::Print() {
    Node *ptr = root;
    int outCounter = 0;
    recurseAndPrint(ptr, outCounter);
}

int QueryPlanner::recurseAndPrint(Node *ptr, int &outPipe_ID) {
    if (!ptr) return 0;
    int inPipeL_ID = recurseAndPrint(ptr->leftLink, outPipe_ID);
    int inPipeR_ID = recurseAndPrint(ptr->rightLink, outPipe_ID);
    outPipe_ID++;
    ptr->Print(inPipeL_ID, inPipeR_ID, outPipe_ID);
    return outPipe_ID;
}

void Node::Print(int &inPipeL_ID, int &inPipeR_ID, int &outPipe_ID) {
    cout << endl;
    switch (operation) {
        case JOIN: {
            cout << "JOIN : " << endl;
            cout << ">>> Input pipe1 ID : " << inPipeL_ID << endl;
            cout << ">>> Input pipe2 ID : " << inPipeR_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Output Schema: " << endl;
            outSchema->Print();
            cout << ">>> CNF : ";
            cnf.Print();
        } break;
        case SELFILE: {
            cout << "SELECT FILE : " << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Input file : " << fileName << endl;
            cout << ">>> Output Schema: " << endl;
            outSchema->Print();
            cout << ">>> CNF : ";
            cnf.Print();
        } break;
        case PROJECT: {
            cout << "PROJECT : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> No. of input attributes : " << numAttsIn << endl;
            cout << ">>> No. of attributes to keep : " << numAttsOut << endl;
            cout << ">>> Index of attributes to keep : ";
            for (int i = 0; i < numAttsOut; i++) {
                cout << attsToKeep[i] << ' ';
            }
            cout << endl;
            cout << ">>> Output Schema: " << endl;
            outSchema->Print();
        } break;
        case SUM: {
            cout << "SUM : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Output Schema: " << endl;
            outSchema->Print();
            // cout << ">>> Function : " << endl;  // TODO: Print function
        } break;
        case GROUPBY: {
            cout << "GROUP BY : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Attributes to group by : ";
            groupOrder->Print();
            cout << ">>> Output Schema: " << endl;
            outSchema->Print();
            // cout << ">>> Function : " << endl;  // TODO: Print function
        } break;
        case DISTINCT: {
            cout << "DUPLICATE REMOVAL : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Output Schema: " << endl;
            outSchema->Print();
        } break;
            // Note that SELPIPE is not used here
    }
    cout << endl;
}

void QueryPlanner::Execute() {
    // TODO: Read output pipe of ptr based on the output mode set by user
    Node *ptr = root;
    recurseAndExecute(ptr);
}

void QueryPlanner::recurseAndExecute(Node *ptr) {
    if (ptr) {
        recurseAndExecute(ptr->leftLink);
        recurseAndExecute(ptr->rightLink);
        ptr->Execute();
    }
}

void Node::Execute() {
    switch (operation) {
        case JOIN: {
            relOp = new Join();
            relOp->Use_n_Pages(bufferSize);
            outPipe = new Pipe(pipeSize);

            Join *J = dynamic_cast<Join *>(relOp);
            J->Run(*inPipeL, *inPipeR, *outPipe, cnf, literal);
        } break;
        case SELFILE: {
            relOp = new SelectFile();
            relOp->Use_n_Pages(bufferSize);
            outPipe = new Pipe(pipeSize);

            DBFile *dbfile = new DBFile();
            dbfile->Open(getFilePath());

            SelectFile *SF = dynamic_cast<SelectFile *>(relOp);
            SF->Run(*dbfile, *outPipe, cnf, literal);
        } break;
        case PROJECT: {
            relOp = new Project();
            relOp->Use_n_Pages(bufferSize);
            outPipe = new Pipe(pipeSize);

            Project *P = dynamic_cast<Project *>(relOp);
            P->Run(*inPipeL, *outPipe, attsToKeep, numAttsIn, numAttsOut);
        } break;
        case SUM: {
            relOp = new Sum();
            relOp->Use_n_Pages(1);
            outPipe = new Pipe(1);

            Sum *S = dynamic_cast<Sum *>(relOp);
            S->Run(*inPipeL, *outPipe, func);
        } break;
        case GROUPBY: {
            relOp = new GroupBy();
            relOp->Use_n_Pages(bufferSize);
            outPipe = new Pipe(pipeSize);

            GroupBy *G = dynamic_cast<GroupBy *>(relOp);
            G->Run(*inPipeL, *outPipe, *groupOrder, func);
        } break;
        case DISTINCT: {
            relOp = new DuplicateRemoval();
            relOp->Use_n_Pages(bufferSize);
            outPipe = new Pipe(pipeSize);

            DuplicateRemoval *D = dynamic_cast<DuplicateRemoval *>(relOp);
            D->Run(*inPipeL, *outPipe, *outSchema);
        } break;
            // Note that SELPIPE is not used here
    }
}

const char *Node::getFilePath() {
    auto config = cpptoml::parse_file("config.toml");
    auto dbfileDir = config->get_as<string>("dbfiles");
    string fpath = *dbfileDir + fileName;
    return fpath.c_str();
}
