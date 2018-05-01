#include <iostream>
#include "QueryPlanner.h"
#include "cpptoml.h"

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
        case SELPIPE: {
            cout << "SELECT PIPE : " << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Input Pipe ID: " << inPipeL_ID << endl;
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
            cout << ">>> Function : ";
            func.Print();
        } break;
        case GROUPBY: {
            cout << "GROUP BY : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Attributes to group by : " << endl;
            groupOrder->Print();
            cout << ">>> Output Schema: " << endl;
            outSchema->Print();
            cout << ">>> Function : ";
            func.Print();
        } break;
        case DISTINCT: {
            cout << "DUPLICATE REMOVAL : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Output Schema: " << endl;
            outSchema->Print();
        } break;
    }
    cout << endl;
}

int Node::clear_pipe(bool print) {
    Record rec;
    int cnt = 0;
    while (outPipe->Remove(&rec)) {
        if (print) {
            rec.Print(outSchema);
        }
        cnt++;
    }
    return cnt;
}

void QueryPlanner::Execute(int outType) {
    
    if (outType == SET_NONE) {
        Print();
    } else if (outType == SET_STDOUT) {
        Node *ptr = root;
        recurseAndExecute(ptr);
        int cnt = ptr->clear_pipe(true);
        cout << "\n query returned " << cnt << " records \n";
    } else { // tokens.outType == SET_FILE
        Node *ptr = root;
        recurseAndExecute(ptr);
        ptr->relOp->WaitUntilDone();
        cout << "\n query results have been written to file - " << ptr->fileName << endl;
    }   
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

            Join *J = dynamic_cast<Join *>(relOp);
            J->Run(*inPipeL, *inPipeR, *outPipe, cnf, literal);
        } break;
        case SELPIPE: {
            relOp = new SelectPipe();
            relOp->Use_n_Pages(bufferSize);

            SelectPipe *SP = dynamic_cast<SelectPipe *>(relOp);
            SP->Run(*inPipeL, *outPipe, cnf, literal);
        } break;
        case SELFILE: {
            relOp = new SelectFile();
            relOp->Use_n_Pages(bufferSize);

            DBFile *dbfile = new DBFile();
            string fpath = getFilePath();
            dbfile->Open(fpath.c_str());

            SelectFile *SF = dynamic_cast<SelectFile *>(relOp);
            SF->Run(*dbfile, *outPipe, cnf, literal);
        } break;
        case PROJECT: {
            relOp = new Project();
            relOp->Use_n_Pages(bufferSize);

            Project *P = dynamic_cast<Project *>(relOp);
            P->Run(*inPipeL, *outPipe, attsToKeep, numAttsIn, numAttsOut);
        } break;
        case SUM: {
            relOp = new Sum();
            relOp->Use_n_Pages(1);

            Sum *S = dynamic_cast<Sum *>(relOp);
            S->Run(*inPipeL, *outPipe, func);
        } break;
        case GROUPBY: {
            relOp = new GroupBy();
            relOp->Use_n_Pages(bufferSize);

            GroupBy *G = dynamic_cast<GroupBy *>(relOp);
            G->Run(*inPipeL, *outPipe, *groupOrder, func);
        } break;
        case DISTINCT: {
            relOp = new DuplicateRemoval();
            relOp->Use_n_Pages(bufferSize);

            DuplicateRemoval *D = dynamic_cast<DuplicateRemoval *>(relOp);
            D->Run(*inPipeL, *outPipe, *outSchema);
        } break;
        case WRITEOUT: {
            relOp = new WriteOut();
            relOp->Use_n_Pages(bufferSize);
            
            FILE *writeFile = fopen(fileName.c_str(), "w");

            WriteOut *W = dynamic_cast<WriteOut *>(relOp);
            W->Run(*inPipeL, writeFile, *outSchema);
        } break;
    }
}

string Node::getFilePath() {
    auto config = cpptoml::parse_file("config.toml");
    auto dbfileDir = config->get_as<string>("dbfiles");
    string fpath = *dbfileDir + fileName;
    return fpath;
}
