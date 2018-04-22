#include "QueryPlanner.h"
#include <iostream>

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
        case JOIN:
            cout << "JOIN : " << endl;
            cout << ">>> Input pipe ID 1 : " << inPipeL_ID << endl;
            cout << ">>> Input pipe ID 2 : " << inPipeR_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> CNF : ";
            cnf.Print();
            break;
        case SELFILE:
            cout << "SELECT FILE : " << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Input file : " << endl;  // TODO: Print filename
            cout << ">>> CNF : ";
            cnf.Print();
            break;
        case PROJECT:
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
            break;
        case SUM:
            cout << "SUM : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Function : " << endl;  // TODO: Print function
            break;
        case GROUPBY:
            cout << "GROUP BY : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Attributes to group by : ";
            groupOrder->Print();
            cout << ">>> Function : " << endl;  // TODO: Print function
            break;
        case DISTINCT:
            cout << "DUPLICATE REMOVAL : " << endl;
            cout << ">>> Input pipe ID : " << inPipeL_ID << endl;
            cout << ">>> Output pipe ID : " << outPipe_ID << endl;
            cout << ">>> Schema : ";
            OrderMaker om(outSchema);
            om.Print();
            break;
            // Note that SELPIPE is not used here
    }
    cout << endl;
}
