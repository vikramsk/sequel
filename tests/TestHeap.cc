#include <DBFile.h>
#include <iostream>
#include "gtest/gtest.h"

TEST(HeapFileTest, Create) {
    DBFile dbfile;
    int ans;
    ans = dbfile.Create("./test", heap, NULL);
    cout << ans << endl;
    ASSERT_EQ(ans, 1);
}
