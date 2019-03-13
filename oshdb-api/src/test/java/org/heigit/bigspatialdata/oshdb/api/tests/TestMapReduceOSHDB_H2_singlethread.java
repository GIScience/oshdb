package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;

public class TestMapReduceOSHDB_H2_singlethread extends TestMapReduce {
  public TestMapReduceOSHDB_H2_singlethread() throws Exception {
    super(
        (new OSHDBH2("./src/test/resources/test-data")).multithreading(false)
    );
  }
}
