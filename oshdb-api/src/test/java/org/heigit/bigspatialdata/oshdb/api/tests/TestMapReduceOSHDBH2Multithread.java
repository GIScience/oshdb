package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;

public class TestMapReduceOSHDBH2Multithread extends TestMapReduce {
  /**
   * Creates the test runner using the multithreaded H2 backend.
   * @throws Exception if something goes wrong
   */
  public TestMapReduceOSHDBH2Multithread() throws Exception {
    super(
        (new OSHDBH2("./src/test/resources/test-data")).multithreading(true)
    );
  }
}
