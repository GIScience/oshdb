package org.heigit.ohsome.oshdb.api.tests;

import org.heigit.ohsome.oshdb.api.db.OSHDBH2;

public class TestMapReduceOSHDBH2Singlethread extends TestMapReduce {
  /**
   * Creates the test runner using the singlethreaded "dummy" H2 backend.
   * @throws Exception if something goes wrong
   */
  public TestMapReduceOSHDBH2Singlethread() throws Exception {
    super(
        (new OSHDBH2("./src/test/resources/test-data")).multithreading(false)
    );
  }
}
