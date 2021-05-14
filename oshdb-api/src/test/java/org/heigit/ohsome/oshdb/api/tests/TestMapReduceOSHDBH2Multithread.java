package org.heigit.ohsome.oshdb.api.tests;

import org.heigit.ohsome.oshdb.api.db.OSHDBH2;

/**
 * {@inheritDoc}
 *
 * <p>Runs the tests using the multithreaded H2 backend.</p>
 */
public class TestMapReduceOSHDBH2Multithread extends TestMapReduce {
  /**
   * Creates the test runner using the multithreaded H2 backend.
   *
   * @throws Exception if something goes wrong
   */
  public TestMapReduceOSHDBH2Multithread() throws Exception {
    super(
        (new OSHDBH2("./src/test/resources/test-data")).multithreading(true)
    );
  }
}
