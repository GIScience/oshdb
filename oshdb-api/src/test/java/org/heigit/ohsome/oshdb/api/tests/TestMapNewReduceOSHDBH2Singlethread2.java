package org.heigit.ohsome.oshdb.api.tests;

import org.heigit.ohsome.oshdb.api.db.OSHDBH2;

/**
 * {@inheritDoc}
 *
 * <p>Runs the tests using the singlethreaded H2 backend.</p>
 */
class TestMapNewReduceOSHDBH2Singlethread2 extends TestMapNewReducer {
  /**
   * Creates the test runner using the singlethreaded "dummy" H2 backend.
   *
   * @throws Exception if something goes wrong
   */
  TestMapNewReduceOSHDBH2Singlethread2() throws Exception {
    super(
        (new OSHDBH2("./src/test/resources/test-data")).multithreading(false)
    );
  }
}
