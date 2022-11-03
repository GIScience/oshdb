package org.heigit.ohsome.oshdb.api.tests;

import org.heigit.ohsome.oshdb.api.db.OSHDBH2;

/**
 * {@inheritDoc}
 *
 * <p>Runs the tests using the singlethreaded H2 backend.</p>
 */
class MapReduceOSHDBH2SinglethreadTest extends MapReduceTest {
  /**
   * Creates the test runner using the singlethreaded "dummy" H2 backend.
   *
   * @throws Exception if something goes wrong
   */
  MapReduceOSHDBH2SinglethreadTest() throws Exception {
    super(
        (new OSHDBH2("../data/test-data")).multithreading(false)
    );
  }
}
