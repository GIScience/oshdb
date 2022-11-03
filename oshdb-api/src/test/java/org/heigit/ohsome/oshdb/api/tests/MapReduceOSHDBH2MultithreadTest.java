package org.heigit.ohsome.oshdb.api.tests;

import org.heigit.ohsome.oshdb.api.db.OSHDBH2;

/**
 * {@inheritDoc}
 *
 * <p>Runs the tests using the multithreaded H2 backend.</p>
 */
class MapReduceOSHDBH2MultithreadTest extends MapReduceTest {
  /**
   * Creates the test runner using the multithreaded H2 backend.
   *
   * @throws Exception if something goes wrong
   */
  MapReduceOSHDBH2MultithreadTest() throws Exception {
    super(
        (new OSHDBH2("../data/test-data")).multithreading(true)
    );
  }
}
