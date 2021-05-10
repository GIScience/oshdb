package org.heigit.ohsome.oshdb.api.tests;

import org.heigit.ohsome.oshdb.api.db.OSHDBH2;

/**
 * {@inheritDoc}
 *
 * <p>Runs the tests using the multithreaded H2 backend.</p>
 */
public class TestFlatMapReduceGroupedByEntityOSHDBH2Multithread extends
    TestFlatMapReduceGroupedByEntity {
  /**
   * Creates the test runner using the multithreaded H2 backend.
   *
   * @throws Exception if something goes wrong
   */
  public TestFlatMapReduceGroupedByEntityOSHDBH2Multithread() throws Exception {
    super(
        (new OSHDBH2("./src/test/resources/test-data")).multithreading(true)
    );
  }
}
