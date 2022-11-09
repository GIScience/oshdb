package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.api.db.OSHDBIgnite.ComputeMode.LOCAL_PEEK;

/**
 * {@inheritDoc}
 *
 * <p>Runs the tests using the "local peek" Ignite backend.</p>
 */
class MapReduceOSHDBIgniteLocalPeekTest extends MapReduceOSHDBIgniteTest {
  /**
   * Creates the test runner using the ignite localpeak backend.
   *
   * @throws Exception if something goes wrong
   */
  MapReduceOSHDBIgniteLocalPeekTest() throws Exception {
    super(oshdb -> oshdb.computeMode(LOCAL_PEEK));
  }
}
