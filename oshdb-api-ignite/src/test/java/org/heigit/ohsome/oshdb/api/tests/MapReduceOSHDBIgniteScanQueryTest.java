package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.api.db.OSHDBIgnite.ComputeMode.SCAN_QUERY;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * {@inheritDoc}
 *
 * <p>Runs the tests using the "scan query" Ignite backend.</p>
 */
class MapReduceOSHDBIgniteScanQueryTest extends MapReduceOSHDBIgniteTest {
  /**
   * Creates the test runner using the ignite scanquery backend.
   *
   * @throws Exception if something goes wrong
   */
  MapReduceOSHDBIgniteScanQueryTest() throws Exception {
    super(oshdb -> oshdb.computeMode(SCAN_QUERY));
  }

  @Override
  @Test
  void testTimeoutStream() {
    // ignore this test -> scanquery backend currently doesn't support timeouts for stream()
    assertTrue(true);
  }
}
