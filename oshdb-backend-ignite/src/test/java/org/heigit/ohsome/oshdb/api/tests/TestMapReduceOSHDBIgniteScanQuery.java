package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.Assert.assertTrue;

import org.heigit.ohsome.oshdb.api.db.OSHDBIgnite;
import org.junit.Test;

/**
 * {@inheritDoc}
 *
 * <p>Runs the tests using the "scan query" Ignite backend.</p>
 */
public class TestMapReduceOSHDBIgniteScanQuery extends TestMapReduceOSHDBIgnite {
  /**
   * Creates the test runner using the ignite scanquery backend.
   *
   * @throws Exception if something goes wrong
   */
  public TestMapReduceOSHDBIgniteScanQuery() throws Exception {
    super(new OSHDBIgnite(ignite).computeMode(OSHDBIgnite.ComputeMode.SCAN_QUERY));
  }

  @Override
  @Test
  public void testTimeoutStream() throws Exception {
    // ignore this test -> scanquery backend currently doesn't support timeouts for stream()
    assertTrue(true);
  }
}
