package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.junit.Test;

public class TestMapReduceOSHDBIgniteScanQuery extends TestMapReduceOSHDBIgnite {
  /**
   * Creates the test runner using the ignite scanquery backend.
   * @throws Exception if something goes wrong
   */
  public TestMapReduceOSHDBIgniteScanQuery() throws Exception {
    super(new OSHDBIgnite(ignite).computeMode(OSHDBIgnite.ComputeMode.ScanQuery));
  }

  @Override
  @Test
  public void testTimeoutStream() throws Exception {
    // ignore this test -> scanquery backend currently doesn't support timeouts for stream()
  }
}
