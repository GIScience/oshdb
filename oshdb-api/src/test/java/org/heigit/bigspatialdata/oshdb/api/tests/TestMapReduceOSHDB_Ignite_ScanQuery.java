package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.junit.Test;

public class TestMapReduceOSHDB_Ignite_ScanQuery extends TestMapReduceOSHDB_Ignite {
  public TestMapReduceOSHDB_Ignite_ScanQuery() throws Exception {
    super(new OSHDBIgnite(ignite).computeMode(OSHDBIgnite.ComputeMode.ScanQuery));
  }

  @Override
  @Test
  public void testTimeoutStream() throws Exception {
    // ignore this test -> scanquery backend currently doesn't support timeouts for stream()
  }
}
