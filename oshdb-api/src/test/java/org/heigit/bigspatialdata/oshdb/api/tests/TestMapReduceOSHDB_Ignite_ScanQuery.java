package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;

public class TestMapReduceOSHDB_Ignite_ScanQuery extends TestMapReduceOSHDB_Ignite {
  public TestMapReduceOSHDB_Ignite_ScanQuery() throws Exception {
    super(new OSHDBIgnite(ignite).computeMode(OSHDBIgnite.ComputeMode.ScanQuery));
  }
}
