package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Ignite;

public class TestMapReduceOSHDB_Ignite_ScanQuery extends TestMapReduceOSHDB_Ignite {
  public TestMapReduceOSHDB_Ignite_ScanQuery() throws Exception {
    super(new OSHDB_Ignite(ignite).computeMode(OSHDB_Ignite.ComputeMode.ScanQuery));
  }
}
