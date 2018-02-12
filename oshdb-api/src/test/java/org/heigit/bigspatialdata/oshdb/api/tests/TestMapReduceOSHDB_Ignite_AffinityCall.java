package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;

public class TestMapReduceOSHDB_Ignite_AffinityCall extends TestMapReduceOSHDB_Ignite {
  public TestMapReduceOSHDB_Ignite_AffinityCall() throws Exception {
    super(new OSHDBIgnite(ignite).computeMode(OSHDBIgnite.ComputeMode.AffinityCall));
  }
}
