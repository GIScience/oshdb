package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;

public class TestMapReduceOSHDB_Ignite_LocalPeek extends TestMapReduceOSHDB_Ignite {
  public TestMapReduceOSHDB_Ignite_LocalPeek() throws Exception {
    super(new OSHDBIgnite(ignite).computeMode(OSHDBIgnite.ComputeMode.LocalPeek));
  }
}
