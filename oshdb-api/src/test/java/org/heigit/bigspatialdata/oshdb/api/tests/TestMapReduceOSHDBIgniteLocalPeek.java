package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;

public class TestMapReduceOSHDBIgniteLocalPeek extends TestMapReduceOSHDBIgnite {
  public TestMapReduceOSHDBIgniteLocalPeek() throws Exception {
    super(new OSHDBIgnite(ignite).computeMode(OSHDBIgnite.ComputeMode.LocalPeek));
  }
}
