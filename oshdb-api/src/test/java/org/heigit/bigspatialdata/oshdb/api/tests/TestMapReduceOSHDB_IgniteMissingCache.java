package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.junit.Test;

public class TestMapReduceOSHDB_IgniteMissingCache extends TestMapReduceOSHDB_Ignite {
  public TestMapReduceOSHDB_IgniteMissingCache() throws Exception {
    super(new OSHDBIgnite(ignite));
    this.oshdb.prefix("<test caches not present>");
  }

  @Override
  @Test(expected = OSHDBTableNotFoundException.class)
  public void testOSMContributionView() throws Exception {
    super.testOSMContributionView();
  }

  @Override
  @Test(expected = OSHDBTableNotFoundException.class)
  public void testOSMEntitySnapshotView() throws Exception {
    super.testOSMEntitySnapshotView();
  }

  @Override
  @Test(expected = OSHDBTableNotFoundException.class)
  public void testOSMContributionViewStream() throws Exception {
    super.testOSMEntitySnapshotView();
  }

  @Override
  @Test(expected = OSHDBTableNotFoundException.class)
  public void testOSMEntitySnapshotViewStream() throws Exception {
    super.testOSMEntitySnapshotView();
  }

  @Override
  @Test(expected = OSHDBTableNotFoundException.class)
  public void testTimeoutMapReduce() throws Exception {
    super.testTimeoutMapReduce();
  }

  @Override
  @Test(expected = OSHDBTableNotFoundException.class)
  public void testTimeoutStream() throws Exception {
    super.testTimeoutStream();
  }
}
