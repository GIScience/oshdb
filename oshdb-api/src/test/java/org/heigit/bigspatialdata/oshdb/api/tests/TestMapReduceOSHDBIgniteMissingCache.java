package org.heigit.bigspatialdata.oshdb.api.tests;

import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.junit.Test;

/**
 * Tests for proper error messages is caches are not pnt on ignite.
 */
public class TestMapReduceOSHDBIgniteMissingCache extends TestMapReduceOSHDBIgnite {
  /**
   * Creates the test runner using an Ignite backend.
   * @throws Exception if something goes wrong
   */
  public TestMapReduceOSHDBIgniteMissingCache() throws Exception {
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
