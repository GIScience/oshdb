package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.heigit.ohsome.oshdb.api.db.OSHDBIgnite;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.junit.jupiter.api.Test;

/**
 * Tests for proper error messages is caches are not pnt on ignite.
 */
public class TestMapReduceOSHDBIgniteMissingCache extends TestMapReduceOSHDBIgnite {
  /**
   * Creates the test runner using an Ignite backend.
   *
   * @throws Exception if something goes wrong
   */
  public TestMapReduceOSHDBIgniteMissingCache() throws Exception {
    super(new OSHDBIgnite(ignite));
    this.oshdb.prefix("<test caches not present>");
  }

  @Override
  @Test()
  public void testOSMContributionView() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, () -> {
      super.testOSMContributionView();
    });
  }

  @Override
  @Test()
  public void testOSMEntitySnapshotView() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, () -> {
      super.testOSMEntitySnapshotView();
    });
  }

  @Override
  @Test()
  public void testOSMContributionViewStream() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, () -> {
      super.testOSMEntitySnapshotView();
    });
  }

  @Override
  @Test()
  public void testOSMEntitySnapshotViewStream() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, () -> {
      super.testOSMEntitySnapshotView();
    });
  }

  @Override
  @Test()
  public void testTimeoutMapReduce() throws Exception {
    // no-op no test for timeout if we got a missing table!
  }

  @Override
  @Test()
  public void testTimeoutStream() throws Exception {
    // no-op no test for timeout if we got a missing table!
  }
}
