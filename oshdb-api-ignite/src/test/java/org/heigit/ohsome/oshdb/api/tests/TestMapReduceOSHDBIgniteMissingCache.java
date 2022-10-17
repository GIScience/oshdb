package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.junit.jupiter.api.Test;

/**
 * Tests for proper error messages is caches are not pnt on ignite.
 */
class TestMapReduceOSHDBIgniteMissingCache extends TestMapReduceOSHDBIgnite {
  /**
   * Creates the test runner using an Ignite backend.
   *
   * @throws Exception if something goes wrong
   */
  TestMapReduceOSHDBIgniteMissingCache() throws Exception {
    super(oshdb -> {});
    this.oshdb.prefix("<test caches not present>");
  }

  @Override
  @Test()
  void testOSMContributionView() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, super::testOSMContributionView);
  }

  @Override
  @Test()
  void testOSMEntitySnapshotView() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, super::testOSMEntitySnapshotView);
  }

  @Override
  @Test()
  void testOSMContributionViewStream() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, super::testOSMContributionViewStream);
  }

  @Override
  @Test()
  void testOSMEntitySnapshotViewStream() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, super::testOSMEntitySnapshotViewStream);
  }

  @Override
  @Test()
  void testTimeoutMapReduce() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, this::timeoutMapReduce);
  }

  @Override
  @Test()
  void testTimeoutStream() {
    assertThrows(OSHDBTableNotFoundException.class, this::timeoutStream);
  }
}
