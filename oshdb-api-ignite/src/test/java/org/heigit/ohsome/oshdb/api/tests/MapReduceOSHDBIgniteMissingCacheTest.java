package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.junit.jupiter.api.Test;

/**
 * Tests for proper error messages is caches are not pnt on ignite.
 */
class MapReduceOSHDBIgniteMissingCacheTest extends MapReduceOSHDBIgniteTest {
  /**
   * Creates the test runner using an Ignite backend.
   *
   * @throws Exception if something goes wrong
   */
  MapReduceOSHDBIgniteMissingCacheTest() throws Exception {
    super(oshdb -> {});
    this.oshdb.prefix("<test caches not present>");
  }

  @Override
  @Test()
  void testOSMContributionView() {
    assertThrows(OSHDBTableNotFoundException.class, super::testOSMContributionView);
  }

  @Override
  @Test()
  void testOSMEntitySnapshotView() {
    assertThrows(OSHDBTableNotFoundException.class, super::testOSMEntitySnapshotView);
  }

  @Override
  @Test()
  void testOSMContributionViewStream() {
    assertThrows(OSHDBTableNotFoundException.class, super::testOSMContributionViewStream);
  }

  @Override
  @Test()
  void testOSMEntitySnapshotViewStream() {
    assertThrows(OSHDBTableNotFoundException.class, super::testOSMEntitySnapshotViewStream);
  }

  @Override
  @Test()
  void testTimeoutMapReduce() {
    assertThrows(OSHDBTableNotFoundException.class, this::timeoutMapReduce);
  }

  @Override
  @Test()
  void testTimeoutStream() {
    assertThrows(OSHDBTableNotFoundException.class, this::timeoutStream);
  }
}
