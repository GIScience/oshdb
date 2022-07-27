package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.junit.jupiter.api.Test;

/**
 * Tests for proper error messages if tables are not present on H2.
 */
class TestMapReduceOSHDBJdbcMissingTables extends TestMapReduce {
  /**
   * Creates the test runner using an H2 backend.
   *
   * @throws Exception if something goes wrong
   */
  TestMapReduceOSHDBJdbcMissingTables() throws Exception {
    super((new OSHDBH2("../data/test-data"))
        .prefix("<test tables not present>")
    );
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
  void testTimeoutMapReduce() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, this::timeoutMapReduce);
  }

  @Override
  @Test()
  void testTimeoutStream() {
    assertThrows(OSHDBTableNotFoundException.class, this::timeoutStream);
  }
}
