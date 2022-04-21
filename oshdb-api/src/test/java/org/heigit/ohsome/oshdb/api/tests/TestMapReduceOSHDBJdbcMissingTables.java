package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.junit.jupiter.api.Test;

/**
 * Tests for proper error messages if tables are not present on H2.
 */
public class TestMapReduceOSHDBJdbcMissingTables extends TestMapReduce {
  /**
   * Creates the test runner using an H2 backend.
   *
   * @throws Exception if something goes wrong
   */
  public TestMapReduceOSHDBJdbcMissingTables() throws Exception {
    super((new OSHDBH2("./src/test/resources/test-data"))
        .prefix("<test tables not present>")
    );
  }

  @Override
  @Test()
  public void testOSMContributionView() {
    assertThrows(OSHDBTableNotFoundException.class, () -> {
      super.testOSMContributionView();
    });
  }

  @Override
  @Test()
  public void testOSMEntitySnapshotView() {
    assertThrows(OSHDBTableNotFoundException.class, () -> {
      super.testOSMEntitySnapshotView();
    });
  }

  @Override
  @Test()
  public void testOSMContributionViewStream() {
    assertThrows(OSHDBTableNotFoundException.class, () -> {
      super.testOSMEntitySnapshotView();
    });
  }

  @Override
  @Test()
  public void testOSMEntitySnapshotViewStream() {
    assertThrows(OSHDBTableNotFoundException.class, () -> {
      super.testOSMEntitySnapshotView();
    });
  }

  @Override
  @Test()
  public void testTimeoutMapReduce() throws Exception {
    assertThrows(OSHDBTableNotFoundException.class, () -> {
      timeoutMapReduce();
    });
  }

  @Override
  @Test()
  public void testTimeoutStream() {
    assertThrows(OSHDBTableNotFoundException.class, () -> {
      timeoutStream();
    });
  }
}
