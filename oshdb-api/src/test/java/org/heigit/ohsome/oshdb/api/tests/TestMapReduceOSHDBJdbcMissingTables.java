package org.heigit.ohsome.oshdb.api.tests;

import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTableNotFoundException;
import org.junit.Test;

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
