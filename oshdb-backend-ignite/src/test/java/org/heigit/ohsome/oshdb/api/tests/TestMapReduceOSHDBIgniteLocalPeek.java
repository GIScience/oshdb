package org.heigit.ohsome.oshdb.api.tests;

import org.heigit.ohsome.oshdb.api.db.OSHDBIgnite;

/**
 * {@inheritDoc}
 *
 * <p>Runs the tests using the "local peek" Ignite backend.</p>
 */
public class TestMapReduceOSHDBIgniteLocalPeek extends TestMapReduceOSHDBIgnite {
  /**
   * Creates the test runner using the ignite localpeak backend.
   *
   * @throws Exception if something goes wrong
   */
  public TestMapReduceOSHDBIgniteLocalPeek() throws Exception {
    super(new OSHDBIgnite(ignite).computeMode(OSHDBIgnite.ComputeMode.LOCAL_PEEK));
  }
}
