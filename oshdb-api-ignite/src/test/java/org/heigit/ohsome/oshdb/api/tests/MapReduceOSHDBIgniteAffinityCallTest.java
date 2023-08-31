package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.api.db.OSHDBIgnite.ComputeMode.AFFINITY_CALL;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * {@inheritDoc}
 *
 * <p>Runs the tests using the "affinity call" Ignite backend.</p>
 */
class MapReduceOSHDBIgniteAffinityCallTest extends MapReduceOSHDBIgniteTest {
  /**
   * Creates the test runner using the ignite affinitycall backend.
   *
   * @throws Exception if something goes wrong
   */
  MapReduceOSHDBIgniteAffinityCallTest() throws Exception {
    super(oshdb -> oshdb.computeMode(AFFINITY_CALL));
  }
}
