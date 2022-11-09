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

  @Test
  void testOSMEntitySnapshotViewStreamNullValues() throws Exception {
    // simple stream query
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .timestamps(
            new OSHDBTimestamps("2010-01-01", "2015-01-01", OSHDBTimestamps.Interval.YEARLY))
        .filter("id:617308093")
        .map(snapshot -> snapshot.getEntity().getUserId())
        .map(x -> (Integer) null)
        .stream()
        .collect(Collectors.toSet());

    assertEquals(1, result.size());
  }
}
