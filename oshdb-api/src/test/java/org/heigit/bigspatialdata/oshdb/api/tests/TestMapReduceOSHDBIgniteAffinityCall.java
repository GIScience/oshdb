package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import java.util.Set;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;

public class TestMapReduceOSHDBIgniteAffinityCall extends TestMapReduceOSHDBIgnite {
  public TestMapReduceOSHDBIgniteAffinityCall() throws Exception {
    super(new OSHDBIgnite(ignite).computeMode(OSHDBIgnite.ComputeMode.AffinityCall));
  }

  @Test
  public void testOSMEntitySnapshotViewStreamNullValues() throws Exception {
    // simple stream query
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .timestamps(new OSHDBTimestamps("2010-01-01", "2015-01-01", OSHDBTimestamps.Interval.YEARLY))
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(snapshot -> snapshot.getEntity().getUserId())
        .map(x -> (Integer) null)
        .stream()
        .collect(Collectors.toSet());

    assertEquals(1, result.size());
  }
}
