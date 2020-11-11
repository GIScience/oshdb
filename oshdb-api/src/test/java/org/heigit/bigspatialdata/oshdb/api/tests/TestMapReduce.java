package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;

/**
 * Test basic map-reduce method of the OSHDB API.
 */
abstract class TestMapReduce {
  final OSHDBDatabase oshdb;
  OSHDBJdbc keytables = null;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
  private final OSHDBTimestamps timestamps6 = new OSHDBTimestamps("2010-01-01", "2015-01-01",
      OSHDBTimestamps.Interval.YEARLY);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestMapReduce(OSHDBDatabase oshdb) throws Exception {
    this.oshdb = oshdb;
  }

  protected MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    MapReducer<OSMContribution> mapRed = OSMContributionView.on(oshdb);
    if (this.keytables != null) {
      mapRed = mapRed.keytables(this.keytables);
    }
    return mapRed.osmType(OSMType.NODE).osmTag("highway").areaOfInterest(bbox);
  }

  protected MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    MapReducer<OSMEntitySnapshot> mapRed = OSMEntitySnapshotView.on(oshdb);
    if (this.keytables != null) {
      mapRed = mapRed.keytables(this.keytables);
    }
    return mapRed.osmType(OSMType.NODE).osmTag("highway").areaOfInterest(bbox);
  }

  @Test
  public void testOSMContributionView() throws Exception {
    // simple query
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(OSMContribution::getContributorUserId)
        .uniq();

    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.size());

    // "flatMap"
    result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(OSMContribution::getContributorUserId)
        .filter(uid -> uid > 0)
        .uniq();

    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.size());

    // "groupByEntity"
    assertEquals(7, createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .groupByEntity()
        .map(List::size)
        .sum()
    );
  }

  @Test
  public void testOSMEntitySnapshotView() throws Exception {
    // simple query
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(snapshot -> snapshot.getEntity().getUserId())
        .uniq();

    assertEquals(3, result.size());

    // "flatMap"
    result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(snapshot -> snapshot.getEntity().getUserId())
        .filter(uid -> uid > 0)
        .uniq();

    assertEquals(3, result.size());

    // "groupByEntity"
    assertEquals(5, createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .groupByEntity()
        .map(List::size)
        .sum()
    );
  }

  @Test
  public void testOSMContributionViewStream() throws Exception {
    // simple query
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(OSMContribution::getContributorUserId)
        .stream()
        .collect(Collectors.toSet());

    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.size());

    // "flatMap"
    result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(OSMContribution::getContributorUserId)
        .filter(uid -> uid > 0)
        .stream()
        .collect(Collectors.toSet());

    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.size());

    // "groupByEntity"
    assertEquals(7, createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .groupByEntity()
        .map(List::size)
        .stream()
        .mapToInt(x -> x)
        .reduce(0, Integer::sum)
    );
  }

  @Test
  public void testOSMEntitySnapshotViewStream() throws Exception {
    // simple stream query
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(snapshot -> snapshot.getEntity().getUserId())
        .stream()
        .collect(Collectors.toSet());

    assertEquals(3, result.size());

    // "flatMap"
    result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(snapshot -> snapshot.getEntity().getUserId())
        .filter(uid -> uid > 0)
        .stream()
        .collect(Collectors.toSet());

    assertEquals(3, result.size());

    // "groupByEntity"
    assertEquals(5, createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .groupByEntity()
        .map(List::size)
        .stream()
        .mapToInt(x -> x)
        .reduce(0, Integer::sum)
    );
  }

  @SuppressWarnings("ResultOfMethodCallIgnored") // we only test for thrown exceptions here
  @Test(expected = OSHDBTimeoutException.class)
  public void testTimeoutMapReduce() throws Exception {
    // set short timeout -> query should fail
    oshdb.timeoutInMilliseconds(30);

    // simple query with a sleep. would take about ~500ms (1 entity for ~5 timestamp)
    createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(delay(100))
        .count();

    // reset timeout
    oshdb.timeoutInMilliseconds(Long.MAX_VALUE);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored") // we only test for thrown exceptions here
  @Test(expected = OSHDBTimeoutException.class)
  public void testTimeoutStream() throws Exception {
    // set super short timeout -> all queries should fail
    oshdb.timeoutInMilliseconds(30);

    // simple query
    createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .osmEntityFilter(entity -> entity.getId() == 617308093)
        .map(snapshot -> snapshot.getEntity().getId())
        .map(delay(100))
        .stream()
        .count();

    // reset timeout
    oshdb.timeoutInMilliseconds(Long.MAX_VALUE);
  }

  private static <T> SerializableFunction<T, T> delay(int ms) {
    return x -> {
      try {
        Thread.sleep(ms);
        return x;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
  }

}
