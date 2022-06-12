package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;
import java.util.stream.Collectors;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Agg;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.api.mapreducer.snapshot.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Base class for testing the map-reducer backend implementations of the OSHDB API.
 */
abstract class TestMapReduce {
  final OSHDBDatabase oshdb;
  OSHDBJdbc keytables = null;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps6 = new OSHDBTimestamps("2010-01-01", "2015-01-01",
      OSHDBTimestamps.Interval.YEARLY);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestMapReduce(OSHDBDatabase oshdb) throws Exception {
    this.oshdb = oshdb;
  }

  protected OSHDBView<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb, keytables)
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  protected OSHDBView<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
    return OSMEntitySnapshotView.on(oshdb, keytables)
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  @Test
  void testOSMContributionView() throws Exception {
    // simple query
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .view()
        .map(OSMContribution::getContributorUserId)
        .reduce(Agg::uniq);

    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.size());

    // "flatMap"
    result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .view()
        .map(OSMContribution::getContributorUserId)
        .filter(uid -> uid > 0)
        .reduce(Agg::uniq);

    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.size());
  }

  @Test
  void testOSMEntitySnapshotView() throws Exception {
    // simple query
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .filter("id:617308093")
        .view()
        .map(snapshot -> snapshot.getEntity().getUserId())
        .uniq();

    assertEquals(3, result.size());

    // "flatMap"
    result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .filter("id:617308093")
        .view()
        .map(snapshot -> snapshot.getEntity().getUserId())
        .filter(uid -> uid > 0)
        .uniq();

    assertEquals(3, result.size());
  }

  @Test
  void testOSMContributionViewStream() throws Exception {
    // simple query
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .view()
        .map(OSMContribution::getContributorUserId)
        .stream()
        .collect(Collectors.toSet());

    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.size());

    // "flatMap"
    result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .filter("id:617308093")
        .view()
        .map(OSMContribution::getContributorUserId)
        .filter(uid -> uid > 0)
        .stream()
        .collect(Collectors.toSet());

    // should be 5: first version doesn't have the highway tag, remaining 7 versions have 5
    // different contributor user ids
    assertEquals(5, result.size());
  }

  @Test
  void testOSMEntitySnapshotViewStream() throws Exception {
    // simple stream query
    Set<Integer> result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .filter("id:617308093")
        .view()
        .map(snapshot -> snapshot.getEntity().getUserId())
        .stream()
        .collect(Collectors.toSet());

    assertEquals(3, result.size());

    // "flatMap"
    result = createMapReducerOSMEntitySnapshot()
        .timestamps(timestamps6)
        .filter("id:617308093")
        .view()
        .map(snapshot -> snapshot.getEntity().getUserId())
        .filter(uid -> uid > 0)
        .stream()
        .collect(Collectors.toSet());

    assertEquals(3, result.size());
  }

  @Test
  void testTimeoutMapReduce() throws Exception {
    assertThrows(OSHDBTimeoutException.class, this::timeoutMapReduce);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored") // we only test for thrown exceptions here
  protected void timeoutMapReduce() throws Exception {
    // set short timeout -> query should fail
    oshdb.timeoutInMilliseconds(1);

    try {
      // simple query with a sleep. would take about ~500ms (1 entity for ~5 timestamp)
      createMapReducerOSMEntitySnapshot()
          .timestamps(timestamps6)
          .filter("id:617308093")
          .view()
          .map(delay(100))
          .count();
    } finally {
      // reset timeout
      oshdb.timeoutInMilliseconds(Long.MAX_VALUE);
    }
  }

  @Test
  void testTimeoutStream() throws Exception {
    assertThrows(OSHDBTimeoutException.class, this::timeoutStream);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored") // we only test for thrown exceptions here
  protected void timeoutStream() throws Exception {
    // set super short timeout -> all queries should fail
    oshdb.timeoutInMilliseconds(1);

    try {
      // simple query
      createMapReducerOSMEntitySnapshot()
          .timestamps(timestamps6)
          .filter("id:617308093")
          .view()
          .map(snapshot -> snapshot.getEntity().getId())
          .map(delay(100))
          .stream()
          .count();
    } finally {
      // reset timeout
      oshdb.timeoutInMilliseconds(Long.MAX_VALUE);
    }
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
