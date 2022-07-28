package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.generic.WeightedValue;
import org.heigit.ohsome.oshdb.api.mapreducer.reduction.Reduce;
import org.heigit.ohsome.oshdb.api.mapreducer.snapshot.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test special reducers of the OSHDB API when using the contribution view.
 */
class TestHelpersOSMEntitySnapshotView {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  private static final double DELTA = 1e-8;

  TestHelpersOSMEntitySnapshotView() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private OSMEntitySnapshotView createMapReducer() throws Exception {
    return OSMEntitySnapshotView.view()
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  @Test
  void testSum() throws Exception {
    // single timestamp
    Map<OSHDBTimestamp, Long> result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(timestamps1.get().first()));

    // many timestamps
    Map<OSHDBTimestamp, Long> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(51, result2.entrySet().size());
    assertEquals(null, result2.get(timestamps72.get().first()));
    assertEquals(42, result2.get(timestamps72.get().last()));

    // total
    Long result3 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(42, result3);

    // custom aggregation identifier
    Map<Boolean, Long> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .map(snapshot -> 1)
        .reduce(Reduce::sumInt);

    assertEquals(21, result4.get(true));
    assertEquals(21, result4.get(false));
  }

  @Test
  void testCount() throws Exception {
    // single timestamp
    var result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .count();

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(timestamps1.get().first()).intValue());

    // many timestamps
    var result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .count();

    assertEquals(51, result2.entrySet().size());
    assertEquals(null, result2.get(timestamps72.get().first()));
    assertEquals(42, result2.get(timestamps72.get().last()).intValue());

    // total
    var result3 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .count();

    assertEquals(42L, result3);

    // custom aggregation identifier
    var result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .count();

    assertEquals(21, result4.get(true).intValue());
    assertEquals(21, result4.get(false).intValue());
  }

  @Test
  void testAverage() throws Exception {
    // single timestamp
    Double result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .map(snapshot -> snapshot.getEntity().getId() % 2)
        .reduce(Reduce::average);

    assertEquals(0.5, result1.doubleValue(), DELTA);

    // many timestamps
    Map<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .map(snapshot -> snapshot.getEntity().getId() % 2)
        .reduce(Reduce::average);

    assertEquals(51, result2.entrySet().size());
    assertEquals(null, result2.get(timestamps72.get().first()));
    assertEquals(0.5, result2.get(timestamps72.get().last()).doubleValue(), DELTA);

    // custom aggregation identifier
    Map<Boolean, Double> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .map(snapshot -> snapshot.getEntity().getId() % 2)
        .reduce(Reduce::average);

    assertEquals(0.0, result4.get(true).doubleValue(), DELTA);
    assertEquals(1.0, result4.get(false).doubleValue(), DELTA);
  }

  @Test
  void testWeightedAverage() throws Exception {
    // single timestamp
    Double result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .map(snapshot -> new WeightedValue(
            snapshot.getEntity().getId() % 2,
            1 * (snapshot.getEntity().getId() % 2)
            ))
        .reduce(Reduce::weightedAverage);

    assertEquals(1.0, result1.doubleValue(), DELTA);

    // many timestamps
    Map<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .map(snapshot -> new WeightedValue(
            snapshot.getEntity().getId() % 2,
            2 * (snapshot.getEntity().getId() % 2)
            ))
        .reduce(Reduce::weightedAverage);

    assertEquals(51, result2.entrySet().size());
    assertEquals(null, result2.get(timestamps72.get().first()));
    assertEquals(1.0, result2.get(timestamps72.get().last()).doubleValue(), DELTA);

    // custom aggregation identifier
    Map<Boolean, Double> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .map(snapshot -> new WeightedValue(
            snapshot.getEntity().getId() % 2,
            2 * (snapshot.getEntity().getId() % 2)
            ))
        .reduce(Reduce::weightedAverage);

    assertEquals(Double.NaN, result4.get(true).doubleValue(), DELTA);
    assertEquals(1.0, result4.get(false).doubleValue(), DELTA);
  }

  @Test
  void testUniq() throws Exception {
    // single timestamp
    Map<OSHDBTimestamp, Set<Long>> result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .map(snapshot -> snapshot.getEntity().getId())
        .reduce(Reduce::uniq);

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(timestamps1.get().first()).size());

    // many timestamps
    Map<OSHDBTimestamp, Set<Long>> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp(OSMEntitySnapshot::getTimestamp)
        .map(snapshot -> snapshot.getEntity().getId())
        .reduce(Reduce::uniq);

    assertEquals(51, result2.entrySet().size());
    assertEquals(null, result2.get(timestamps72.get().first()));
    assertEquals(42, result2.get(timestamps72.get().last()).size());

    // total
    Set<Long> result3 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .map(snapshot -> snapshot.getEntity().getId())
        .reduce(Reduce::uniq);

    assertEquals(42, result3.size());

    // custom aggregation identifier
    Map<Boolean, Set<Long>> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .on(oshdb)
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .map(snapshot -> snapshot.getEntity().getId())
        .reduce(Reduce::uniq);

    assertEquals(21, result4.get(true).size());
    assertEquals(21, result4.get(false).size());
  }

}
