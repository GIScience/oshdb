package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.generic.WeightedValue;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test special reducers of the OSHDB API when using the contribution view.
 */
class HelpersOSMEntitySnapshotViewTest {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  private static final double DELTA = 1e-8;

  HelpersOSMEntitySnapshotViewTest() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private MapReducer<OSMEntitySnapshot> createMapReducer() throws Exception {
    return OSMEntitySnapshotView
        .on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  @Test
  void testSum() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Number> result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .aggregateByTimestamp()
        .sum(snapshot -> 1);

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()));

    // many timestamps
    SortedMap<OSHDBTimestamp, Number> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .sum(snapshot -> 1);

    assertEquals(72, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()));
    assertEquals(42, result2.get(result2.lastKey()));

    // total
    Number result3 = this.createMapReducer()
        .timestamps(timestamps1)
        .sum(snapshot -> 1);

    assertEquals(42, result3);

    // custom aggregation identifier
    SortedMap<Boolean, Number> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .sum(snapshot -> 1);

    assertEquals(21, result4.get(true));
    assertEquals(21, result4.get(false));
  }

  @Test
  void testCount() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Integer> result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .aggregateByTimestamp()
        .count();

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()).intValue());

    // many timestamps
    SortedMap<OSHDBTimestamp, Integer> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .count();

    assertEquals(72, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).intValue());
    assertEquals(42, result2.get(result2.lastKey()).intValue());

    // total
    Integer result3 = this.createMapReducer()
        .timestamps(timestamps1)
        .count();

    assertEquals(42, result3.intValue());

    // custom aggregation identifier
    SortedMap<Boolean, Integer> result4 = this.createMapReducer()
        .timestamps(timestamps1)
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
        .map(snapshot -> snapshot.getEntity().getId() % 2)
        .average();

    assertEquals(0.5, result1.doubleValue(), DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .map(snapshot -> snapshot.getEntity().getId() % 2)
        .average();

    assertEquals(72, result2.entrySet().size());
    assertEquals(Double.NaN, result2.get(result2.firstKey()), DELTA);
    assertEquals(0.5, result2.get(result2.lastKey()).doubleValue(), DELTA);

    // custom aggregation identifier
    SortedMap<Boolean, Double> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .average(snapshot -> snapshot.getEntity().getId() % 2);

    assertEquals(0.0, result4.get(true).doubleValue(), DELTA);
    assertEquals(1.0, result4.get(false).doubleValue(), DELTA);
  }

  @Test
  void testWeightedAverage() throws Exception {
    // single timestamp
    Double result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .weightedAverage(snapshot -> new WeightedValue(
            snapshot.getEntity().getId() % 2,
            1 * (snapshot.getEntity().getId() % 2)
        ));

    assertEquals(1.0, result1.doubleValue(), DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .weightedAverage(snapshot -> new WeightedValue(
            snapshot.getEntity().getId() % 2,
            2 * (snapshot.getEntity().getId() % 2)
        ));

    assertEquals(72, result2.entrySet().size());
    assertEquals(Double.NaN, result2.get(result2.firstKey()), DELTA);
    assertEquals(1.0, result2.get(result2.lastKey()).doubleValue(), DELTA);

    // custom aggregation identifier
    SortedMap<Boolean, Double> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .weightedAverage(snapshot -> new WeightedValue(
            snapshot.getEntity().getId() % 2,
            2 * (snapshot.getEntity().getId() % 2)
        ));

    assertEquals(Double.NaN, result4.get(true).doubleValue(), DELTA);
    assertEquals(1.0, result4.get(false).doubleValue(), DELTA);
  }

  @Test
  void testUniq() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Set<Long>> result1 = this.createMapReducer()
        .timestamps(timestamps1)
        .aggregateByTimestamp()
        .uniq(snapshot -> snapshot.getEntity().getId());

    assertEquals(1, result1.entrySet().size());
    assertEquals(42, result1.get(result1.firstKey()).size());

    // many timestamps
    SortedMap<OSHDBTimestamp, Set<Long>> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .uniq(snapshot -> snapshot.getEntity().getId());

    assertEquals(72, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).size());
    assertEquals(42, result2.get(result2.lastKey()).size());

    // total
    Set<Long> result3 = this.createMapReducer()
        .timestamps(timestamps1)
        .uniq(snapshot -> snapshot.getEntity().getId());

    assertEquals(42, result3.size());

    // custom aggregation identifier
    SortedMap<Boolean, Set<Long>> result4 = this.createMapReducer()
        .timestamps(timestamps1)
        .aggregateBy(snapshot -> snapshot.getEntity().getId() % 2 == 0)
        .uniq(snapshot -> snapshot.getEntity().getId());

    assertEquals(21, result4.get(true).size());
    assertEquals(21, result4.get(false).size());
  }

}
