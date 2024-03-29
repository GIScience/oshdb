package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.generic.WeightedValue;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test special reducers of the OSHDB API when using the contribution view.
 */
class HelpersOSMContributionViewTest {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2015-01-01");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  private static final double DELTA = 1e-8;

  HelpersOSMContributionViewTest() {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private MapReducer<OSMContribution> createMapReducer() {
    return OSMContributionView
        .on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  @Test
  void testSum() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Number> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .aggregateByTimestamp()
        .sum(contribution -> contribution
            .getContributionTypes()
            .contains(ContributionType.TAG_CHANGE)
            ? 1 : 0);

    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()));

    // many timestamps
    SortedMap<OSHDBTimestamp, Number> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .sum(contribution ->
            contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0);

    assertEquals(71, result2.entrySet().size());
    assertEquals(42, result2
        .values()
        .stream()
        .reduce(0, (acc, num) -> acc.intValue() + num.intValue()));

    // total
    Number result3 = this.createMapReducer()
        .timestamps(timestamps72)
        .sum(contribution ->
            contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0);

    assertEquals(42, result3);

    // custom aggregation identifier
    SortedMap<String, Number> result4 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateBy(contribution -> contribution.getContributionTypes().toString())
        .sum(contribution -> 1);

    assertEquals(42, result4.get(EnumSet.of(ContributionType.CREATION).toString()));
    assertNull(result4.get(EnumSet.of(ContributionType.DELETION).toString()));
  }

  @Test
  void testCount() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Integer> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .aggregateByTimestamp()
        .count();

    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()).intValue());

    // many timestamps
    SortedMap<OSHDBTimestamp, Integer> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .count();

    assertEquals(71, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).intValue());
    assertEquals(0, result2.get(result2.lastKey()).intValue());

    // total
    Integer result3 = this.createMapReducer()
        .timestamps(timestamps72)
        .count();

    assertEquals(70, result3.intValue());

    // custom aggregation identifier
    SortedMap<Boolean, Integer> result4 = this.createMapReducer()
        .timestamps(timestamps2)
        .aggregateBy(contribution -> contribution.getEntityAfter().getId() % 2 == 0)
        .count();

    assertEquals(4, result4.get(true).intValue());
    assertEquals(10, result4.get(false).intValue());
  }

  @Test
  void testAverage() throws Exception {
    // single timestamp
    Double result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .map(contribution ->
            contribution.getContributionTypes().contains(ContributionType.TAG_CHANGE) ? 1 : 0)
        .average();

    assertEquals(1.0, result1, DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .map(contribution ->
            contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0)
        .average();

    assertEquals(71, result2.entrySet().size());
    assertEquals(Double.NaN, result2.get(result2.firstKey()), DELTA);
    assertEquals(3, result2
        .entrySet()
        .stream()
        .filter(data -> !data.getValue().isNaN() && data.getValue() > 0)
        .count());

    // custom aggregation identifier
    SortedMap<Boolean, Double> result4 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateBy(contribution -> contribution
            .getContributionTypes()
            .contains(ContributionType.CREATION))
        .average(contribution -> contribution.getEntityAfter().getId() % 2);

    assertEquals(0.5, result4.get(true), DELTA);
  }

  @Test
  void testWeightedAverage() throws Exception {
    // single timestamp
    Double result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .weightedAverage(contribution -> new WeightedValue(
            contribution.getContributionTypes().contains(ContributionType.TAG_CHANGE) ? 1 : 0,
            2 * (contribution.getEntityAfter().getId() % 2)
        ));

    assertEquals(1.0, result1, DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .weightedAverage(contribution -> new WeightedValue(
            contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0,
            2 * (contribution.getEntityAfter().getId() % 2)
        ));

    assertEquals(71, result2.entrySet().size());
    assertEquals(Double.NaN, result2.get(result2.firstKey()), DELTA);
    assertEquals(3, result2
        .entrySet()
        .stream()
        .filter(data -> !data.getValue().isNaN() && data.getValue() > 0)
        .count());

    // custom aggregation identifier
    SortedMap<Boolean, Double> result4 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateBy(contribution ->
            contribution.getContributionTypes().contains(ContributionType.CREATION))
        .weightedAverage(contribution -> new WeightedValue(
            contribution.getEntityAfter().getId() % 2,
            2 * (contribution.getEntityAfter().getId() % 2)
        ));

    assertEquals(1.0, result4.get(true), DELTA);
  }

  @Test
  void testUniq() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Set<Long>> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .aggregateByTimestamp()
        .uniq(contribution -> contribution.getEntityAfter().getId());

    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()).size());

    // many timestamps
    SortedMap<OSHDBTimestamp, Set<Long>> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateByTimestamp()
        .uniq(contribution -> contribution.getEntityAfter().getId());

    assertEquals(71, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).size());
    assertEquals(42, result2.values().stream().reduce(new HashSet<>(), (acc, cur) -> {
      acc = new HashSet<>(acc);
      acc.addAll(cur);
      return acc;
    }).size());

    // total
    Set<Long> result3 = this.createMapReducer()
        .timestamps(timestamps72)
        .uniq(contribution -> contribution.getEntityAfter().getId());

    assertEquals(42, result3.size());

    // custom aggregation identifier
    SortedMap<Boolean, Set<Long>> result4 = this.createMapReducer()
        .timestamps(timestamps72)
        .aggregateBy(contribution -> contribution.getEntityAfter().getId() % 2 == 0)
        .uniq(contribution -> contribution.getEntityAfter().getId());

    assertEquals(21, result4.get(true).size());
    assertEquals(21, result4.get(false).size());
  }

}
