package org.heigit.ohsome.oshdb.api.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.generic.WeightedValue;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Agg;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSHDBView;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSMContributionView;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test special reducers of the OSHDB API when using the contribution view.
 */
class TestHelpersOSMContributionView {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox =
      OSHDBBoundingBox.bboxWgs84Coordinates(8.651133, 49.387611, 8.6561, 49.390513);
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2015-01-01");
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  private static final double DELTA = 1e-8;

  TestHelpersOSMContributionView() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private OSHDBView<OSMContribution> createMapReducer() throws Exception {
    return OSMContributionView.view()
        .areaOfInterest(bbox)
        .filter("type:way and building=yes");
  }

  @Test
  void testSum() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Long> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(contribution -> contribution
            .getContributionTypes()
            .contains(ContributionType.TAG_CHANGE)
            ? 1 : 0)
        .aggregate(Agg::sumInt);


    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()));

    // many timestamps
    SortedMap<OSHDBTimestamp, Long> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(contribution ->
            contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0)
        .aggregate(Agg::sumInt);

    assertEquals(71, result2.entrySet().size());
    assertEquals(42, result2
        .values()
        .stream()
        .reduce(0L, Long::sum));

    // total
    Number result3 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .map(contribution ->
            contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0)
        .aggregate(Agg::sumInt);

    assertEquals(42, result3.intValue());

    // custom aggregation identifier
    SortedMap<String, Long> result4 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateBy(contribution -> contribution.getContributionTypes().toString())
        .map(contribution -> 1)
        .aggregate(Agg::sumInt);

    assertEquals(42, result4.get(EnumSet.of(ContributionType.CREATION).toString()));
    assertEquals(null, result4.get(EnumSet.of(ContributionType.DELETION).toString()));
  }

  @Test
  void testCount() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Long> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .on(oshdb)
        .aggregateByTimestamp()
        .count();

    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()).intValue());

    // many timestamps
    SortedMap<OSHDBTimestamp, Long> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .count();

    assertEquals(71, result2.entrySet().size());
    assertEquals(0, result2.get(result2.firstKey()).intValue());
    assertEquals(0, result2.get(result2.lastKey()).intValue());

    // total
    Long result3 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .count();

    assertEquals(70, result3.intValue());

    // custom aggregation identifier
    SortedMap<Boolean, Long> result4 = this.createMapReducer()
        .timestamps(timestamps2)
        .on(oshdb)
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
        .on(oshdb)
        .map(contribution ->
            contribution.getContributionTypes().contains(ContributionType.TAG_CHANGE) ? 1 : 0)
        .aggregate(Agg::average);

    assertEquals(1.0, result1.doubleValue(), DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(contribution ->
            contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0)
        .aggregate(Agg::average);

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
        .on(oshdb)
        .aggregateBy(contribution -> contribution
            .getContributionTypes()
            .contains(ContributionType.CREATION))
        .map(contribution -> contribution.getEntityAfter().getId() % 2)
        .aggregate(Agg::average);

    assertEquals(0.5, result4.get(true).doubleValue(), DELTA);
  }

  @Test
  void testWeightedAverage() throws Exception {
    // single timestamp
    Double result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .on(oshdb)
        .map(contribution -> new WeightedValue(
            contribution.getContributionTypes().contains(ContributionType.TAG_CHANGE) ? 1 : 0,
            2 * (contribution.getEntityAfter().getId() % 2)
        ))
        .aggregate(Agg::weightedAverage);

    assertEquals(1.0, result1.doubleValue(), DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(contribution -> new WeightedValue(
            contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0,
            2 * (contribution.getEntityAfter().getId() % 2)
        ))
        .aggregate(Agg::weightedAverage);

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
        .on(oshdb)
        .aggregateBy(contribution ->
            contribution.getContributionTypes().contains(ContributionType.CREATION))
        .map(contribution -> new WeightedValue(
            contribution.getEntityAfter().getId() % 2,
            2 * (contribution.getEntityAfter().getId() % 2)
        ))
        .aggregate(Agg::weightedAverage);

    assertEquals(1.0, result4.get(true).doubleValue(), DELTA);
  }

  @Test
  void testUniq() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Set<Long>> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(contribution -> contribution.getEntityAfter().getId())
        .aggregate(Agg::uniq);

    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()).size());

    // many timestamps
    SortedMap<OSHDBTimestamp, Set<Long>> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateByTimestamp()
        .map(contribution -> contribution.getEntityAfter().getId())
        .aggregate(Agg::uniq);

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
        .on(oshdb)
        .map(contribution -> contribution.getEntityAfter().getId())
        .aggregate(Agg::uniq);

    assertEquals(42, result3.size());

    // custom aggregation identifier
    SortedMap<Boolean, Set<Long>> result4 = this.createMapReducer()
        .timestamps(timestamps72)
        .on(oshdb)
        .aggregateBy(contribution -> contribution.getEntityAfter().getId() % 2 == 0)
        .map(contribution -> contribution.getEntityAfter().getId())
        .aggregate(Agg::uniq);

    assertEquals(21, result4.get(true).size());
    assertEquals(21, result4.get(false).size());

    // doesn't crash with null pointers
    Set<Object> result5 = this.createMapReducer()
        .timestamps(timestamps2)
        .on(oshdb)
        .map(x -> null)
        .aggregate(Agg::uniq);
    assertEquals(result5.size(), 1);
  }

}
