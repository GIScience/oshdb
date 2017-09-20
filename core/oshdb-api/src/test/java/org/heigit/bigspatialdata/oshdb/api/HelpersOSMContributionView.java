/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.oshdb.api;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.ContributionType;
import org.junit.Test;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class HelpersOSMContributionView {
  private final OSHDB oshdb;

  private final BoundingBox bbox = new BoundingBox(8.651133,8.6561,49.387611,49.390513);
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps(2014, 2015, 1, 1);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps(2010, 2015, 1, 12);

  private final double DELTA = 1e-8;

  public HelpersOSMContributionView() throws Exception {
    oshdb = new OSHDB_H2("./src/test/resources/test-data;ACCESS_MODE_DATA=r");
  }

  private MapReducer<OSMContribution> createMapReducer() throws Exception {
    return OSMContributionView.on(oshdb).osmTypes(OSMType.WAY).filterByTagValue("building", "yes").areaOfInterest(bbox);
  }

  @Test
  public void testSum() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Number> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .sumAggregateByTimestamp(contribution -> contribution.getContributionTypes().contains(ContributionType.TAG_CHANGE) ? 1 : 0);

    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()));

    // many timestamps
    SortedMap<OSHDBTimestamp, Number> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .sumAggregateByTimestamp(contribution -> contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0);

    assertEquals(71, result2.entrySet().size());
    assertEquals(42, result2.values().stream().reduce(0, (acc, num) -> acc.intValue()+num.intValue()));

    // total
    Number result3 = this.createMapReducer()
        .timestamps(timestamps72)
        .sum(contribution -> contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0);

    assertEquals(42, result3);

    // custom aggregation identifier
    SortedMap<String, Number> result4 = this.createMapReducer()
        .timestamps(timestamps72)
        .osmTypes(OSMType.WAY)
        .filterByTagValue("building", "yes")
        .sumAggregate(contribution -> new ImmutablePair<>(contribution.getContributionTypes().toString(), 1));

    assertEquals(42, result4.get(EnumSet.of(ContributionType.CREATION).toString()));
    assertEquals(null, result4.get(EnumSet.of(ContributionType.DELETION).toString()));
  }

  @Test
  public void testCount() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Integer> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .countAggregateByTimestamp();

    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()).intValue());

    // many timestamps
    SortedMap<OSHDBTimestamp, Integer> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .countAggregateByTimestamp();

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
        .countAggregate(contribution -> contribution.getEntityAfter().getId() % 2 == 0);

    assertEquals(4, result4.get(true).intValue());
    assertEquals(10, result4.get(false).intValue());
  }

  @Test
  public void testAverage() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Double> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .averageAggregateByTimestamp(contribution -> contribution.getContributionTypes().contains(ContributionType.TAG_CHANGE) ? 1 : 0);

    assertEquals(1, result1.entrySet().size());
    assertEquals(1.0, result1.get(result1.firstKey()).doubleValue(), DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .averageAggregateByTimestamp(contribution -> contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0);

    assertEquals(71, result2.entrySet().size());
    assertEquals(Double.NaN, result2.get(result2.firstKey()), DELTA);
    assertEquals(3, result2.entrySet().stream().filter(data -> !data.getValue().isNaN() && data.getValue() > 0).count());

    // custom aggregation identifier
    SortedMap<Boolean, Double> result4 = this.createMapReducer()
        .timestamps(timestamps72)
        .averageAggregate(contribution -> new ImmutablePair<>(contribution.getContributionTypes().contains(ContributionType.CREATION), contribution.getEntityAfter().getId() % 2));

    assertEquals(0.5, result4.get(true).doubleValue(), DELTA);
  }

  @Test
  public void testWeightedAverage() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Double> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .weightedAverageAggregateByTimestamp(contribution -> new MapReducer.WeightedValue<>(contribution.getContributionTypes().contains(ContributionType.TAG_CHANGE) ? 1 : 0,contribution.getEntityAfter().getId() % 2));

    assertEquals(1, result1.entrySet().size());
    assertEquals(1.4, result1.get(result1.firstKey()).doubleValue(), DELTA);

    // many timestamps
    SortedMap<OSHDBTimestamp, Double> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .weightedAverageAggregateByTimestamp(contribution -> new MapReducer.WeightedValue<>(contribution.getContributionTypes().contains(ContributionType.CREATION) ? 1 : 0,contribution.getEntityAfter().getId() % 2));

    assertEquals(71, result2.entrySet().size());
    assertEquals(Double.NaN, result2.get(result2.firstKey()), DELTA);
    assertEquals(3, result2.entrySet().stream().filter(data -> !data.getValue().isNaN() && data.getValue() > 0).count());

    // custom aggregation identifier
    SortedMap<Boolean, Double> result4 = this.createMapReducer()
        .timestamps(timestamps72)
        .weightedAverageAggregate(contribution -> new ImmutablePair<>(contribution.getContributionTypes().contains(ContributionType.CREATION), new MapReducer.WeightedValue<>(contribution.getEntityAfter().getId() % 2, contribution.getEntityAfter().getId() % 2)));

    assertEquals(1.0, result4.get(true).doubleValue(), DELTA);
  }

  @Test
  public void testUniq() throws Exception {
    // single timestamp
    SortedMap<OSHDBTimestamp, Set<Long>> result1 = this.createMapReducer()
        .timestamps(timestamps2)
        .uniqAggregateByTimestamp(contribution -> contribution.getEntityAfter().getId());

    assertEquals(1, result1.entrySet().size());
    assertEquals(14, result1.get(result1.firstKey()).size());

    // many timestamps
    SortedMap<OSHDBTimestamp, Set<Long>> result2 = this.createMapReducer()
        .timestamps(timestamps72)
        .uniqAggregateByTimestamp(contribution -> contribution.getEntityAfter().getId());

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
        .uniqAggregate(contribution -> new ImmutablePair<>(contribution.getEntityAfter().getId() % 2 == 0, contribution.getEntityAfter().getId()));

    assertEquals(21, result4.get(true).size());
    assertEquals(21, result4.get(false).size());
  }

}
