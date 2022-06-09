package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.Set;
import java.util.TreeSet;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducer;
import org.heigit.ohsome.oshdb.api.mapreducer.aggregation.Agg;
import org.heigit.ohsome.oshdb.api.mapreducer.view.OSMContributionView;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test flat map method of the MapReducer class of the OSHDB API.
 */
class TestFlatMapReduce {
  private final OSHDBH2 oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestFlatMapReduce() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution(OSHDBTimestamps timestamps)
      throws Exception {
    return OSMContributionView.on(oshdb)
        .areaOfInterest(bbox)
        .filter("type:node and highway=*")
        .timestamps(timestamps)
        .view();
  }

  @Test
  void test() throws Exception {
    Set<Entry<Integer, Integer>> result = createMapReducerOSMContribution(timestamps72)
        .flatMap(contribution -> {
          if (contribution.getEntityAfter().getId() != 617308093) {
            return Stream.empty();
          }
          List<Entry<Integer, Integer>> ret = new ArrayList<>();
          for (OSHDBTag tag : contribution.getEntityAfter().getTags()) {
            ret.add(new SimpleImmutableEntry<>(tag.getKey(), tag.getValue()));
          }
          return ret.stream();
        })
        .reduce(
            HashSet::new,
            (x, y) -> {
              x.add(y);
              return x;
            },
            (x, y) -> {
              HashSet<Entry<Integer, Integer>> ret = new HashSet<>(x);
              ret.addAll(y);
              return ret;
            }
        );

    assertEquals(2, result.size());
  }

  @Test
  void testSet() throws Exception {
    Set<Integer> input = new TreeSet<>();
    input.add(1);
    input.add(2);
    input.add(3);
    Set<Integer> result = createMapReducerOSMContribution(timestamps72)
        .flatMap(contribution -> input.stream())
        .aggregate(Agg::uniq);

    assertEquals(input, result);
  }

  @Test
  void testIterable() throws Exception {
    Set<Integer> input = new TreeSet<>();
    input.add(1);
    input.add(2);
    input.add(3);
    Set<Integer> result = createMapReducerOSMContribution(timestamps72)
        .flatMap(contribution -> input.stream())
        .aggregate(Agg::uniq);

    assertEquals(input, result);
  }
}
