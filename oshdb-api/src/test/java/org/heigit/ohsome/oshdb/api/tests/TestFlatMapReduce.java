package org.heigit.ohsome.oshdb.api.tests;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBH2;
import org.heigit.ohsome.oshdb.api.mapreducer.contribution.OSMContributionView;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.junit.jupiter.api.Test;

/**
 * Test flat map method of the MapReducer class of the OSHDB API.
 */
class TestFlatMapReduce {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = bboxWgs84Coordinates(8.0, 49.0, 9.0, 50.0);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  TestFlatMapReduce() throws Exception {
    oshdb = new OSHDBH2("../data/test-data");
  }

  private OSMContributionView createMapReducerOSMContribution() throws Exception {
    return new OSMContributionView(oshdb, null)
        .areaOfInterest(bbox)
        .filter("type:node and highway=*");
  }

  @Test
  void test() throws Exception {
    Set<Entry<Integer, Integer>> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .view()
        .flatMap(contribution -> Stream.of(contribution)
            .map(OSMContribution::getEntityAfter)
            .filter(entity -> entity.getId() == 617308093)
            .flatMap(entity -> entity.getTags().stream())
            .map(tag -> Map.entry(tag.getKey(), tag.getValue())))
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
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .view()
        .flatMap(contribution -> input.stream())
        .uniq();

    assertEquals(input, result);
  }
}
