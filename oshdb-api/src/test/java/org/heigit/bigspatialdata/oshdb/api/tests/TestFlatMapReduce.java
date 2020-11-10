package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;

/**
 * Test flat map method of the MapReducer class of the OSHDB API.
 */
public class TestFlatMapReduce {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01",
      OSHDBTimestamps.Interval.MONTHLY);

  public TestFlatMapReduce() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView
        .on(oshdb)
        .osmType(OSMType.NODE)
        .osmTag("highway")
        .areaOfInterest(bbox);
  }

  @Test
  public void test() throws Exception {
    Set<Entry<Integer, Integer>> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .flatMap(contribution -> {
          if (contribution.getEntityAfter().getId() != 617308093) {
            return new ArrayList<>();
          }
          List<Entry<Integer, Integer>> ret = new ArrayList<>();
          int[] tags = contribution.getEntityAfter().getRawTags();
          for (int i = 0; i < tags.length; i += 2) {
            ret.add(new SimpleImmutableEntry<>(tags[i], tags[i + 1]));
          }
          return ret;
        })
        .reduce(
            HashSet::new,
            (x,y) -> {
              x.add(y);
              return x;
            },
            (x,y) -> {
              HashSet<Entry<Integer, Integer>> ret = new HashSet<>(x);
              ret.addAll(y);
              return ret;
            }
        );

    assertEquals(2, result.size());
  }

  @Test
  public void testSet() throws Exception {
    Set<Integer> input = new TreeSet<>();
    input.add(1);
    input.add(2);
    input.add(3);
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .flatMap(contribution -> input)
        .uniq();

    assertEquals(input, result);
  }

  @Test
  public void testIterable() throws Exception {
    Set<Integer> input = new TreeSet<>();
    input.add(1);
    input.add(2);
    input.add(3);
    Set<Integer> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .flatMap(contribution -> input.stream()::iterator)
        .uniq();

    assertEquals(input, result);
  }
}
