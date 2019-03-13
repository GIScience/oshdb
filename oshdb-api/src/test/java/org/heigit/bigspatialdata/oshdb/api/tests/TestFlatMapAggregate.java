package org.heigit.bigspatialdata.oshdb.api.tests;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestFlatMapAggregate {
  private final OSHDBDatabase oshdb;

  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
  private final OSHDBTimestamps timestamps72 = new OSHDBTimestamps("2010-01-01", "2015-12-01", OSHDBTimestamps.Interval.MONTHLY);

  private final double DELTA = 1e-8;

  public TestFlatMapAggregate() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data");
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
    return OSMContributionView.on(oshdb).osmType(OSMType.NODE).osmTag("highway").areaOfInterest(bbox);
  }

  @Test
  public void test() throws Exception {
    SortedMap<Long, Set<Entry<Integer, Integer>>> result = createMapReducerOSMContribution()
        .timestamps(timestamps72)
        .flatMap(
            contribution -> {
              if (contribution.getEntityAfter().getId() != 617308093)
                return new ArrayList<>();
              List<Entry<Long, Entry<Integer, Integer>>> ret = new ArrayList<>();
              int[] tags = contribution.getEntityAfter().getRawTags();
              for (int i=0; i<tags.length; i+=2)
                ret.add(new SimpleImmutableEntry<>(
                    contribution.getEntityAfter().getId(),
                    new SimpleImmutableEntry<>(tags[i], tags[i+1])
                ));
              return ret;
            }
        )
        .aggregateBy(Entry::getKey)
        .map(Entry::getValue)
        .reduce(
            HashSet::new,
            (x,y) -> { x.add(y); return x; },
            (x,y) -> { Set<Entry<Integer, Integer>> ret = new HashSet<>(x); ret.addAll(y); return ret; }
        );

    assertEquals(1, result.entrySet().size());
    assertEquals(2, result.get(617308093L).size());
  }
}
