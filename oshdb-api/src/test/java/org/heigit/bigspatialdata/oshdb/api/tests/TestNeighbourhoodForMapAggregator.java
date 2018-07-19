package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.List;
import java.util.SortedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapAggregator;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps.Interval;
import org.junit.Test;

public class TestNeighbourhoodForMapAggregator {

  private final OSHDBJdbc oshdb;

  // create bounding box from coordinates
  //private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(85.3406012, 27.6991942, 85.3585444, 27.7121143);
  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01", "2014-12-31", Interval.YEARLY);
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2015-12-31", Interval.YEARLY);
  private final OSHDBTimestamps timestamps3 = new OSHDBTimestamps("2015-01-01", "2015-12-31", Interval.MONTHLY);

  public TestNeighbourhoodForMapAggregator() throws Exception {
    oshdb = new OSHDBH2("./src/test/resources/test-data").multithreading(true);
  }

  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() {
    return OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2)
        .areaOfInterest(bbox)
        .osmTag("building");
  }

  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshotID() {
    return OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps3)
        .areaOfInterest(bbox)
        .filter(x -> x.getEntity().getId() == 172510827);
  }

  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshotID2() {
    return OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps1)
        .areaOfInterest(bbox)
        .filter(x -> x.getEntity().getId() == 172510827);
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() {
    return OSMContributionView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2)
        .areaOfInterest(bbox)
        .filter(x -> x.getEntityAfter().getId() == 172510827);
  }

  @Test
  public void test_neighbourhood_snapshot_snapshot_callback_aggregator() throws Exception {
    // Create MapReducer
    SortedMap<OSHDBTimestamp, List<Pair<OSMEntitySnapshot, List<Object>>>> result1 = createMapReducerOSMEntitySnapshotID()
        .aggregateByTimestamp()
        .neighbourhood(
            250.,
            mapReduce -> mapReduce.osmTag("highway").collect())
        .collect();

    SortedMap<OSHDBTimestamp, List<Pair<OSMEntitySnapshot, List<Object>>>> result2 = createMapReducerOSMEntitySnapshotID()
        .neighbourhood(
            250.,
            mapReduce -> mapReduce.osmTag("highway").collect())
        .aggregateByTimestamp()
        .collect();

    for ( OSHDBTimestamp key : result1.keySet()) {
      assertEquals(result1.get(key).get(0).getRight().size(), result2.get(key).get(0).getRight().size());
      assertEquals(result1.get(key).size(), result2.get(key).size());
      assertEquals(result1.get(key).get(0).getLeft().getEntity().getId(), result2.get(key).get(0).getLeft().getEntity().getId());
      assertEquals(result1.get(key).get(0).getRight().size(), result1.get(key).get(0).getRight().size());
      //System.out.println(result1.get(key).get(0).getRight().size());
    }
  }

  @Test
  public void test_neighbouring_snapshot_snapshot_callback_aggregator() throws Exception {
    // Create MapReducer
    SortedMap<OSHDBTimestamp, List<OSMEntitySnapshot>> result1 = createMapReducerOSMEntitySnapshotID()
        .aggregateByTimestamp()
        .neighbouring(
            25.,
            mapReduce -> mapReduce.osmTag("highway").count() > 0)
        .collect();

    SortedMap<OSHDBTimestamp, List<OSMEntitySnapshot>> result2 = createMapReducerOSMEntitySnapshotID()
        .neighbouring(
            25.,
            mapReduce -> mapReduce.osmTag("highway").count() > 0)
        .aggregateByTimestamp()
        .collect();

    for ( OSHDBTimestamp key : result1.keySet()) {
      assertEquals(result1.get(key).size(), result2.get(key).size());
    }
  }
}
