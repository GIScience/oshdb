package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.*;

import java.util.List;
import java.util.SortedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunctionWithException;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps.Interval;
import org.junit.Test;

public class TestMapReduceSpatialRelation {

  private final OSHDBJdbc oshdb;

  // create bounding box from coordinates
  //private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(85.3406012, 27.6991942, 85.3585444, 27.7121143);
  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01", "2014-12-31", Interval.YEARLY);
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2015-12-31", Interval.MONTHLY);
  private final OSHDBTimestamps timestamps3 = new OSHDBTimestamps("2015-01-01", "2015-12-31", Interval.MONTHLY);
  private final OSHDBTimestamps timestamps2017 = new OSHDBTimestamps("2017-01-01");

  public TestMapReduceSpatialRelation() throws Exception {
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

  private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot2017() {
    return OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox);
  }

  private MapReducer<OSMContribution> createMapReducerOSMContribution() {
    return OSMContributionView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2)
        .areaOfInterest(bbox)
        .filter(x -> x.getEntityAfter().getId() == 172510827);
  }

  // -----------------------------------------------------------------------------------------------
  // Test inside and contains
  // -----------------------------------------------------------------------------------------------

  @Test
  public void test_contains_inside() throws Exception {

    Integer result_contains = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .osmTag("building")
        .containment(
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getRight())
        .count();

    Integer result_inside = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.NODE)
        .inside(
            mapReduce -> mapReduce.osmType(OSMType.WAY).osmTag("building").collect())
        .count();

    assertEquals( result_inside, result_contains);

    System.out.println("Count: " + result_contains);
  }

  @Test
  public void test_neighbouring() throws Exception {

    Integer result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps2017)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 130530843)
        .neighbourhood(12.,
            mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .flatMap(x -> x.getRight())
        .count();

    assertEquals(4, result.intValue());
  }

}