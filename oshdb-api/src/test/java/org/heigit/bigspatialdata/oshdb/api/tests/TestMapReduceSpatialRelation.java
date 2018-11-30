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
  // Neighbourhood tests
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testNeighbourhoodForSnapshotAndNearbySnapshotsWithCallBackFunction() throws Exception {
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = createMapReducerOSMEntitySnapshotID()
        .getNeighbouringSnapshots(
            25.,
            mapReduce -> mapReduce.osmTag("highway").collect())
        .collect();
    assertEquals( 1, result.get(0).getValue().size());
  }

  @Test
  public void testNeighbourhoodForSnapshotAndNearbyContributionsWithKey() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMContribution>>> result = createMapReducerOSMEntitySnapshotID2()
        .getNeighbouringContributions(
            40.,
            MapReducer::collect)
        .collect();
    assertEquals( 5, result.get(0).getValue().size());
  }


  @Test
  public void testNeighbourhoodForSnapshotAndNearbyContributionsWithContributionType() throws Exception {
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMContribution>>> result = createMapReducerOSMEntitySnapshotID2()
        .getNeighbouringContributions(
            40.,
            MapReducer::collect)
        .collect();
    assertEquals( 3, result.get(0).getValue().size());
  }

  @Test
  public void testNeighbourhoodForSnapshotAndNearbySnapshotsWithoutCallBackFunction() throws Exception {
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = createMapReducerOSMEntitySnapshotID()
        .getNeighbouringSnapshots(25.)
        .collect();
    assertEquals(5, result.get(0).getRight().size());
  }

  @Test
  public void testNeighbourhoodForContributionAndNearbySnapshots() throws Exception {
    SortedMap<OSHDBTimestamp, List<Pair<OSMContribution, List<OSMEntitySnapshot>>>> result = createMapReducerOSMContribution()
        .getNeighbouringSnapshots(25.,
            mapReduce -> mapReduce.osmTag("highway").collect())
        .aggregateByTimestamp()
        .collect();
    System.out.println("done");
    //assertEquals( 1, result.get(0).getValue().size());
  }

  // -----------------------------------------------------------------------------------------------
  // Neighbouring tests
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testNeighbouringKeyForSnapshotAndNearbySnapshots() throws Exception {
    Number result = createMapReducerOSMEntitySnapshotID()
        .neighbouringSnapshots(25.,"highway")
        .count();
    assertEquals( 1, result);
  }


  @Test
  public void testNeighbouringKeyAndValueForSnapshotAndNearbyContributions() throws Exception {
    Number result = createMapReducerOSMEntitySnapshotID2()
        .neighbouringContributions(25.)
        .count();
    assertEquals( 1, result);
  }

  @Test
  public void testNeighbouringKeyForOSMContributionAndNearbySnapshots() throws Exception {
    // Create MapReducer
    Number result = createMapReducerOSMContribution()
        .neighbouringSnapshots(54., "highway")
        .count();
    assertEquals(1, result);
  }

  // -----------------------------------------------------------------------------------------------
  // getContainedSnapshots
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testContainedSnapshots() throws Exception {
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = createMapReducerOSMEntitySnapshot2017()
        .filter(x -> x.getEntity().getId() == 130522985)
        .getContainedSnapshots(mapReduce -> mapReduce.osmType(OSMType.NODE).collect())
        .collect();
    assertEquals(1437748618, result.get(0).getValue().get(0).getEntity().getId());
  }

  // -----------------------------------------------------------------------------------------------
  // getEnclosingSnapshots
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testEnclosingSnapshots() throws Exception {
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = createMapReducerOSMEntitySnapshot2017()
        .filter(x -> x.getEntity().getId() == 1437748618)
        .getEnclosingSnapshots(mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .collect();
    assertEquals( 130522985, result.get(0).getValue().get(0).getEntity().getId());
  }

  // -----------------------------------------------------------------------------------------------
  // Covering snapshots
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testNeighbouringSnapshots() throws Exception {
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = createMapReducerOSMEntitySnapshotID()
        .getNeighbouringSnapshots(
            25.,
            mapReduce -> mapReduce.osmTag("highway").collect())
        .collect();
    assertEquals( 1, result.get(0).getValue().size());
  }

  // -----------------------------------------------------------------------------------------------
  // Covering snapshots
  // -----------------------------------------------------------------------------------------------

  /*
  @Test
  public void testCoveringSnapshots() throws Exception {
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = createMapReducerOSMEntitySnapshot2017()
        .filter(x -> x.getEntity().getId() == 130522985)
        .getCoveringSnapshots(mapReduce -> mapReduce.osmType(OSMType.NODE).filter(x -> x.getEntity().getId() == 1437748618).collect())
        .collect();
    assertEquals(1437748618, result.get(0).getValue().get(0).getEntity().getId());
  }
  */

  // -----------------------------------------------------------------------------------------------
  // covered snapshots
  // -----------------------------------------------------------------------------------------------

  /*
  @Test
  public void testCoveredSnapshots() throws Exception {
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = createMapReducerOSMEntitySnapshot2017()
        .filter(x -> x.getEntity().getId() == 1437748618)
        .getCoveredSnapshots(mapReduce -> mapReduce.osmType(OSMType.WAY).collect())
        .collect();
    assertEquals( 130522985, result.get(0).getValue().get(0).getEntity().getId());
  }
  */

  // -----------------------------------------------------------------------------------------------
  // INSIDE
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testInsideMapForSnapshots() throws Exception {
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = createMapReducerOSMEntitySnapshotID()
        .getEnclosingSnapshots()
        .collect();
    System.out.println(result);
    assertEquals( 2, result.get(0).getValue().size());
  }

  @Test
  public void testInsideForSnapshotsWithKey() throws Exception {
    // Create MapReducer
    Number result = createMapReducerOSMEntitySnapshotID()
        .inside("building", "yes")
        .count();
    //todo improve test
    assertEquals( 0, result);
  }

  // -----------------------------------------------------------------------------------------------
  // OVERLAPS
  // -----------------------------------------------------------------------------------------------


  @Test
  public void testOverlappingLines() throws Exception {
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps3)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 26175276)
        .getOverlappingSnapshots(mapReduce -> mapReduce.collect())
        .collect();
    assertEquals( 1, result.get(0).getValue().size());
  }

  @Test
  public void testOverlapsLines() throws Exception {
    // Create MapReducer
    List<OSMEntitySnapshot> result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps3)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 26175276)
        .overlaps("amenity")
        .collect();
    assertEquals( 0, result.size());
  }

  @Test
  public void testOverlappingPolygon() throws Exception {
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps3)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 203266416)
        .getOverlappingSnapshots(mapReduce -> mapReduce.collect())
        .collect();
    assertEquals( 1, result.get(0).getValue().size());
  }

  @Test
  public void testOverlappingPolygonAgg() throws Exception {
    // Create MapReducer
    SortedMap<OSHDBTimestamp, List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>>> result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps3)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 203266416)
        .aggregateByTimestamp()
        .getTouchingSnapshots(mapReduce -> mapReduce.collect())
        .collect();
    //assertEquals( 1, result.get(0).getValue().size());
  }

  @Test
  public void contains() {
  }

  @Test
  public void containsContributions() {
  }

}