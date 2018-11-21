package org.heigit.bigspatialdata.oshdb.api.tests;

import java.util.SortedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
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

import static org.junit.Assert.assertEquals;

import java.util.List;

public class TestNeighbourhood {
  private final OSHDBJdbc oshdb;

  // create bounding box from coordinates
  //private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(85.3406012, 27.6991942, 85.3585444, 27.7121143);
  private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
  private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2014-01-01", "2014-12-31", Interval.YEARLY);
  private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2014-01-01", "2015-12-31", Interval.YEARLY);
  private final OSHDBTimestamps timestamps3 = new OSHDBTimestamps("2015-01-01", "2015-12-31", Interval.YEARLY);

  public TestNeighbourhood() throws Exception {
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

  // -----------------------------------------------------------------------------------------------
  // Neighbourhood tests
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testNeighbourhoodForSnapshotAndNearbySnapshotsWithCallBackFunction() throws Exception {
    long startTime = System.nanoTime();
    List<Pair<OSMEntitySnapshot, List<Object>>> result = createMapReducerOSMEntitySnapshotID()
        .neighbourhood(
            25.,
            mapReduce -> mapReduce.osmTag("highway").collect())
        .collect();
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 1, result.get(0).getValue().size());
  }

  @Test
  public void testNeighbourhoodForSnapshotAndNearbyContributionsWithKey() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<Object>>> result = createMapReducerOSMEntitySnapshotID2()
        .neighbourhood(
            40.,
            MapReducer::collect,
            true,
            null)
        .collect();
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 5, result.get(0).getValue().size());
  }

  @Test
  public void testNeighbourhoodForSnapshotAndNearbyContributionsWithContributionType() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<Object>>> result = createMapReducerOSMEntitySnapshotID2()
        .neighbourhood(
            40.,
            MapReducer::collect,
            true,
            ContributionType.TAG_CHANGE)
        .collect();
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 3, result.get(0).getValue().size());
  }

  @Test
  public void testNeighbourhoodForSnapshotAndNearbySnapshotsWithoutCallBackFunction() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<Object>>> result = createMapReducerOSMEntitySnapshotID()
        .neighbourhood(25.)
        .collect();
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals(5, result.get(0).getRight().size());
  }

  @Test
  public void testNeighbourhoodForContributionAndNearbySnapshots() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    List<Pair<OSMContribution, List<Object>>> result = createMapReducerOSMContribution()
        .neighbourhood(25.,
            mapReduce -> mapReduce.osmTag("highway").collect())
        .collect();
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 1, result.get(0).getValue().size());
  }

  // -----------------------------------------------------------------------------------------------
  // Neighbouring tests
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testNeighbouringKeyForSnapshotAndNearbySnapshots() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    Number result = createMapReducerOSMEntitySnapshotID()
        .neighbouring(25.,"highway")
        .count();
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 1, result);
  }


  @Test
  public void testNeighbouringKeyAndValueForSnapshotAndNearbyContributions() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    Number result = createMapReducerOSMEntitySnapshotID2()
            .neighbouring(25., mapReduce -> mapReduce.count() > 0, true, null)
            .count();
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 1, result);
  }

  @Test
  public void testNeighbouringKeyForOSMContributionAndNearbySnapshots() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    Number result = createMapReducerOSMContribution()
        .neighbouring(54., "highway")
        .count();
    // Calculate execution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals(1, result);
  }

  // -----------------------------------------------------------------------------------------------
  // INSIDE
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testInsideMapForSnapshots() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = createMapReducerOSMEntitySnapshotID()
        .getEnclosingSnapshots()
        .collect();
    System.out.println(result);
    // Calculate execution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 2, result.get(0).getValue().size());
  }

  @Test
  public void testInsideForSnapshotsWithKey() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    Number result = createMapReducerOSMEntitySnapshotID()
        .inside("building", "yes")
        .count();
    //todo improve test
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 0, result);
  }

  // -----------------------------------------------------------------------------------------------
  // OVERLAPS
  // -----------------------------------------------------------------------------------------------


  @Test
  public void testOverlappingLines() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps3)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 26175276)
        .getOverlappingSnapshots(mapReduce -> mapReduce.collect())
        .collect();
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 1, result.get(0).getValue().size());
  }

  @Test
  public void testOverlapsLines() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    List<OSMEntitySnapshot> result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps3)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 26175276)
        .overlaps("amenity")
        .collect();
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 0, result.size());
  }

  @Test
  public void testOverlappingPolygon() throws Exception {
    long startTime = System.nanoTime();
    // Create MapReducer
    List<Pair<OSMEntitySnapshot, List<OSMEntitySnapshot>>> result = OSMEntitySnapshotView.on(oshdb)
        .keytables(oshdb)
        .timestamps(timestamps3)
        .areaOfInterest(bbox)
        .osmType(OSMType.WAY)
        .filter(x -> x.getEntity().getId() == 203266416)
        .getOverlappingSnapshots(mapReduce -> mapReduce.collect())
        .collect();
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    assertEquals( 1, result.get(0).getValue().size());
  }

  @Test
  public void testOverlappingPolygonAgg() throws Exception {
    long startTime = System.nanoTime();
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
    // Calculate xecution time
    long endTime = System.nanoTime();
    double duration = (endTime - startTime) / 1000000.;
    System.out.format("Execution time: %.3f sec", duration / 1000.);
    // Check result
    //assertEquals( 1, result.get(0).getValue().size());
  }


}