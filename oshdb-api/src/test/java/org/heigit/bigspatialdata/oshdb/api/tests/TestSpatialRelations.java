package org.heigit.bigspatialdata.oshdb.api.tests;

import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.SpatialRelations.GEOMETRY_OPTIONS;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps.Interval;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.List;

public class TestSpatialRelations {
    private final OSHDBJdbc oshdb;

    // create bounding box from coordinates
    //private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(85.3406012, 27.6991942, 85.3585444, 27.7121143);
    private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
    private final OSHDBTimestamps timestamps1 = new OSHDBTimestamps("2010-01-01", "2014-01-02", Interval.YEARLY);
    private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2013-01-01", "2014-01-02", Interval.YEARLY);

    public TestSpatialRelations() throws Exception {
        //oshdb = (new OSHDBH2("/Users/chludwig/Documents/oshdb/nepal_20180201_z12_keytables.compressed.oshdb")).multithreading(true);
        oshdb = new OSHDBH2("./src/test/resources/test-data").multithreading(true);
    }

    private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() {
        return OSMEntitySnapshotView.on(oshdb)
                .keytables(oshdb)
                .timestamps(timestamps2)
                .areaOfInterest(bbox)
                //.osmTag("leisure", "park");
                .osmTag("building");
    }

    private MapReducer<OSMContribution> createMapReducerOSMContribution() {
        return OSMContributionView.on(oshdb)
                .keytables(oshdb)
                .timestamps(timestamps1)
                .areaOfInterest(bbox)
                .osmTag("building");
    }

    @Test
    public void testNeighbourhoodFilterKeyForSnapshotAndNearbySnapshots() throws Exception {

        // Create MapReducer
        Number result = createMapReducerOSMEntitySnapshot()
                .neighbourhoodFilter(54., "highway")
                //.neighbouring(54., "amenity")
                .count();
        //assertEquals( 2, result);
        assertEquals( 148, result);
    }

    @Test
    public void testNeighbourhoodFilterKeyForSnapshotAndNearbyContributions() throws Exception {

        // Create MapReducer
        Number result = createMapReducerOSMEntitySnapshot()
                .neighbourhoodFilter(54., "highway", true)
                //.neighbouring(54., "amenity")
                .count();
        //assertEquals( 2, result);
        assertEquals( 6, result);
    }

    @Test
    public void testNeighbourhoodFilterKeyAndValueForSnapshotAndNearbySnapshots() throws Exception {

        // Create MapReducer
        Number result = createMapReducerOSMEntitySnapshot()
                .neighbourhoodFilter(54., "highway", "primary")
                //.neighbouring(54., "amenity", "post_box")
                .count();
        //assertEquals( 1, result);
        assertEquals( 2, result);
    }

    @Test
    public void testNeighbourhoodFilterKeyAndValueForSnapshotAndNearbyContributions() throws Exception {

        // Create MapReducer
        Number result = createMapReducerOSMEntitySnapshot()
                .neighbourhoodFilter(54., "highway", "primary", true)
                //.neighbouring(54., "amenity", "post_box")
                .count();
        //assertEquals( 1, result);
        assertEquals( 1, result);
    }

    @Test
    public void testNeighbourhoodFilterCallbackForSnapshotAndNearbySnapshots() throws Exception {

        // Create MapReducer
        Number result = createMapReducerOSMEntitySnapshot()
                .neighbourhoodFilter(54., mapReduce -> mapReduce.osmTag("highway", "primary").count() > 0)
                //.neighbouring(54., mapReduce -> mapReduce.osmTag("amenity", "post_box").count() > 0)
                .count();
        //assertEquals( 1, result);
        assertEquals( 2, result);
    }


    @Test
    public void testNeighbourhoodFilterCallbackForSnapshotAndNearbyContributions() throws Exception {

        // Create MapReducer
        Number result = createMapReducerOSMEntitySnapshot()
                .neighbourhoodFilter(54.,
                        mapReduce -> mapReduce.osmTag("highway", "primary").count() > 0, true)
                //.neighbouring(54., mapReduce -> mapReduce.osmTag("amenity", "post_box").count() > 0)
                .count();
        //assertEquals( 1, result);
        assertEquals( 1, result);
    }


    @Test
    public void testNeighbourhoodMapForSnapshotAndNearbySnapshots() throws Exception {

        // Create MapReducer
        List<Pair<OSHDBMapReducible, List>> result = createMapReducerOSMEntitySnapshot()
                .neighbourhoodMap(54.,
                        mapReduce -> mapReduce.osmTag("highway", "primary").collect(),
                        false)
                //.neighbourhood(54., mapReduce -> mapReduce.osmTag("amenity", "post_box").collect())
                .collect();
        //assertEquals( 1, result.get(0).getRight().size());
        //assertEquals( 0, result.get(1).getRight().size());
        assertEquals( 2, result.get(0).getRight().size());
        assertEquals( 4, result.get(1).getRight().size());
    }

    @Test
    public void testNeighbourhoodMapForSnapshotAndNearbyContributions() throws Exception {

        // Create MapReducer
        List<Pair<OSHDBMapReducible, List>> result = createMapReducerOSMEntitySnapshot()
                .neighbourhoodMap(
                    54.,
                    mapReduce -> mapReduce.osmTag("highway", "primary").collect(),
                    true,
                    ContributionType.CREATION,
                    GEOMETRY_OPTIONS.BOTH)
                //.neighbourhood(54., mapReduce -> mapReduce.osmTag("amenity", "post_box").collect())
                .collect();
        //assertEquals( 1, result.get(0).getRight().size());
        //assertEquals( 0, result.get(1).getRight().size());
        assertEquals( 2, result.get(0).getRight().size());
        assertEquals( 0, result.get(1).getRight().size());
    }

  @Test
  public void testNeighbourhoodMapForSnapshotAndNearbyContributions2() throws Exception {

    // Create MapReducer
    List<Pair<OSHDBMapReducible, List>> result = createMapReducerOSMEntitySnapshot()
        .neighbourhoodMap(
            54.,
            mapReduce -> mapReduce.osmTag("highway", "primary").collect(),
            true,
            ContributionType.CREATION)
        //.neighbourhood(54., mapReduce -> mapReduce.osmTag("amenity", "post_box").collect())
        .collect();
    //assertEquals( 1, result.get(0).getRight().size());
    //assertEquals( 0, result.get(1).getRight().size());
    assertEquals( 2, result.get(0).getRight().size());
    assertEquals( 0, result.get(1).getRight().size());
  }

  @Test
  public void testNeighbourhoodMapForContributionAndNearbySnapshots() throws Exception {

    // Create MapReducer
    List<Pair<OSHDBMapReducible, List>> result = createMapReducerOSMContribution()
        .neighbourhoodMap(54.,
            mapReduce -> mapReduce.osmTag("highway", "primary").collect(),
            GEOMETRY_OPTIONS.BOTH)
        .collect();

    //assertEquals( 1, result.get(0).getRight().size());
    //assertEquals( 0, result.get(1).getRight().size());
    assertEquals( 2, result.get(1).getRight().size());

    // Create MapReducer
    List<Pair<OSHDBMapReducible, List>> resultAfter = createMapReducerOSMContribution()
        .neighbourhoodMap(54.,
            mapReduce -> mapReduce.osmTag("highway", "primary").collect(),
            GEOMETRY_OPTIONS.AFTER)
        .collect();

    //assertEquals( 1, result.get(0).getRight().size());
    //assertEquals( 0, result.get(1).getRight().size());
    assertEquals( 2, resultAfter.get(1).getRight().size());

    // todo add test for BEFORE that checks exception
  }

  @Test
  public void testNeighbourhoodFilterKeyForOSMContributionAndNearbySnapshots() throws Exception {
    // Create MapReducer
    Number result = createMapReducerOSMContribution()
        .neighbourhoodFilter(54., "highway")
        .count();
    //assertEquals( 2, result);
    assertEquals( 92, result);
  }

  @Test
  public void testNeighbourhoodFilterCallbackForContributionAndNearbySnapshots() throws Exception {
    // Create MapReducer
    Number result = createMapReducerOSMContribution()
        .neighbourhoodFilter(54., mapReduce -> mapReduce.osmTag("highway").count() > 2)
        //.neighbouring(54., mapReduce -> mapReduce.osmTag("amenity", "post_box").count() > 0)
        .count();
    //assertEquals( 1, result);
    assertEquals( 55, result);
  }

  @Test
  public void testInsideForSnapshots() throws Exception {
    // Create MapReducer
    Number result = createMapReducerOSMEntitySnapshot()
        .inside("landuse")
        .count();
    //todo improve test
    assertEquals( 0, result);
  }

}