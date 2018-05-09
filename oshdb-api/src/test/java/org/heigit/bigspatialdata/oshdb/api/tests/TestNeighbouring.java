package org.heigit.bigspatialdata.oshdb.api.tests;

import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.NeighbourhoodFilter;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.List;

public class TestNeighbouring {
    private final OSHDBJdbc oshdb;

    // create bounding box from coordinates
    //private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(85.3406012, 27.6991942, 85.3585444, 27.7121143);
    private final OSHDBBoundingBox bbox = new OSHDBBoundingBox(8, 49, 9, 50);
    private final OSHDBTimestamps timestamps = new OSHDBTimestamps("2017-01-01", "2017-01-02", OSHDBTimestamps.Interval.MONTHLY);
    private final OSHDBTimestamps timestamps2 = new OSHDBTimestamps("2013-01-01", "2014-01-02", OSHDBTimestamps.Interval.YEARLY);

    public TestNeighbouring() throws Exception {
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

    // todo add tests for OSMContribution
    private MapReducer<OSMContribution> createMapReducerOSMContribution() {
        return OSMContributionView.on(oshdb)
                .keytables(oshdb)
                .timestamps(timestamps2)
                .areaOfInterest(bbox)
                .osmTag("building");
    }

    @Test
    public void testNeighbouringKeyForOSMContribution() throws Exception {

        // Create MapReducer
        MapReducer<OSMContribution> mr = createMapReducerOSMContribution();
                //.neighbouring(54., "highway")
        Number result = mr.neighbouring(54., "highway").count();

        //assertEquals( 2, result);
        assertEquals( 37, result);
    }

    @Test
    public void testNeighbouringKeyForOSMSnapshot() throws Exception {

        // Create MapReducer
        Number result = createMapReducerOSMEntitySnapshot()
                .neighbouring(54., "highway")
                //.neighbouring(54., "amenity")
                .count();
        //assertEquals( 2, result);
        assertEquals( 148, result);
    }

    @Test
    public void testNeighbouringKeyAndValue() throws Exception {

        // Create MapReducer
        Number result = createMapReducerOSMEntitySnapshot()
                .neighbouring(54., "highway", "primary")
                //.neighbouring(54., "amenity", "post_box")
                .count();
        //assertEquals( 1, result);
        assertEquals( 2, result);
    }

    @Test
    public void testNeighbouringMapReducer() throws Exception {

        // Create MapReducer
        Number result = createMapReducerOSMEntitySnapshot()
                .neighbouring(54., mapReduce -> mapReduce.osmTag("highway", "primary").count() > 0)
                //.neighbouring(54., mapReduce -> mapReduce.osmTag("amenity", "post_box").count() > 0)
                .count();
        //assertEquals( 1, result);
        assertEquals( 2, result);
    }

    @Test
    public void testNeighbourhoodForOSMContributionAndNearbySnapshots() throws Exception {

        // Create MapReducer
        List<Pair<OSHDBMapReducible, List>> result = createMapReducerOSMContribution()
                .neighbourhood(54.,
                        mapReduce -> mapReduce.osmTag("highway", "primary").collect(),
                        NeighbourhoodFilter.geometryOptions.BOTH)
                .collect();

        //assertEquals( 1, result.get(0).getRight().size());
        //assertEquals( 0, result.get(1).getRight().size());
        //todo improve test
        assertEquals( 0, result.get(0).getRight().size());
        assertEquals( 0, result.get(1).getRight().size());
    }

    @Test
    public void testNeighbourhoodForOSMSnapshotAndNearbyOSMContributions() throws Exception {

        // Create MapReducer
        List<Pair<OSHDBMapReducible, List>> result = createMapReducerOSMEntitySnapshot()
                .neighbourhood(54.,
                        mapReduce -> mapReduce.osmTag("highway", "primary").collect(),
                        NeighbourhoodFilter.targetOptions.CONTRIBUTION)
                //.neighbourhood(54., mapReduce -> mapReduce.osmTag("amenity", "post_box").collect())
                .collect();
        //assertEquals( 1, result.get(0).getRight().size());
        //assertEquals( 0, result.get(1).getRight().size());
        assertEquals( 14, result.get(0).getRight().size());
        assertEquals( 0, result.get(3).getRight().size());
    }

    @Test
    public void testNeighbourhoodForOSMSnapshotAndNearbyOSMSnapshots() throws Exception {

        // Create MapReducer
        List<Pair<OSHDBMapReducible, List>> result = createMapReducerOSMEntitySnapshot()
                .neighbourhood(54.,
                        mapReduce -> mapReduce.osmTag("highway", "primary").collect(),
                        NeighbourhoodFilter.targetOptions.SNAPSHOT)
                //.neighbourhood(54., mapReduce -> mapReduce.osmTag("amenity", "post_box").collect())
                .collect();
        //assertEquals( 1, result.get(0).getRight().size());
        //assertEquals( 0, result.get(1).getRight().size());
        assertEquals( 2, result.get(0).getRight().size());
        assertEquals( 4, result.get(1).getRight().size());
    }
}