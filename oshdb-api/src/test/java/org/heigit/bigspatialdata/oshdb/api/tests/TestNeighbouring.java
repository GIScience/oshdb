package org.heigit.bigspatialdata.oshdb.api.tests;

import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
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

    public TestNeighbouring() throws Exception {
        //oshdb = (new OSHDBH2("/Users/chludwig/Documents/oshdb/nepal_20180201_z12_keytables.compressed.oshdb")).multithreading(true);
        oshdb = new OSHDBH2("./src/test/resources/test-data").multithreading(true);
    }

    private MapReducer<OSMEntitySnapshot> createMapReducerOSMEntitySnapshot() throws Exception {
        return OSMEntitySnapshotView.on(oshdb)
                .keytables(oshdb)
                .timestamps(timestamps)
                .areaOfInterest(bbox)
                //.osmTag("leisure", "park");
                .osmTag("building");
    }

    // todo add tests for OSMContribution
    private MapReducer<OSMContribution> createMapReducerOSMContribution() throws Exception {
        return OSMContributionView.on(oshdb).osmType(OSMType.WAY).osmTag("highway").areaOfInterest(bbox);
    }

    @Test
    public void testNeighbouringKey() throws Exception {

        // Create MapReducer
        Number result = createMapReducerOSMEntitySnapshot()
                .neighbouring(54., "highway")
                //.neighbouring(54., "amenity")
                .count();
        //assertEquals( 2, result);
        assertEquals( 108, result);
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
    public void testNeighbourhood() throws Exception {

        // Create MapReducer
        List<Pair<OSHDBMapReducible, List>> result = createMapReducerOSMEntitySnapshot()
                .neighbourhood(54., mapReduce -> mapReduce.osmTag("highway", "primary").collect())
                //.neighbourhood(54., mapReduce -> mapReduce.osmTag("amenity", "post_box").collect())
                .collect();
        //assertEquals( 1, result.get(0).getRight().size());
        //assertEquals( 0, result.get(1).getRight().size());
        assertEquals( 3, result.get(0).getRight().size());
        assertEquals( 0, result.get(1).getRight().size());
    }
}