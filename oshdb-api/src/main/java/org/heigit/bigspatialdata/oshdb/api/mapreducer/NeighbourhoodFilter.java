package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunctionWithException;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;

/**
 * Finds objects within in the neigbourhoood of an object
 *
 **/
public class NeighbourhoodFilter {

    private static double earthRadius = 6371000; //meters

    public static <X, Y> Y apply(OSHDBJdbc oshdb, Double distanceInMeter, SerializableFunctionWithException<MapReducer, Y> MapReduce, OSMEntitySnapshot snapshot) throws Exception{

        DefaultTagInterpreter defaultTagInterpreter;
        Y result;

        // Get geometry of feature
        defaultTagInterpreter = new DefaultTagInterpreter(oshdb.getConnection());
        Geometry geom = OSHDBGeometryBuilder.getGeometry(snapshot.getEntity(),
                snapshot.getTimestamp(),
                defaultTagInterpreter);

        // Buffer geometry
        Envelope geomBuffered = geom.buffer(distanceInMeter / earthRadius).getEnvelopeInternal();

        // Get min/max coordinates of bounding box
        double minLat = geomBuffered.getMinY();
        double maxLat = geomBuffered.getMaxY();
        double minLon = geomBuffered.getMinX();
        double maxLon = geomBuffered.getMaxX();

        MapReducer SubMapReducer = OSMEntitySnapshotView.on(oshdb)
            .keytables(oshdb)
            .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
            .timestamps(snapshot.getTimestamp().toString())
            .filter((snapshot2) -> {
                try {
                    // Get geometry of object and convert it to projected CRS
                    Geometry geom2 = OSHDBGeometryBuilder.getGeometry(snapshot2.getEntity(),
                            snapshot2.getTimestamp(),
                            defaultTagInterpreter);

                    // Check if geometry is within buffer distance
                    return Geo.isWithinDistance(geom, geom2, distanceInMeter);

                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            });

        // Apply mapReducer given by user
        result = MapReduce.apply(SubMapReducer);

        return result;

    }

}