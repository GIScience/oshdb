package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunctionWithException;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
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

    private static double ONE_DEGREE_IN_METERS_AT_EQUATOR = 111000; //meters

    public static <Y> Y applyToOSMSnapshot(OSHDBJdbc oshdb, Double distanceInMeter, SerializableFunctionWithException<MapReducer, Y> MapReduce, OSMEntitySnapshot snapshot) throws Exception {

        DefaultTagInterpreter defaultTagInterpreter = new DefaultTagInterpreter(oshdb.getConnection());

        // Get geometry of feature
        Geometry geom = OSHDBGeometryBuilder.getGeometry(snapshot.getEntity(),
                snapshot.getTimestamp(),
                defaultTagInterpreter);

        // Convert distanceInMeters to degree longitude for bounding box of second mapreducer
        double distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geom.getCentroid().getX(), distanceInMeter);

        // Buffer geometry
        Envelope geomBufferedLng = geom.buffer(distanceInDegreeLongitude).getEnvelopeInternal();
        Envelope geomBufferedLat = geom.buffer(distanceInMeter / ONE_DEGREE_IN_METERS_AT_EQUATOR).getEnvelopeInternal();

        // Get min/max coordinates of bounding box
        double minLat = geomBufferedLat.getMinY();
        double maxLat = geomBufferedLat.getMaxY();
        double minLon = geomBufferedLng.getMinX();
        double maxLon = geomBufferedLng.getMaxX();

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
        return MapReduce.apply(SubMapReducer);
    }

    public static <Y> Y applyToOSMContribution(OSHDBJdbc oshdb, Double distanceInMeter, SerializableFunctionWithException<MapReducer, Y> MapReduce, OSMContribution contribution) throws Exception {

        DefaultTagInterpreter defaultTagInterpreter = new DefaultTagInterpreter(oshdb.getConnection());

        // Get geometry of feature before editing
        Geometry geomBefore = OSHDBGeometryBuilder.getGeometry(contribution.getEntityBefore(),
                contribution.getTimestamp(),
                defaultTagInterpreter);
        // Buffer geometry
        Envelope geomBeforeBuffered = geomBefore.buffer(distanceInMeter / ONE_DEGREE_IN_METERS_AT_EQUATOR).getEnvelopeInternal();

        // Get geometry of feature after editing
        Geometry geomAfter = OSHDBGeometryBuilder.getGeometry(contribution.getEntityBefore(),
                contribution.getTimestamp(),
                defaultTagInterpreter);

        // Convert distanceInMeters to degree longitude for bounding box of second mapreducer
        double distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geomAfter.getCentroid().getX(), distanceInMeter);

        // Buffer geometry
        Envelope geomBufferedLng = geomAfter.buffer(distanceInDegreeLongitude).getEnvelopeInternal();
        Envelope geomBufferedLat = geomAfter.buffer(distanceInMeter / ONE_DEGREE_IN_METERS_AT_EQUATOR).getEnvelopeInternal();

        // Get min/max coordinates of bounding box
        double minLat = geomBufferedLat.getMinY();
        double maxLat = geomBufferedLat.getMaxY();
        double minLon = geomBufferedLng.getMinX();
        double maxLon = geomBufferedLng.getMaxX();


        // Find neighbours of geometry
        MapReducer SubMapReducer = OSMEntitySnapshotView.on(oshdb)
                .keytables(oshdb)
                .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
                .timestamps(contribution.getTimestamp().toString())
                .filter((snapshot) -> {
                    try {
                        // Get geometry of object and convert it to projected CRS
                        Geometry geomNeighbour = OSHDBGeometryBuilder.getGeometry(snapshot.getEntity(),
                                snapshot.getTimestamp(),
                                defaultTagInterpreter);

                        // Check if geometry is within buffer distance
                        return Geo.isWithinDistance(geomBefore, geomNeighbour, distanceInMeter) | Geo.isWithinDistance(geomAfter, geomNeighbour, distanceInMeter) ;

                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                });

        // Apply mapReducer given by user
        return MapReduce.apply(SubMapReducer);
    }
    /*
    public static <Y> Y applyToOSMContribution(OSHDBJdbc oshdb, Double distanceInMeter, SerializableFunctionWithException<MapReducer, Y> MapReduce, OSMContribution contribution, boolean useGeometryAfter) throws Exception {

        DefaultTagInterpreter defaultTagInterpreter = new DefaultTagInterpreter(oshdb.getConnection());

        // Get geometry of feature before editing
        if (useGeometryAfter) {
            Geometry geom = OSHDBGeometryBuilder.getGeometry(contribution.getEntityAfter(),
                    contribution.getTimestamp(),
                    defaultTagInterpreter);
        } else {
            Geometry geom = OSHDBGeometryBuilder.getGeometry(contribution.getEntityBefore(),
                    contribution.getTimestamp(),
                    defaultTagInterpreter);
        }

        // Buffer geometry
        Envelope geomBuffered = geom.buffer(distanceInMeter / earthRadius).getEnvelopeInternal();

        // Get min/max coordinates of bounding box
        double minLat = Math.min(geomBuffered.getMinY(), geomAfterBuffered.getMinY());
        double maxLat = Math.max(geomBuffered.getMaxY(), geomAfterBuffered.getMaxY());
        double minLon = Math.min(geomBuffered.getMinX(), geomAfterBuffered.getMinX());
        double maxLon = Math.max(geomBuffered.getMaxX(), geomAfterBuffered.getMaxX());

        // Find neighbours of geometry
        MapReducer SubMapReducer = OSMEntitySnapshotView.on(oshdb)
                .keytables(oshdb)
                .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
                .timestamps(contribution.getTimestamp().toString())
                .filter((snapshot) -> {
                    try {
                        // Get geometry of object and convert it to projected CRS
                        Geometry geomNeighbour = OSHDBGeometryBuilder.getGeometry(snapshot.getEntity(),
                                snapshot.getTimestamp(),
                                defaultTagInterpreter);

                        // Check if geometry is within buffer distance
                        return Geo.isWithinDistance(geomBefore, geomNeighbour, distanceInMeter) | Geo.isWithinDistance(geomAfter, geomNeighbour, distanceInMeter) ;

                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                });

        // Apply mapReducer given by user
        return MapReduce.apply(SubMapReducer);
    }
    */
}