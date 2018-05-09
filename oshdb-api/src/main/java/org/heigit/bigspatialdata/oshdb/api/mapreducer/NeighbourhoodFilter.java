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
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;

/**
 * Finds objects within in the neigbourhood of an object
 *
 **/
public class NeighbourhoodFilter {

    private static final double ONE_DEGREE_IN_METERS_AT_EQUATOR = 110567.;

    public enum geometryOptions {BEFORE, AFTER, BOTH}
    public enum targetOptions {SNAPSHOT, CONTRIBUTION}

    public static <Y> Y applyToOSMSnapshot(OSHDBJdbc oshdb,
                                           OSHDBTimestampList timestamp,
                                           Double distanceInMeter,
                                           SerializableFunctionWithException<MapReducer, Y> MapReduce,
                                           OSMEntitySnapshot snapshot,
                                           targetOptions target) throws Exception {

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

        // mapreducer instance
        MapReducer SubMapReducer = null;

        switch (target) {
            case CONTRIBUTION:
                SubMapReducer = OSMContributionView.on(oshdb)
                        .keytables(oshdb)
                        .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
                        .timestamps(timestamp)
                        .filter((snapshot2) -> {
                            try {

                                boolean geomBeforeWithinDistance;
                                boolean geomAfterWithinDistance;

                                try {
                                    // Check if geometry before editing is within distance of entity snapshot geometry
                                    Geometry geometryBefore = OSHDBGeometryBuilder.getGeometry(snapshot2.getEntityAfter(),
                                            snapshot2.getTimestamp(),
                                            defaultTagInterpreter);
                                    geomBeforeWithinDistance = Geo.isWithinDistance(geom, geometryBefore, distanceInMeter);
                                } catch (Exception e){
                                    geomBeforeWithinDistance = false;
                                }

                                try {
                                    // Check if geometry after editing is within distance of entity snapshot geometry
                                    Geometry geometryAfter = OSHDBGeometryBuilder.getGeometry(snapshot2.getEntityAfter(),
                                            snapshot2.getTimestamp(),
                                            defaultTagInterpreter);
                                    geomAfterWithinDistance =  Geo.isWithinDistance(geom, geometryAfter, distanceInMeter);
                                } catch (Exception e){
                                    geomAfterWithinDistance = false;
                                }

                                // Check if either one of the geometries are within the buffer distance
                                return geomBeforeWithinDistance | geomAfterWithinDistance;

                            } catch (Exception e) {
                                return false;
                            }
                        });
                break;

            case SNAPSHOT:
                SubMapReducer = OSMEntitySnapshotView.on(oshdb)
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
                                return false;
                            }
                        });
                break;
        }


        // Apply mapReducer given by user
        return MapReduce.apply(SubMapReducer);
    }

    public static <Y> Y applyToOSMContribution(OSHDBJdbc oshdb, Double distanceInMeter,
                                               SerializableFunctionWithException<MapReducer, Y> MapReduce,
                                               OSMContribution contribution,
                                               geometryOptions geometryVersion) throws Exception {

        DefaultTagInterpreter defaultTagInterpreter = new DefaultTagInterpreter(oshdb.getConnection());
        Geometry geomBefore;

        // Get geometry of feature after editing
        Geometry geomAfter = OSHDBGeometryBuilder.getGeometry(contribution.getEntityAfter(),
                contribution.getTimestamp(),
                defaultTagInterpreter);

        // Get geometry of feature before editing. If it fails, use geometryAfter instead.
        try {
            geomBefore = OSHDBGeometryBuilder.getGeometry(contribution.getEntityBefore(),
                    contribution.getTimestamp(),
                    defaultTagInterpreter);
        } catch (Exception e) {
            geomBefore = geomAfter;
        }

        // Convert distanceInMeters to degree longitude for bounding box of second mapreducer
        double distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geomAfter.getCentroid().getX(), distanceInMeter);

        // Buffer geometries
        Envelope geomBeforeBufferedLng = geomBefore.buffer(distanceInDegreeLongitude).getEnvelopeInternal();
        Envelope geomBeforeBufferedLat = geomBefore.buffer(distanceInMeter / ONE_DEGREE_IN_METERS_AT_EQUATOR).getEnvelopeInternal();
        Envelope geomAfterBufferedLng = geomAfter.buffer(distanceInDegreeLongitude).getEnvelopeInternal();
        Envelope geomAfterBufferedLat = geomAfter.buffer(distanceInMeter / ONE_DEGREE_IN_METERS_AT_EQUATOR).getEnvelopeInternal();

        // Get min/max coordinates of bounding box
        double minLat = Math.min(geomBeforeBufferedLat.getMinY(), geomAfterBufferedLat.getMinY());
        double maxLat = Math.max(geomBeforeBufferedLat.getMaxY(), geomAfterBufferedLat.getMinY());
        double minLon = Math.min(geomBeforeBufferedLng.getMinX(), geomAfterBufferedLng.getMinX());
        double maxLon = Math.max(geomBeforeBufferedLng.getMaxX(), geomAfterBufferedLng.getMaxX());

        // Make geomBefore final for lambda function in following filter(...)
        Geometry finalGeomBefore = geomBefore;

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
                        switch (geometryVersion) {
                            case BEFORE: return Geo.isWithinDistance(finalGeomBefore, geomNeighbour, distanceInMeter);
                            case AFTER: return Geo.isWithinDistance(geomAfter, geomNeighbour, distanceInMeter);
                            case BOTH: return Geo.isWithinDistance(finalGeomBefore, geomNeighbour, distanceInMeter) | Geo.isWithinDistance(geomAfter, geomNeighbour, distanceInMeter);
                            default: return Geo.isWithinDistance(geomAfter, geomNeighbour, distanceInMeter);
                        }

                    } catch (Exception e) {
                        return false;
                    }
                });

        // Apply mapReducer given by user
        return MapReduce.apply(SubMapReducer);
    }

}