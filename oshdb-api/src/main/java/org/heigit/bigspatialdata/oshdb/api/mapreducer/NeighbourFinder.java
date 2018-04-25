package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;

import java.util.LinkedList;
import java.util.List;

/**
 * Finds objects with a certain tag nearby the input object
 *
 **/
public class NeighbourFinder implements SerializableFunction<OSMEntitySnapshot, List<OSMEntitySnapshot>> {

    private OSHDBJdbc oshdb;
    private String key;
    private String tag;
    private Double distanceInMeter;

    protected NeighbourFinder(OSHDBJdbc oshdb, String key, String tag, Double distanceInMeter) {
        this.oshdb = oshdb;
        this.key = key;
        this.tag = tag;
        this.distanceInMeter = distanceInMeter;
    }

    public List<OSMEntitySnapshot> apply(OSMEntitySnapshot snapshot) {

        List<OSMEntitySnapshot> result = new LinkedList<>();
        DefaultTagInterpreter defaultTagInterpreter;

        try {

            // Convert from geographic coordinates to projected CRS
                /* ---- Not working -----
                // Source CRS (WGS 84)
                CoordinateReferenceSystem sourceCRS = CRS.decode("EPSG:1981", true); // WGS 84
                // Set target SRS (Pseudo mercado)
                CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:3857", true);
                // Transformation object
                MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, false);

                // Create circle polygon around centroid with buffer as radius
                //Geometry geom = JTS.transform(snapshot.getGeometry(), transform);
                */

            // Get geometry of feature

            defaultTagInterpreter = new DefaultTagInterpreter(this.oshdb.getConnection());
            Geometry geom = OSHDBGeometryBuilder.getGeometry(snapshot.getEntity(),
                    snapshot.getTimestamp(),
                    defaultTagInterpreter);

            // Buffer geometry
            Envelope geomBuffered = geom.buffer(distanceInMeter).getEnvelopeInternal();

            // Get min/max coordinates of bounding box
            double minLat = geomBuffered.getMinY();
            double maxLat = geomBuffered.getMaxY();
            double minLon = geomBuffered.getMinX();
            double maxLon = geomBuffered.getMaxX();

            OSMEntitySnapshotView.on(this.oshdb)
                .keytables(this.oshdb)
                .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
                .timestamps("2014-01-01", "2014-02-01", OSHDBTimestamps.Interval.MONTHLY)
                .where(key, tag)
                .collect()
                .forEach((snapshot2) -> {

                    try {
                        // Get geometry of object and convert it to projected CRS
                        //Geometry geom2 = JTS.transform(snapshot2.getGeometry(), transform);
                        Geometry geom2 = OSHDBGeometryBuilder.getGeometry(snapshot2.getEntity(),
                                snapshot2.getTimestamp(),
                                defaultTagInterpreter);

                        // Check if geometry is within buffer distance
                        if (geom2.isWithinDistance(geom, distanceInMeter)) {
                            result.add(snapshot2);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }
}