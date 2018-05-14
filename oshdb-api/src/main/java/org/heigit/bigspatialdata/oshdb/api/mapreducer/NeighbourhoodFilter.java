package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import it.unimi.dsi.fastutil.longs.LongList;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunctionWithException;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;

import java.time.LocalDate;
import java.util.*;

/**
 * Finds objects within in the neigbourhood of an object
 *
 **/
public class NeighbourhoodFilter {

    private static final double ONE_DEGREE_IN_METERS_AT_EQUATOR = 110567.;
    public enum geometryOptions {BEFORE, AFTER, BOTH}

    public static <Y> Y applyToOSMSnapshot(OSHDBJdbc oshdb,
                                           OSHDBTimestampList timestampList,
                                           Double distanceInMeter,
                                           SerializableFunctionWithException<MapReducer, Y> MapReduce,
                                           OSMEntitySnapshot snapshot,
                                           boolean queryContributions) throws Exception {

      DefaultTagInterpreter defaultTagInterpreter = new DefaultTagInterpreter(oshdb.getConnection());
      MapReducer SubMapReducer;
      OSHDBTimestamp end;
      OSHDBTimestamps timestamps = (OSHDBTimestamps) timestampList;

      // Get geometry of feature
      Geometry geom = OSHDBGeometryBuilder.getGeometry(snapshot.getEntity(),
          snapshot.getTimestamp(),
          defaultTagInterpreter);

      // Convert distanceInMeters to degree longitude for bounding box of second mapreducer
      double distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geom.getCentroid().getX(), distanceInMeter);

      // Buffer geometry twice with different buffer distances in degrees for latitude and longitude
      Envelope geomBufferedLng = geom.buffer(distanceInDegreeLongitude).getEnvelopeInternal();
      Envelope geomBufferedLat = geom.buffer(distanceInMeter / ONE_DEGREE_IN_METERS_AT_EQUATOR).getEnvelopeInternal();

      // Get coordinates of bounding box
      double minLat = geomBufferedLat.getMinY();
      double maxLat = geomBufferedLat.getMaxY();
      double minLon = geomBufferedLng.getMinX();
      double maxLon = geomBufferedLng.getMaxX();

      // Get start and end timestamp of current snapshot
      ArrayList<OSHDBTimestamp> timestampArrayList = new ArrayList(timestampList.get().tailSet(snapshot.getTimestamp()));
      if (timestampArrayList.size() <=1) {
        end = timestamps.getEnd();
      } else {
        //todo subtract one day from date
        end = timestampArrayList.get(1);
      }

      if (queryContributions) {
        SubMapReducer = OSMContributionView.on(oshdb)
            .keytables(oshdb)
            .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
            .timestamps(new OSHDBTimestamps(snapshot.getTimestamp().toString(), end.toString()))
            .filter((contribution) -> {
              try {
                boolean geomBeforeWithinDistance;
                boolean geomAfterWithinDistance;

                // Check if geometry before editing is within distance of entity snapshot geometry
                try {
                  Geometry geometryBefore = OSHDBGeometryBuilder.getGeometry(contribution.getEntityAfter(),
                          contribution.getTimestamp(),
                          defaultTagInterpreter);
                  geomBeforeWithinDistance = Geo.isWithinDistance(geom, geometryBefore, distanceInMeter);
                } catch (Exception e) {
                  geomBeforeWithinDistance = false;
                }

                // Check if geometry after editing is within distance of entity snapshot geometry
                try {
                  Geometry geometryAfter = OSHDBGeometryBuilder.getGeometry(contribution.getEntityAfter(),
                          contribution.getTimestamp(),
                          defaultTagInterpreter);
                  geomAfterWithinDistance = Geo.isWithinDistance(geom, geometryAfter, distanceInMeter);
                } catch (Exception e) {
                  geomAfterWithinDistance = false;
                }

                // Check if either one of the geometries are within the buffer distance
                return geomBeforeWithinDistance | geomAfterWithinDistance;

              } catch (Exception e) {
                return false;
              }
            });
      } else {
        SubMapReducer = OSMEntitySnapshotView.on(oshdb)
            .keytables(oshdb)
            .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
            .timestamps(snapshot.getTimestamp().toString())
            .filter((snapshotNgb) -> {
              try {
                  // Get geometry of object
                Geometry geomNgb = OSHDBGeometryBuilder.getGeometry(snapshotNgb.getEntity(),
                    snapshotNgb.getTimestamp(),
                    defaultTagInterpreter);
                // Check if geometry is within buffer distance
                return Geo.isWithinDistance(geom, geomNgb, distanceInMeter);
              } catch (Exception e) {
                return false;
              }
            });
      }

      // Apply mapReducer given by user
      return MapReduce.apply(SubMapReducer);
    }

    public static <Y> Y applyToOSMContribution(OSHDBJdbc oshdb, Double distanceInMeter,
                                               SerializableFunctionWithException<MapReducer, Y> MapReduce,
                                               OSMContribution contribution,
                                               geometryOptions geometryVersion) throws Exception {

      DefaultTagInterpreter defaultTagInterpreter = new DefaultTagInterpreter(oshdb.getConnection());
      Geometry geomBefore = null;
      Geometry geomAfter = null;
      Double distanceInDegreeLongitude = null;

      // Get geometry of feature after editing
      if (geometryVersion == geometryOptions.BOTH | geometryVersion == geometryOptions.AFTER) {
        try {
          geomAfter = OSHDBGeometryBuilder.getGeometry(contribution.getEntityAfter(),
              contribution.getTimestamp(),
              defaultTagInterpreter);
          // Convert distanceInMeters to degree longitude for bounding box of second mapreducer
          distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geomAfter.getCentroid().getX(), distanceInMeter);
        } catch (Exception e) {
          System.out.println("invalid geometry.1");
          if (geometryVersion == geometryOptions.AFTER) throw new Exception();
        }
      }

      if (geometryVersion == geometryOptions.BOTH | geometryVersion == geometryOptions.BEFORE) {
        // Get geometry of feature before editing.
        try {
          geomBefore = OSHDBGeometryBuilder.getGeometry(contribution.getEntityBefore(),
                  contribution.getTimestamp(),
                  defaultTagInterpreter);
          // Convert distanceInMeters to degree longitude for bounding box of second mapreducer
          distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geomBefore.getCentroid().getX(), distanceInMeter);
        } catch (Exception e) {
          System.out.println("invalid geometry.2");
          if (geometryVersion == geometryOptions.BEFORE) throw new Exception("Invalid geometry.");
        }
      }

      // Check if either one of the geometries is invalid. If so
      if (geometryVersion == geometryOptions.BOTH) {
        if (geomBefore == null & geomAfter != null) {
          geomBefore = geomAfter;
        } else if (geomAfter == null & geomBefore != null) {
          geomAfter = geomBefore;
        } else if (((geomAfter == null) & (geomBefore == null)) | (distanceInDegreeLongitude == null)) {
          System.out.println("invalid geometry.3");
          throw new Exception("Invalid geometry.");
        }
      }

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

      // Make geom variables final for lambda function
      Geometry finalGeomBefore = geomBefore;
      Geometry finalGeomAfter = geomAfter;

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
                    case AFTER: {
                        boolean res = Geo.isWithinDistance(finalGeomAfter, geomNeighbour, distanceInMeter);
                        System.out.print(res);
                        return res;
                    }
                    case BOTH: return Geo.isWithinDistance(finalGeomBefore, geomNeighbour, distanceInMeter) | Geo.isWithinDistance(finalGeomAfter, geomNeighbour, distanceInMeter);
                    default: return Geo.isWithinDistance(finalGeomAfter, geomNeighbour, distanceInMeter);
                  }
              } catch (Exception e) {
                  return false;
              }
          });

      // Apply mapReducer given by user
      return MapReduce.apply(SubMapReducer);
  }

}