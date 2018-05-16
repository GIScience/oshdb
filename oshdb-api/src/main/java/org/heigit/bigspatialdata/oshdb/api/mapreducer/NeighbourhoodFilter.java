package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunctionWithException;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;

import java.util.*;

/**
 * Finds objects within in the neigbourhood of an object
 *
 **/
public class NeighbourhoodFilter {
    public enum GEOMETRY_OPTIONS {BEFORE, AFTER, BOTH}

    public static <Y> Y applyToOSMEntitySnapshot(
        OSHDBJdbc oshdb,
        OSHDBTimestampList timestampList,
        Double distanceInMeter,
        SerializableFunctionWithException<MapReducer, Y> mapReduce,
        OSMEntitySnapshot snapshot,
        boolean queryContributions
    ) throws Exception {

      OSHDBTimestamp end;
      OSHDBTimestamps timestamps = (OSHDBTimestamps) timestampList;
      MapReducer subMapReducer;

      // Get geometry of feature
      Geometry geom = snapshot.getGeometryUnclipped();

      // Convert distanceInMeters to degree longitude for bounding box of second mapreducer
      double distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geom.getCentroid().getX(), distanceInMeter);

      // Get coordinates of bounding box
      Envelope envelope = geom.getEnvelopeInternal();
      double minLon = envelope.getMinX() - distanceInDegreeLongitude;
      double maxLon = envelope.getMaxX() + distanceInDegreeLongitude;
      double minLat = envelope.getMinY() - distanceInMeter / Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR;
      double maxLat = envelope.getMaxY() + distanceInMeter / Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR;

      // Get start and end timestamp of current snapshot
      ArrayList<OSHDBTimestamp> timestampArrayList = new ArrayList<>(timestampList.get().tailSet(snapshot.getTimestamp()));
      if (timestampArrayList.size() <=1) {
        end = timestamps.getEnd();
      } else {
        //todo subtract one day from date
        end = timestampArrayList.get(1);
      }

      if (queryContributions) {
        subMapReducer = OSMContributionView.on(oshdb)
            .keytables(oshdb)
            .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
            .timestamps(new OSHDBTimestamps(snapshot.getTimestamp().toString(), end.toString()))
            .filter((contribution) -> {
              try {
                boolean geomBeforeWithinDistance;
                boolean geomAfterWithinDistance;

                // Check if geometry before editing is within distance of entity snapshot geometry
                try {
                  Geometry geometryBefore = contribution.getGeometryUnclippedBefore();
                  geomBeforeWithinDistance = Geo.isWithinDistance(geom, geometryBefore, distanceInMeter);
                } catch (Exception e) {
                  geomBeforeWithinDistance = false;
                }

                // Check if geometry after editing is within distance of entity snapshot geometry
                try {
                  Geometry geometryAfter = contribution.getGeometryUnclippedAfter();
                  geomAfterWithinDistance = Geo.isWithinDistance(geom, geometryAfter, distanceInMeter);
                } catch (Exception e) {
                  geomAfterWithinDistance = false;
                }

                // Check if either one of the geometries are within the buffer distance
                return geomBeforeWithinDistance || geomAfterWithinDistance;

              } catch (Exception e) {
                return false;
              }
            });
      } else {
        subMapReducer = OSMEntitySnapshotView.on(oshdb)
            .keytables(oshdb)
            .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
            .timestamps(snapshot.getTimestamp().toString())
            .filter((snapshotNgb) -> {
              try {
                Geometry geomNgb = snapshotNgb.getGeometryUnclipped();
                return Geo.isWithinDistance(geom, geomNgb, distanceInMeter);
              } catch (Exception e) {
                return false;
              }
            });
      }
      // Apply mapReducer given by user
      return mapReduce.apply(subMapReducer);
    }

    /* ----- under construction -------
    Function that returns neighbours of contributions
    Still needs discussion what it should return exactly and based on which attributes the contributions
    and neighbours should be filtered
     */
    public static <Y> Y applyToOSMContribution(
        OSHDBJdbc oshdb, Double distanceInMeter,
        SerializableFunctionWithException<MapReducer, Y> mapReduce,
        OSMContribution contribution,
        GEOMETRY_OPTIONS geometryVersion
    ) throws Exception {

      Geometry geomBefore = null;
      Geometry geomAfter = null;
      Double distanceInDegreeLongitude = null;

      // Get geometry of feature after editing
      if (geometryVersion == GEOMETRY_OPTIONS.BOTH || geometryVersion == GEOMETRY_OPTIONS.AFTER) {
        try {
          geomAfter = contribution.getGeometryUnclippedAfter();
          // Convert distanceInMeters to degree longitude for bounding box of second mapreducer
          distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geomAfter.getCentroid().getX(), distanceInMeter);
        } catch (Exception e) {
          System.out.println("invalid geometry.1");
          if (geometryVersion == GEOMETRY_OPTIONS.AFTER) throw new Exception();
        }
      }

      if (geometryVersion == GEOMETRY_OPTIONS.BOTH || geometryVersion == GEOMETRY_OPTIONS.BEFORE) {
        // Get geometry of feature before editing.
        try {
          geomBefore = contribution.getGeometryUnclippedBefore();
          // Convert distanceInMeters to degree longitude for bounding box of second mapreducer
          distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geomBefore.getCentroid().getX(), distanceInMeter);
        } catch (Exception e) {
          System.out.println("invalid geometry.2");
          if (geometryVersion == GEOMETRY_OPTIONS.BEFORE) throw new Exception("Invalid geometry.");
        }
      }

      // Check if either one of the geometries is invalid. If so
      if (geometryVersion == GEOMETRY_OPTIONS.BOTH) {
        if (geomBefore == null && geomAfter != null) {
          geomBefore = geomAfter;
        } else if (geomAfter == null && geomBefore != null) {
          geomAfter = geomBefore;
        } else if (((geomAfter == null) && (geomBefore == null)) || (distanceInDegreeLongitude == null)) {
          System.out.println("invalid geometry.3");
          throw new Exception("Invalid geometry.");
        }
      }

      // Get coordinates of bounding box
      Envelope envelopeBefore = geomBefore.getEnvelopeInternal();
      double minLonB = envelopeBefore.getMinX() - distanceInDegreeLongitude;
      double maxLonB = envelopeBefore.getMaxX() + distanceInDegreeLongitude;
      double minLatB = envelopeBefore.getMinY() - distanceInMeter / Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR;
      double maxLatB = envelopeBefore.getMaxY() + distanceInMeter / Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR;

      // Get coordinates of bounding box
      Envelope envelopeAfter = geomAfter.getEnvelopeInternal();
      double minLonA = envelopeAfter.getMinX() - distanceInDegreeLongitude;
      double maxLonA = envelopeAfter.getMaxX() + distanceInDegreeLongitude;
      double minLatA = envelopeAfter.getMinY() - distanceInMeter / Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR;
      double maxLatA = envelopeAfter.getMaxY() + distanceInMeter / Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR;

      // Get min/max coordinates of bounding box
      double minLat = Math.min(minLatA, minLatB);
      double maxLat = Math.max(maxLatA, maxLatB);
      double minLon = Math.min(minLonA, minLonB);
      double maxLon = Math.max(maxLonA, maxLonB);

      // Make geom variables final for lambda function
      Geometry finalGeomBefore = geomBefore;
      Geometry finalGeomAfter = geomAfter;

      // Find neighbours of geometry
      MapReducer subMapReducer = OSMEntitySnapshotView.on(oshdb)
          .keytables(oshdb)
          .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
          .timestamps(contribution.getTimestamp().toString())
          .filter((snapshot) -> {
              try {
                Geometry geomNeighbour = snapshot.getGeometryUnclipped();
                switch (geometryVersion) {
                    case BEFORE: return Geo.isWithinDistance(finalGeomBefore, geomNeighbour, distanceInMeter);
                    case AFTER: {
                        boolean res = Geo.isWithinDistance(finalGeomAfter, geomNeighbour, distanceInMeter);
                        System.out.print(res);
                        return res;
                    }
                    case BOTH: return Geo.isWithinDistance(finalGeomBefore, geomNeighbour, distanceInMeter) || Geo.isWithinDistance(finalGeomAfter, geomNeighbour, distanceInMeter);
                    default: return Geo.isWithinDistance(finalGeomAfter, geomNeighbour, distanceInMeter);
                  }
              } catch (Exception e) {
                  return false;
              }
          });

      // Apply mapReducer given by user
      return mapReduce.apply(subMapReducer);
  }

}