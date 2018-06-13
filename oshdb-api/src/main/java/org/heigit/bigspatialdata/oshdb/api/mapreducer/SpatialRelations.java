package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import static org.heigit.bigspatialdata.oshdb.util.geometry.Geo.isWithinDistance;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.TopologyException;
import com.vividsolutions.jts.index.strtree.STRtree;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunctionWithException;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMContributionView;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.OSMEntitySnapshotView;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;

import java.util.*;

/**
 * Finds objects within in the neigbourhood of an object
 *
 **/
public class SpatialRelations {
  public enum GEOMETRY_OPTIONS {BEFORE, AFTER, BOTH}

  public static <Y> Y neighbourhood(
      OSHDBJdbc oshdb,
      OSHDBTimestampList timestampList,
      Double distanceInMeter,
      SerializableFunctionWithException<MapReducer, Y> mapReduce,
      OSMEntitySnapshot snapshot,
      boolean queryContributions,
      ContributionType contributionType
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
            boolean geomBeforeWithinDistance = false;
            boolean geomAfterWithinDistance = false;
            // Filter by contribution type if given
            if (contributionType != null && !contribution.getContributionTypes().contains(contributionType)) return false;
            // Check if geometry before editing is within distance of entity snapshot geometry
            if (!contribution.getContributionTypes().contains(ContributionType.CREATION)) {
              Geometry geometryBefore = contribution.getGeometryUnclippedBefore();
              geomBeforeWithinDistance = isWithinDistance(geom, geometryBefore, distanceInMeter);
            }
            // Check if geometry after editing is within distance of entity snapshot geometry
            if (!contribution.getContributionTypes().contains(ContributionType.DELETION)) {
                Geometry geometryAfter = contribution.getGeometryUnclippedAfter();
                geomAfterWithinDistance = isWithinDistance(geom, geometryAfter, distanceInMeter);
            }
            // Check if either one of the geometries are within the buffer distance
            return geomBeforeWithinDistance || geomAfterWithinDistance;
          });
    } else {
      subMapReducer = OSMEntitySnapshotView.on(oshdb)
          .keytables(oshdb)
          .areaOfInterest(new OSHDBBoundingBox(minLon, minLat, maxLon, maxLat))
          .timestamps(snapshot.getTimestamp().toString())
          .filter((snapshotNgb) -> {
            try {
              Geometry geomNgb = snapshotNgb.getGeometryUnclipped();
              return isWithinDistance(geom, geomNgb, distanceInMeter);
            } catch (Exception e) {
              return false;
            }
          });
    }
    // Apply mapReducer given by user
    return mapReduce.apply(subMapReducer);
  }

  public static <Y> Y neighbourhood(
      OSHDBJdbc oshdb,
      Double distanceInMeter,
      SerializableFunctionWithException<MapReducer, Y> mapReduce,
      OSMContribution contribution,
      GEOMETRY_OPTIONS geometryVersion
  ) throws Exception {
    Geometry geomBefore = null;
    Geometry geomAfter = null;
    Double distanceInDegreeLongitude = null;

    switch (geometryVersion) {
      case BEFORE:
        if (contribution.getContributionTypes().contains(ContributionType.CREATION)) {
          geomBefore = contribution.getGeometryUnclippedAfter();
          distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geomBefore.getCentroid().getX(),distanceInMeter);
        } else {
          throw new Exception("Contribution of type CREATION. No geometry before contribution available.");
        }
        geomAfter = geomBefore;
        break;
      case AFTER:
        if (!contribution.getContributionTypes().contains(ContributionType.DELETION)) {
          geomAfter = contribution.getGeometryUnclippedAfter();
          distanceInDegreeLongitude = Geo
              .convertMetricDistanceToDegreeLongitude(geomAfter.getCentroid().getX(),
                  distanceInMeter);
        } else {
          throw new Exception("Contribution of type DELETION. No geometry after contribution available.");
        }
        geomBefore = geomAfter;
        break;
      case BOTH:
        if (!contribution.getContributionTypes().contains(ContributionType.DELETION)) {
          geomAfter = contribution.getGeometryUnclippedAfter();
          distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geomAfter.getCentroid().getX(),distanceInMeter);
        }
        if (!contribution.getContributionTypes().contains(ContributionType.CREATION)) {
          geomBefore = contribution.getGeometryUnclippedBefore();
          distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geomBefore.getCentroid().getX(),distanceInMeter);
        }
        if (geomAfter == null) geomAfter = geomBefore;
        if (geomBefore == null) geomBefore = geomAfter;
        break;
    }

    // If both geometries are invalid, throw exception
    if (((geomAfter == null) && (geomBefore == null)) || (distanceInDegreeLongitude == null)) {
      throw new Exception("Invalid geometry");
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
            Geometry geomNeighbour = snapshot.getGeometryUnclipped();
            switch (geometryVersion) {
                case BEFORE: return Geo.isWithinDistance(finalGeomBefore, geomNeighbour, distanceInMeter);
                case AFTER: return Geo.isWithinDistance(finalGeomAfter, geomNeighbour, distanceInMeter);
                case BOTH: return Geo.isWithinDistance(finalGeomBefore, geomNeighbour, distanceInMeter) || Geo.isWithinDistance(finalGeomAfter, geomNeighbour, distanceInMeter);
                default: return Geo.isWithinDistance(finalGeomAfter, geomNeighbour, distanceInMeter);
              }
        });

    // Apply mapReducer given by user
    return mapReduce.apply(subMapReducer);
}


  /**
   * Checks if elements are located inside another object
   * @param snapshot
   * @param outerElements
   * @param <Y>
   * @return
   */
  public static <Y extends OSHDBMapReducible> Pair<OSMEntitySnapshot, List<Y>> inside(
      OSMEntitySnapshot snapshot,
      STRtree outerElements) {
    Geometry geom = snapshot.getGeometryUnclipped();
    List<Y> candidates = outerElements.query(snapshot.getGeometryUnclipped().getEnvelopeInternal());
    List<Y> result = new ArrayList<>();
    // No candidates are found return null
    if (candidates.size() == 0) return Pair.of(snapshot, result);
    // Check which of the candidates contain snapshot
    if (candidates.get(0).getClass() == OSMEntitySnapshot.class) {
      result = candidates
          .stream()
          .filter(candidate -> {
            try {
              OSMEntitySnapshot candidate1 = (OSMEntitySnapshot) candidate;
              Geometry geomCandidate = candidate1.getGeometryUnclipped();
              return geomCandidate.contains(geom);
            } catch (TopologyException e) {
              System.out.println(e);
              return false;
            }
          })
          .collect(Collectors.toList());
    } else if (candidates.get(0).getClass() == OSMContribution.class) {
      result = candidates
          .stream()
          .filter(candidate -> {
            try {
              OSMContribution candidate1 = (OSMContribution) candidate;
              Geometry geomCandidate = candidate1.getGeometryUnclippedAfter();
              return geomCandidate.contains(geom);
            } catch (TopologyException e) {
              System.out.println(e);
              return false;
            }
          })
          .collect(Collectors.toList());
    } else {
      throw new UnsupportedOperationException("Method 'inside' is not implemented for this class.");
    }
    return Pair.of(snapshot, result);
  }

}