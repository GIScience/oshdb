package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import static org.heigit.bigspatialdata.oshdb.util.geometry.Geo.isWithinDistance;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.TopologyException;
import com.vividsolutions.jts.index.strtree.STRtree;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunctionWithException;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;

/**
 * Class that computes the SpatialRelations relations between two geometries
 */
public class SpatialRelations<X,Y> {

  public enum relationType {
    EQUALS, OVERLAPS, DISJOINT, CONTAINS, COVEREDBY, COVERS, TOUCHES, INSIDE, UNKNOWN
  }

  private final STRtree featuresToCompareTree = new STRtree();
  private OSHDBBoundingBox bbox;
  private OSHDBJdbc oshdb;
  private OSHDBTimestampList tstamps;

  /**
   * Basic constructor
   *
   * @param oshdb osdhb connection
   * @param bbox bounding box for querying nearby features to compare with
   * @param tstamps timestamps for querying nearby features to compare with
   * @param mapReduce MapReduce function that specifies features that are used for comparison
   */
  public SpatialRelations(
    OSHDBJdbc oshdb,
    OSHDBBoundingBox bbox,
    OSHDBTimestampList tstamps,
    SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce,
    boolean queryContributions) throws Exception {

    this.oshdb = oshdb;
    this.bbox = bbox;
    this.tstamps = tstamps;

    // todo: estimate how big the data will be to decide whether storing data in a local strtree is possible
    createSTRtree(mapReduce, queryContributions);
  }

  /**
   * Creates a STRtree that contains all features to which the main features should be compared to.
   *
   * The oshdb conntection, bbox and tstamps are taken from the main MapReducer. Optionally a filter
   * is applied using the key and value tags.
   *
   * @param mapReduce MapReduce function that specifies features that are used for comparison
   * @param queryContributions Query OSMcontributions (true) or OSMEntitySnapshots (false)
   */
  public void createSTRtree(
      SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce,
      boolean queryContributions) throws Exception {

    if (!queryContributions) {
      MapReducer<OSMEntitySnapshot> mapReducer = OSMEntitySnapshotView
          .on(this.oshdb)
          .keytables(this.oshdb)
          .areaOfInterest(this.bbox)
          .timestamps(this.tstamps);
      // Apply mapReducer given by user
      List<Y> featuresToCompare;
      if (mapReduce != null) {
        featuresToCompare = mapReduce.apply( (MapReducer<Y>) mapReducer);
      } else {
        featuresToCompare = (List<Y>) mapReducer.collect();
      }
      // Store features in STRtree
      featuresToCompare.forEach(snapshot -> this.featuresToCompareTree
              .insert(((OSMEntitySnapshot) snapshot).getGeometryUnclipped().getEnvelopeInternal(), snapshot));
    } else {
      MapReducer<OSMContribution> featuresToCompare = OSMContributionView
          .on(this.oshdb)
          .keytables(this.oshdb)
          .areaOfInterest(this.bbox)
          .timestamps(this.tstamps);
      // Apply mapReducer given by user
      List<Y> result;
      if (mapReduce != null) {
        result = mapReduce.apply( (MapReducer<Y>) featuresToCompare);
      } else {
        result = (List<Y>) featuresToCompare.collect();
      }
      // Store features in STRtree
      result.forEach(contribution -> {
            try{
              featuresToCompareTree
                  .insert(((OSMContribution) contribution).getGeometryUnclippedAfter().getEnvelopeInternal(),
                      contribution);
            } catch(Exception e) {
              featuresToCompareTree
                  .insert(((OSMContribution) contribution).getGeometryUnclippedBefore().getEnvelopeInternal(),
                      contribution);
            }
          });
    }
  }

  /**
   * Returns the type of spatial relation between two geometries
   *
   * Important note: COVERS and TOUCHES are only recognized, if the geometries share a common
   * vertex/node, not if they touch at a segment.
   *
   * @param geom1 Geometry 1
   * @param geom2 Geometry 2
   * @return True, if the geometry is within the distance of the other geometry, otherwise false
   */
  public static <S extends Geometry> relationType relate(S geom1, S geom2) {

    // Variables that hold transformed geometry which is either LineString or Polygon
    // todo: is this necessary or does getGeometry() already return a Polygon if the linestring is closed?
    // todo: implement touches with buffer
    Geometry geomTrans1 = geom1;
    Geometry geomTrans2 = geom2;

    // Check if geometry is closed line string
    if (geom1 instanceof LineString) {
      if (((LineString) geom1).isClosed()) {
        geomTrans1 = new GeometryFactory().createPolygon(geom1.getCoordinates());
      }
    }
    if (geom2 instanceof LineString) {
      if (((LineString) geom2).isClosed()) {
        geomTrans2 = new GeometryFactory().createPolygon(geom2.getCoordinates());
      }
    }
    // Get boundaries of geometries
    Geometry boundary2 = geomTrans2.getBoundary();
    Geometry boundary1 = geomTrans1.getBoundary();

    if (geom1.disjoint(geomTrans2)) {
      return relationType.DISJOINT;
    } else if (geomTrans1.getDimension() == geomTrans2.getDimension()) {
      if (geom1.equalsNorm(geomTrans2)) {
        return relationType.EQUALS;
      } else if (geomTrans1.touches(geomTrans2)) {
        return relationType.TOUCHES;
      } else if (geomTrans1.contains(geomTrans2) & !boundary1.intersects(boundary2)) {
        return relationType.CONTAINS;
      } else if (geomTrans1.covers(geomTrans2) & boundary1.intersects(boundary2)) {
        return relationType.COVERS;
      } else if (geomTrans1.coveredBy(geomTrans2) & boundary1.intersects(boundary2)) {
        return relationType.COVEREDBY;
      } else if (geomTrans1.overlaps(geomTrans2) || (geomTrans1.intersects(geomTrans2) && !geomTrans1.within(geomTrans2))) {
        return relationType.OVERLAPS;
      } else if (geomTrans1.within(geomTrans2) & !boundary1.intersects(boundary2)) {
        return relationType.INSIDE;
      }
    } else if (geomTrans1.getDimension() < geomTrans2.getDimension()) {
      if (geomTrans1.touches(geomTrans2)) {
        return relationType.TOUCHES;
      } else if (geomTrans1.coveredBy(geomTrans2) & boundary1.intersects(boundary2)) {
        return relationType.COVEREDBY;
      } else if (geomTrans1.within(geomTrans2) & !boundary1.intersects(boundary2)) {
        return relationType.INSIDE;
      } else if (geomTrans1.intersects(geomTrans2)) {
        return relationType.OVERLAPS;
      }
    } else if (geomTrans1.getDimension() > geomTrans2.getDimension()) {
      if (geomTrans1.touches(geomTrans2)) {
        return relationType.TOUCHES;
      } else if (geomTrans1.contains(geomTrans2) & !boundary1.intersects(boundary2)) {
        return relationType.CONTAINS;
      } else if (geomTrans1.covers(geomTrans2) & boundary1.intersects(boundary2)) {
        return relationType.COVERS;
      } else if (geomTrans1.contains(geomTrans2) & !boundary1.intersects(geomTrans2)) {
        return relationType.CONTAINS;
      } else if (geomTrans1.intersects(geomTrans2)) {
        return relationType.OVERLAPS;
      }
    }
    return relationType.UNKNOWN;
  }

  /**
   * Finds matching objects based on the type of spatial relation
   *
   * @param oshdbMapReducible main feature for which matches should be found
   * @param relationType Type of Egenhofer relation
   * @return
   */
  private Pair<X, List<Y>> match(
    X oshdbMapReducible,
    relationType relationType) {

    Geometry geom;
    Long id;
    List<Y> result = new ArrayList<>();

    // Get geometry of snapshot or contribution
    if (oshdbMapReducible.getClass() == OSMEntitySnapshot.class) {
      geom = ((OSMEntitySnapshot) oshdbMapReducible).getGeometryUnclipped();
      id = ((OSMEntitySnapshot) oshdbMapReducible).getEntity().getId();
    } else {
      try {
        geom = ((OSMContribution) oshdbMapReducible).getGeometryUnclippedAfter();
        id = ((OSMContribution) oshdbMapReducible).getEntityAfter().getId();
      } catch (Exception e) {
        geom = ((OSMContribution) oshdbMapReducible).getGeometryUnclippedBefore();
        id = ((OSMContribution) oshdbMapReducible).getEntityBefore().getId();
      }
    }

    // Get candidate objects in the neighbourhood of snapshot
    List<Y> nearbyFeaturesToCompare = this.featuresToCompareTree.query(geom.getEnvelopeInternal());
    // todo: match out candidates that were not present at the same time as snapshot

    // If no nearby features are found, return empty list
    if (nearbyFeaturesToCompare.isEmpty()) return Pair.of(oshdbMapReducible, result);

    // Get nearby features that have the required Egenhofer relation with the main feature
    if (nearbyFeaturesToCompare.get(0).getClass() == OSMEntitySnapshot.class) {
      Long finalId = id;
      Geometry finalGeom = geom;
      result = nearbyFeaturesToCompare
          .stream()
          .filter(candidate -> {
            try {
              OSMEntitySnapshot candidateSnapshot = (OSMEntitySnapshot) candidate;
              // Skip if this candidate snapshot belongs to the same entity
              if (finalId == candidateSnapshot.getEntity().getId()) return false;
              // Skip if it is a relation
              // todo check functionality for relations
              if (candidateSnapshot.getEntity().getType().equals(OSMType.RELATION)) return false;
              // Get geometry of candidate and match based on relationType
              Geometry candidateGeom = candidateSnapshot.getGeometryUnclipped();
              return relate(finalGeom, candidateGeom).equals(relationType);
            } catch (TopologyException | IllegalArgumentException e) {
              System.out.println(e);
              return false;
            }
          })
          .collect(Collectors.toList());

      // If disjoint, add all objects that are outside the bounding box of snapshot
      if (relationType.equals(SpatialRelations.relationType.DISJOINT)) {
        STRtree disjointObjects = new STRtree();
        nearbyFeaturesToCompare.stream().forEach(x -> disjointObjects.remove(
                ((OSMEntitySnapshot) x).getGeometryUnclipped().getEnvelopeInternal(), x));
        List<Y> disjointObjectList = this.featuresToCompareTree.query(new Envelope(this.bbox.getMinLon(),
            this.bbox.getMaxLon(), this.bbox.getMinLat(), this.bbox.getMaxLat()));
        result.addAll(disjointObjectList);
        return Pair.of(oshdbMapReducible, result);
      }

    } else if (nearbyFeaturesToCompare.get(0).getClass() == OSMContribution.class) {
      Long finalId1 = id;
      Geometry finalGeom1 = geom;
      result = nearbyFeaturesToCompare
          .stream()
          .filter(candidate -> {
            try {
              OSMContribution candidateContribution = (OSMContribution) candidate;
              // Skip if this candidate snapshot belongs to the same entity
              if (finalId1 == candidateContribution.getEntityAfter().getId()) return false;
              // Skip if it is a relation
              if (candidateContribution.getEntityAfter().getType().equals(OSMType.RELATION)) return false;
              // Get geometry of candidate and match based on relationType
              Geometry candidateGeom;
              try {
                candidateGeom = candidateContribution.getGeometryUnclippedAfter();
              } catch(Exception e) {
                candidateGeom = candidateContribution.getGeometryUnclippedBefore();
              }
              return relate(finalGeom1, candidateGeom).equals(relationType);
            } catch (TopologyException | IllegalArgumentException e) {
              System.out.println(e);
              return false;
            }
          })
          .collect(Collectors.toList());

      // If disjoint, add all objects that are outside the bounding box of snapshot
      if (relationType.equals(SpatialRelations.relationType.DISJOINT)) {
        STRtree disjointObjects = this.featuresToCompareTree;
        nearbyFeaturesToCompare.forEach(contribution -> {
          if (!((OSMContribution) contribution).getContributionTypes().contains(ContributionType.DELETION)) {
            disjointObjects.remove(((OSMContribution) contribution).getGeometryUnclippedAfter().getEnvelopeInternal(), contribution);
          } else {
            disjointObjects.remove(((OSMContribution) contribution).getGeometryUnclippedBefore().getEnvelopeInternal(), contribution);
          }
        });
        List<Y> disjointObjectList = this.featuresToCompareTree.query(new Envelope(this.bbox.getMinLon(),
            this.bbox.getMaxLon(), this.bbox.getMinLat(), this.bbox.getMaxLat()));
        result.addAll(disjointObjectList);
        return Pair.of(oshdbMapReducible, result);
      }

    } else {
      throw new UnsupportedOperationException("Method is not implemented for this class.");
    }
    return Pair.of(oshdbMapReducible, result);
  }

  /**
   * Checks if feature is within the neighbourhood of the other
   * @return
   */
  public Pair<X, List<Y>> neighbourhood(X oshdbMapReducible, Double distanceInMeter) {

    Geometry geom;
    Long id;
    List<Y> result = new ArrayList<>();

    // Get geometry of snapshot or contribution
    if (oshdbMapReducible.getClass() == OSMEntitySnapshot.class) {
      geom = ((OSMEntitySnapshot) oshdbMapReducible).getGeometryUnclipped();
      id = ((OSMEntitySnapshot) oshdbMapReducible).getEntity().getId();
    } else {
      try {
        geom = ((OSMContribution) oshdbMapReducible).getGeometryUnclippedAfter();
        id = ((OSMContribution) oshdbMapReducible).getEntityAfter().getId();
      } catch (Exception e) {
        geom = ((OSMContribution) oshdbMapReducible).getGeometryUnclippedBefore();
        id = ((OSMContribution) oshdbMapReducible).getEntityBefore().getId();
      }
    }

    // Convert distanceInMeters to degree longitude for bounding box of second mapreducer
    double distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geom.getCentroid().getY(), distanceInMeter);

    // Get coordinates of bounding box
    Envelope envelope = geom.getEnvelopeInternal();
    // Multiply by 1.2 to avoid excluding possible candidates
    double minLon = envelope.getMinX() - distanceInDegreeLongitude * 1.2;
    double maxLon = envelope.getMaxX() + distanceInDegreeLongitude * 1.2;
    double minLat = envelope.getMinY() - (distanceInMeter / Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR) * 1.2;
    double maxLat = envelope.getMaxY() + (distanceInMeter / Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR) * 1.2;

    Envelope env = new Envelope(minLon, maxLon, minLat, maxLat);

    // Get candidate objects in the neighbourhood of snapshot
    List<Y> nearbyFeaturesToCompare = this.featuresToCompareTree.query(env);
    // todo: match out candidates that were not present at the same time as snapshot

    // If no nearby features are found, return empty list
    if (nearbyFeaturesToCompare.isEmpty()) return Pair.of(oshdbMapReducible, result);

    // Get nearby features that have the required Egenhofer relation with the main feature
    if (nearbyFeaturesToCompare.get(0).getClass() == OSMEntitySnapshot.class) {
      Long finalId = id;
      Geometry finalGeom = geom;
      result = nearbyFeaturesToCompare
          .stream()
          .filter(candidate -> {
            try {
              OSMEntitySnapshot candidateSnapshot = (OSMEntitySnapshot) candidate;
              // Skip if this candidate snapshot belongs to the same entity
              if (finalId == candidateSnapshot.getEntity().getId()) return false;
              // todo check functionality for relations
              if (candidateSnapshot.getEntity().getType().equals(OSMType.RELATION)) return false;
              Geometry candidateGeom = candidateSnapshot.getGeometryUnclipped();
              return isWithinDistance(finalGeom, candidateGeom, distanceInMeter) && relate(finalGeom, candidateGeom) == relationType.DISJOINT;
            } catch (Exception e) {
              System.out.println(e);
              return false;
            }
          })
          .collect(Collectors.toList());
    } else if (nearbyFeaturesToCompare.get(0).getClass() == OSMContribution.class) {
      Long finalId = id;
      Geometry finalGeom = geom;
      result = nearbyFeaturesToCompare
          .stream()
          .filter(candidate -> {
            try {
              OSMContribution candidateContribution = (OSMContribution) candidate;
              // Skip if this candidate snapshot belongs to the same entity
              if (finalId == candidateContribution.getEntityAfter().getId()) return false;
              // todo check functionality for relations
              if (candidateContribution.getEntityAfter().getType().equals(OSMType.RELATION)) return false;
              // Filter by contribution type if given ---> not implemented
              //if (contributionType != null && !candidateContribution.getContributionTypes().contains(contributionType)) return false;
              // Get geometry of candidate and match based on relationType
              Geometry candidateGeom;
              try {
                candidateGeom = candidateContribution.getGeometryUnclippedAfter();
              } catch(Exception e) {
                candidateGeom = candidateContribution.getGeometryUnclippedBefore();
              }
              return isWithinDistance(finalGeom, candidateGeom, distanceInMeter) && relate(finalGeom, candidateGeom) == relationType.DISJOINT;
            } catch (Exception e) {
              System.out.println(e);
              return false;
            }
          })
          .collect(Collectors.toList());
    } else {
      throw new UnsupportedOperationException("Method is not implemented for this class.");
    }
    return Pair.of(oshdbMapReducible, result);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> overlaps(X oshdbMapReducible) {
    return this.match(oshdbMapReducible, relationType.OVERLAPS);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  // todo: solve issue with overriding Object.equals() --> renamed to equalTo for now
  public Pair<X, List<Y>> equalTo(X oshdbMapReducible) {
    return this.match(oshdbMapReducible, relationType.EQUALS);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  // todo does not work for disjoint
  public Pair<X, List<Y>> disjoint(X oshdbMapReducible) {
    return this.match(oshdbMapReducible, relationType.DISJOINT);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> touches(X oshdbMapReducible) {
    return this.match(oshdbMapReducible, relationType.TOUCHES);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> contains(X oshdbMapReducible) {
    return this.match(oshdbMapReducible, relationType.CONTAINS);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> inside(X oshdbMapReducible) {
    return this.match(oshdbMapReducible, relationType.INSIDE);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> covers(X oshdbMapReducible) {
    return this.match(oshdbMapReducible, relationType.COVERS);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> coveredBy(X oshdbMapReducible) {
    return this.match(oshdbMapReducible, relationType.COVEREDBY);
  }

}
