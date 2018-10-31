package org.heigit.bigspatialdata.oshdb.api.mapreducer;

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
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;

/**
 * Class that computes the DE9IM relations between two geometries
 */
public class DE9IM<X> {

  public enum relationType {
    EQUALS, OVERLAPS, DISJOINT, CONTAINS, COVEREDBY, COVERS, TOUCHES, INSIDE, UNKNOWN
  }

  private final STRtree featuresToCompareTree = new STRtree();
  private OSHDBBoundingBox bbox;
  private OSHDBJdbc oshdb;
  private OSHDBTimestampList tstamps;

  /**
   * basic constructor
   * @param oshdb osdhb connection
   * @param bbox bounding box for querying nearby features to compare with
   * @param tstamps timestamps for querying nearby features to compare with
   * @param key OSM key of objects that should be queried
   * @param value OSM value of objects that should be queried
   */
  // todo replace key and value with mapReduce function
  DE9IM(
    OSHDBJdbc oshdb,
    OSHDBBoundingBox bbox,
    OSHDBTimestampList tstamps,
    String key,
    String value,
    // SerializableFunctionWithException<MapReducer<X>, Y> mapReduce,
    boolean queryContributions) throws Exception {

    this.oshdb = oshdb;
    this.bbox = bbox;
    this.tstamps = tstamps;

    // todo: check how big the data will be to decide whether storing data in a local strtree is possible
    createSTRtree(key, value, queryContributions);

  }

  /**
   * Creates a STRtree that contains all features to which the main features should be compared to.
   *
   * The oshdb conntection, bbox and tstamps are taken from the main MapReducer. Optionally a filter
   * is applied using the key and value tags.
   *
   * @param key OSM key of objects that should be queried
   * @param value OSM value of objects that should be queried
   * @param queryContributions Query OSMcontributions (true) or OSMEntitySnapshots (false)
   */
  public void createSTRtree(
      String key,
      String value,
      boolean queryContributions) throws Exception {
    if (!queryContributions) {
      MapReducer<OSMEntitySnapshot> featuresToCompare = OSMEntitySnapshotView
          .on(this.oshdb)
          .keytables(this.oshdb)
          .areaOfInterest(this.bbox)
          .timestamps(this.tstamps);
      // Filter by key and value if given
      if (key != null & value != null) {
        featuresToCompare = featuresToCompare.osmTag(key, value);
      } else if (key != null & value == null) {
        featuresToCompare = featuresToCompare.osmTag(key);
      }
      featuresToCompare.collect()
          .forEach(snapshot -> this.featuresToCompareTree
              .insert(snapshot.getGeometryUnclipped().getEnvelopeInternal(), snapshot));
    } else {
      MapReducer<OSMContribution> featuresToCompare = OSMContributionView
          .on(this.oshdb)
          .keytables(this.oshdb)
          .areaOfInterest(this.bbox)
          .timestamps(this.tstamps);
      // Filter by key and value of features used for comparison
      if (key != null & value != null) {
        featuresToCompare = featuresToCompare.osmTag(key, value);
      } else if (key != null & value == null) {
        featuresToCompare = featuresToCompare.osmTag(key);
      }
      featuresToCompare.collect()
          .forEach(contribution -> {
            try{
              featuresToCompareTree
                  .insert(contribution.getGeometryUnclippedAfter().getEnvelopeInternal(),
                      contribution);
            } catch(Exception e) {
              featuresToCompareTree
                  .insert(contribution.getGeometryUnclippedBefore().getEnvelopeInternal(),
                      contribution);
            }
          });
    }
  }

  /**
   * Returns the DE9IM relation type between two geometries
   *
   * Important note: COVERS and TOUCHES are only recognized, if the geometries share a
   * vertex/node, not if they touch at a segment.
   *
   * @param geom1 Geometry 1
   * @param geom2 Geometry 2
   * @return True, if the geometry is within the distance of the other geometry, otherwise false
   */
  public static <S extends Geometry> relationType relate(S geom1, S geom2) {

    // Variables that hold transformed geometry which is either LineString or Polygon
    // todo: is this necessary or does getGeometry() already return a Polygon if the linestring is closed?
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
   * @oaram oshdbMapReducible main feature for which matches should be found
   * @param relationType Type of Egenhofer relation
   * @param <Y> Type of objects to compare to. Either OSMContribution or OSMEntitySnapshot.
   * @return
   */
  private <Y> Pair<X, List<Y>> findMatches(
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
    // todo: findMatches out candidates that were not present at the same time as snapshot

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
              // Get geometry of candidate and findMatches based on relationType
              Geometry candidateGeom = candidateSnapshot.getGeometryUnclipped();
              return relate(finalGeom, candidateGeom).equals(relationType);
            } catch (TopologyException | IllegalArgumentException e) {
              System.out.println(e);
              return false;
            }
          })
          .collect(Collectors.toList());

      // If disjoint, add all objects that are outside the bounding box of snapshot
      if (relationType.equals(DE9IM.relationType.DISJOINT)) {
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
              // Get geometry of candidate and findMatches based on relationType
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
      if (relationType.equals(DE9IM.relationType.DISJOINT)) {
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
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<X, List<Y>> overlaps(X oshdbMapReducible) {
    return this.findMatches(oshdbMapReducible, relationType.OVERLAPS);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  // todo: solve issue with overriding Object.equals() --> renamed to equalTo for now
  public <Y> Pair<X, List<Y>> equalTo(X oshdbMapReducible) {
    return this.findMatches(oshdbMapReducible, relationType.EQUALS);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  // todo does not work for disjoint
  public <Y> Pair<X, List<Y>> disjoint(X oshdbMapReducible) {
    return this.findMatches(oshdbMapReducible, relationType.DISJOINT);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<X, List<Y>> touches(X oshdbMapReducible) {
    return this.findMatches(oshdbMapReducible, relationType.TOUCHES);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<X, List<Y>> contains(X oshdbMapReducible) {
    return this.findMatches(oshdbMapReducible, relationType.CONTAINS);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<X, List<Y>> inside(X oshdbMapReducible) {
    return this.findMatches(oshdbMapReducible, relationType.INSIDE);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<X, List<Y>> covers(X oshdbMapReducible) {
    return this.findMatches(oshdbMapReducible, relationType.COVERS);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<X, List<Y>> coveredBy(X oshdbMapReducible) {
    return this.findMatches(oshdbMapReducible, relationType.COVEREDBY);
  }

}
