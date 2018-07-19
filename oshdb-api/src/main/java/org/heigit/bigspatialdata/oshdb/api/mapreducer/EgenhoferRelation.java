package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
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

public class EgenhoferRelation {

  public enum relationType {
    EQUALS, OVERLAPS, DISJOINT, CONTAINS, COVEREDBY, COVERS, TOUCHES, INSIDE, UNKNOWN
  }
  private final STRtree candidateTree = new STRtree();

  public EgenhoferRelation(
      OSHDBJdbc oshdb,
      OSHDBBoundingBox bbox,
      OSHDBTimestampList tstamps,
      String key,
      String value,
      //SerializableFunctionWithException<MapReducer<X>, Y> mapReduce,
      boolean queryContributions) throws Exception {

    if (!queryContributions) {
      // Search for snapshots that might contain snapshot
      MapReducer<OSMEntitySnapshot> mapReduceCandidates = OSMEntitySnapshotView
          .on(oshdb)
          .keytables(oshdb)
          .areaOfInterest(bbox)
          .timestamps(tstamps);
      // Filter by key and value if given
      if (key != null & value != null) {
        mapReduceCandidates = mapReduceCandidates.osmTag(key, value);
      } else if (key != null & value == null) {
        mapReduceCandidates = mapReduceCandidates.osmTag(key);
      }
      // Store all elements in str tree
      mapReduceCandidates.collect()
          .forEach(snapshot -> this.candidateTree
              .insert(snapshot.getGeometryUnclipped().getEnvelopeInternal(), snapshot));
    } else {
      // Search for snapshots that might contain snapshot
      MapReducer<OSMContribution> mapReduceCandidates = OSMContributionView
          .on(oshdb)
          .keytables(oshdb)
          .areaOfInterest(bbox)
          .timestamps(tstamps);
      // Filter by key and value if given
      if (key != null & value != null) {
        mapReduceCandidates = mapReduceCandidates.osmTag(key, value);
      } else if (key != null & value == null) {
        mapReduceCandidates = mapReduceCandidates.osmTag(key);
      }
      // Store all elements in str tree
        mapReduceCandidates.collect()
          .forEach(contribution -> this.candidateTree
              .insert(contribution.getGeometryUnclippedAfter().getEnvelopeInternal(), contribution));
    }
  }

  /**
   * Checks whether a geometry lies within a distance from another geometry
   * The algorithm searches for the two closest points and calcualtes the distance between them.
   * @param geom1 Geometry 1
   * @param geom2 Geometry 2
   * @return True, if the geometry is within the distance of the other geometry, otherwise false
   */
  public static <X extends Geometry> relationType getRelation(X geom1, X geom2) {

    Geometry geom1converted = geom1;
    Geometry geom2converted = geom2;

    // Check if geometry is closed line string
    if (geom1 instanceof LineString) {
      if (((LineString) geom1).isClosed()) {
        geom1converted = new GeometryFactory().createPolygon(geom1.getCoordinates());
      }
    }
    if (geom2 instanceof LineString) {
      if (((LineString) geom2).isClosed()) {
        geom2converted = new GeometryFactory().createPolygon(geom2.getCoordinates());
      }
    }

    if (geom1converted instanceof Polygon & geom2converted instanceof Polygon) {
      // Get boundary and interior of both geometries
      Geometry boundary1 = geom1converted.getBoundary();
      Geometry boundary2 = geom2converted.getBoundary();

      if (geom1converted.equalsNorm(geom2converted)) {
        return relationType.EQUALS;
      } else if (geom1converted.touches(geom2converted)) {
        return relationType.TOUCHES;
      } else if (geom1converted.covers(geom2converted)) {
        if (!boundary1.touches(geom2converted)) {
          return relationType.CONTAINS;
        } else {
          return relationType.COVERS;
        }
      } else if (geom1converted.overlaps(geom2converted)) {
        return relationType.OVERLAPS;
      } else if (geom1converted.coveredBy(geom2converted) & boundary1.intersects(boundary2)) {
        return relationType.COVEREDBY;
      } else if (geom1converted.within(geom2converted) & !boundary1.intersects(boundary2)) {
        return relationType.INSIDE;
      } else if (geom1converted.disjoint(geom2converted)) {
        return relationType.DISJOINT;
      } else {
        return relationType.UNKNOWN;
      }

    } else if (geom1 instanceof Polygon && geom2converted instanceof LineString) {
      throw new UnsupportedOperationException("Not implemented yet.");
    } else if (geom1 instanceof Polygon && geom2converted instanceof Point) {
      throw new UnsupportedOperationException("Not implemented yet.");
    } else if (geom1 instanceof LineString && geom2converted instanceof Polygon) {
      throw new UnsupportedOperationException("Not implemented yet.");
    } else if (geom1 instanceof LineString && geom2converted instanceof LineString) {
      if (geom1.equalsNorm(geom2converted)) {
        return relationType.EQUALS;
      } else if (geom1converted.contains(geom2converted) & !geom1converted.equalsNorm(geom2converted)) {
        return relationType.CONTAINS;
      } else if (geom1converted.coveredBy(geom2converted)) {
        return relationType.COVEREDBY;
      } else if (geom1converted.touches(geom2converted)) {
        return relationType.TOUCHES;
      } else if (geom1converted.intersects(geom2converted) & !geom1converted.touches(geom2converted)) {
        return relationType.OVERLAPS;
      } else if (!geom1converted.intersects(geom2converted) & !geom1converted.equalsNorm(geom2converted)) {
        return relationType.DISJOINT;
      } else {
        return relationType.UNKNOWN;
      }
    } else if (geom1converted instanceof LineString && geom2converted instanceof Point) {
      throw new UnsupportedOperationException("Not implemented yet.");
    } else if (geom1converted instanceof Point && geom2converted instanceof Polygon) {
      throw new UnsupportedOperationException("Not implemented yet.");
    } else if (geom1converted instanceof Point  && geom2converted instanceof LineString) {
      throw new UnsupportedOperationException("Not implemented yet.");
    } else if (geom1converted instanceof Point  && geom2converted instanceof Point ) {
      throw new UnsupportedOperationException("Not implemented yet.");
    } else {
      System.out.println("Unknown geometry type.");
    }
    return relationType.UNKNOWN;
  }

  /**
   * Filter objects based on type of spatial relation to snapshot
   * @param <Y>
   * @oaram snapshot OSMEntitySnapshot for which other objects should be checked for relation type
   * @param relationType Type of egenhofer relation
   * @return
   */
  private <Y> Pair<OSMEntitySnapshot, List<Y>> filter(
      OSMEntitySnapshot snapshot,
      relationType relationType) {

      // Get geometry of snapshot
      Geometry geom = snapshot.getGeometryUnclipped();
      List<Y> result = new ArrayList<>();

      // Get candidate objects in the neighbourhood of snapshot
      List<Y> candidates = this.candidateTree.query(snapshot.getGeometryUnclipped().getEnvelopeInternal());

      // If no candidates are found, return empty list
      if (candidates.size() == 0) return Pair.of(snapshot, result);

      // Check the spatial relation of each candidate to the snapshot
      if (candidates.get(0).getClass() == OSMEntitySnapshot.class) {
        result = candidates
            .stream()
            .filter(candidate -> {
              try {
                OSMEntitySnapshot candidateSnapshot = (OSMEntitySnapshot) candidate;
                // Skip if this candidate snapshot belongs to the same entity
                if (snapshot.getEntity().getId() == candidateSnapshot.getEntity().getId()) return false;
                // Skip if it is a relation
                if (candidateSnapshot.getEntity().getType().equals(OSMType.RELATION)) return false;
                // Get geometry of candidate and filter based on relationType
                Geometry candidateGeom = candidateSnapshot.getGeometryUnclipped();
                return getRelation(geom, candidateGeom).equals(relationType);
              } catch (TopologyException | IllegalArgumentException e) {
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
                OSMContribution candidateContribution = (OSMContribution) candidate;
                // Skip if this candidate snapshot belongs to the same entity
                if (snapshot.getEntity().getId() == candidateContribution.getEntityAfter().getId()) return false;
                // Skip if it is a relation
                if (candidateContribution.getEntityAfter().getType().equals(OSMType.RELATION)) return false;
                // Get geometry of candidate and filter based on relationType
                // todo add option to choose whether geometry before or after should be used as reference
                Geometry candidateGeom;
                if (!candidateContribution.getContributionTypes().contains(ContributionType.DELETION)) {
                  candidateGeom = candidateContribution.getGeometryUnclippedAfter();
                } else {
                  candidateGeom = candidateContribution.getGeometryUnclippedBefore();
                }
                return getRelation(geom, candidateGeom).equals(relationType);
              } catch (TopologyException | IllegalArgumentException e) {
                System.out.println(e);
                return false;
              }
            })
            .collect(Collectors.toList());
      } else {
        throw new UnsupportedOperationException("Method is not implemented for this class.");
      }
      return Pair.of(snapshot, result);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<OSMEntitySnapshot, List<Y>> overlaps(OSMEntitySnapshot snapshot) {
    return this.filter( snapshot, relationType.OVERLAPS);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<OSMEntitySnapshot, List<Y>> equals(OSMEntitySnapshot snapshot) {
    return this.filter(snapshot, relationType.EQUALS);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  // todo does not work for disjoint
  public <Y> Pair<OSMEntitySnapshot, List<Y>> disjoint(OSMEntitySnapshot snapshot) {
    return this.filter(snapshot, relationType.DISJOINT);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<OSMEntitySnapshot, List<Y>> touches(OSMEntitySnapshot snapshot) {
    return this.filter(snapshot, relationType.TOUCHES);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<OSMEntitySnapshot, List<Y>> contains(OSMEntitySnapshot snapshot) {
    return this.filter(snapshot, relationType.CONTAINS);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<OSMEntitySnapshot, List<Y>> inside(OSMEntitySnapshot snapshot) {
    return this.filter(snapshot, relationType.INSIDE);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<OSMEntitySnapshot, List<Y>> covers(OSMEntitySnapshot snapshot) {
    return this.filter(snapshot, relationType.COVERS);
  }

  /**
   * Checks if elements are located inside another object
   * @param <Y>
   * @return
   */
  public <Y> Pair<OSMEntitySnapshot, List<Y>> coveredBy(OSMEntitySnapshot snapshot) {
    return this.filter(snapshot, relationType.COVEREDBY);
  }

}
