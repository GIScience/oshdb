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
 *
 */
public class DE9IM {

  public enum relationType {
    EQUALS, OVERLAPS, DISJOINT, CONTAINS, COVEREDBY, COVERS, TOUCHES, INSIDE, UNKNOWN
  }
  private final STRtree candidateTree = new STRtree();
  private OSHDBBoundingBox bbox;

  /**
   * basic constructor
   * @param oshdb OSHDB connection
   * @param bbox Bounding box for query
   * @param tstamps Timestamps of query
   * @param key OSM key of objects that should be queried
   * @param value OSM value of objects that should be queried
   */
  // todo replace key and value with mapReduce function
  public DE9IM(
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
      // Search for contributions that might contain snapshot
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
          .forEach(contribution -> {
            if (!contribution.getContributionTypes().contains(ContributionType.DELETION)) {
              candidateTree
                  .insert(contribution.getGeometryUnclippedAfter().getEnvelopeInternal(), contribution);
            } else {
              candidateTree
                  .insert(contribution.getGeometryUnclippedBefore().getEnvelopeInternal(), contribution);
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
  public static <X extends Geometry> relationType relate(X geom1, X geom2) {

    Geometry geom11 = geom1;
    Geometry geom21 = geom2;

    // Check if geometry is closed line string
    if (geom1 instanceof LineString) {
      if (((LineString) geom1).isClosed()) {
        geom11 = new GeometryFactory().createPolygon(geom1.getCoordinates());
      }
    }
    if (geom2 instanceof LineString) {
      if (((LineString) geom2).isClosed()) {
        geom21 = new GeometryFactory().createPolygon(geom2.getCoordinates());
      }
    }

    // Get boundaries of geometries
    Geometry boundary2 = geom21.getBoundary();
    Geometry boundary1 = geom11.getBoundary();

    if (geom1.disjoint(geom21)) {
      return relationType.DISJOINT;

    } else if (geom11.getDimension() == geom21.getDimension()) {
      if (geom1.equalsNorm(geom21)) {
        return relationType.EQUALS;
      } else if (geom11.touches(geom21)) {
        return relationType.TOUCHES;
      } else if (geom11.contains(geom21) & !boundary1.intersects(boundary2)) {
        return relationType.CONTAINS;
      } else if (geom11.covers(geom21) & boundary1.intersects(boundary2)) {
        return relationType.COVERS;
      } else if (geom11.coveredBy(geom21) & boundary1.intersects(boundary2)) {
        return relationType.COVEREDBY;
      } else if (geom11.overlaps(geom21) || (geom11.intersects(geom21) && !geom11.within(geom21))) {
        return relationType.OVERLAPS;
      } else if (geom11.within(geom21) & !boundary1.intersects(boundary2)) {
        return relationType.INSIDE;
      }

    } else if (geom11.getDimension() < geom21.getDimension()) {

      if (geom11.touches(geom21)) {
        return relationType.TOUCHES;
      } else if (geom11.coveredBy(geom21) & boundary1.intersects(boundary2)) {
        return relationType.COVEREDBY;
      } else if (geom11.within(geom21) & !boundary1.intersects(boundary2)) {
        return relationType.INSIDE;
      } else if (geom11.intersects(geom21)) {
        return relationType.OVERLAPS;
      }

    } else if (geom11.getDimension() > geom21.getDimension()) {

      if (geom11.touches(geom21)) {
        return relationType.TOUCHES;
      } else if (geom11.contains(geom21) & !boundary1.intersects(boundary2)) {
        return relationType.CONTAINS;
      } else if (geom11.covers(geom21) & boundary1.intersects(boundary2)) {
        return relationType.COVERS;
      } else if (geom11.contains(geom21) & !boundary1.intersects(geom21)) {
        return relationType.CONTAINS;
      } else if (geom11.intersects(geom21)) {
        return relationType.OVERLAPS;
      }
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
    // todo: filter out candidates that were not present at the same time as snapshot

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
              return relate(geom, candidateGeom).equals(relationType);
            } catch (TopologyException | IllegalArgumentException e) {
              System.out.println(e);
              return false;
            }
          })
          .collect(Collectors.toList());

      // If disjoint, add all objects that are outside the bounding box of snapshot
      if (relationType.equals(DE9IM.relationType.DISJOINT)) {
        STRtree disjointObjects = new STRtree();
        candidates.stream().forEach(x -> disjointObjects.remove(
                ((OSMEntitySnapshot) x).getGeometryUnclipped().getEnvelopeInternal(), x));
        List<Y> disjointObjectList = this.candidateTree.query(new Envelope(this.bbox.getMinLon(),
            this.bbox.getMaxLon(), this.bbox.getMinLat(), this.bbox.getMaxLat()));
        result.addAll(disjointObjectList);
        return Pair.of(snapshot, result);
      }

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
              return relate(geom, candidateGeom).equals(relationType);
            } catch (TopologyException | IllegalArgumentException e) {
              System.out.println(e);
              return false;
            }
          })
          .collect(Collectors.toList());

      // If disjoint, add all objects that are outside the bounding box of snapshot
      if (relationType.equals(DE9IM.relationType.DISJOINT)) {
        STRtree disjointObjects = this.candidateTree;
        candidates.stream().forEach(contribution -> {
          if (!((OSMContribution) contribution).getContributionTypes().contains(ContributionType.DELETION)) {
            disjointObjects.remove(((OSMContribution) contribution).getGeometryUnclippedAfter().getEnvelopeInternal(), contribution);
          } else {
            disjointObjects.remove(((OSMContribution) contribution).getGeometryUnclippedBefore().getEnvelopeInternal(), contribution);
          }
        });
        List<Y> disjointObjectList = this.candidateTree.query(new Envelope(this.bbox.getMinLon(),
            this.bbox.getMaxLon(), this.bbox.getMinLat(), this.bbox.getMaxLat()));
        result.addAll(disjointObjectList);
        return Pair.of(snapshot, result);
      }

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
    return this.filter(snapshot, relationType.OVERLAPS);
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
