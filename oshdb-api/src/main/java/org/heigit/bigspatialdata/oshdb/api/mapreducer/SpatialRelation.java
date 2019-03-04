package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import static org.heigit.bigspatialdata.oshdb.util.geometry.Geo.isWithinDistance;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableThrowingFunction;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer.Pair;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.index.strtree.STRtree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that selects OSM objects based on their spatial relation (as defined by Egenhofer, 1991)
 * and neighbourhood to other nearby OSM objects.
 *
 * @param <X> type of OSHDB object (OSMEntitySnapshot or OSMContribution) which is selected based
 *            on comparison
 *
 */
public class SpatialRelation<X> {
  private static final Logger LOG = LoggerFactory.getLogger(SpatialRelation.class);

  public enum Relation {
    EQUALS,
    OVERLAPS,
    DISJOINT,
    CONTAINS,
    COVEREDBY,
    COVERS,
    TOUCHES,
    INSIDE,
    UNKNOWN,
    NEIGHBOURING,
    INTERSECTS
  }

  private MapReducer mapReducer;
  private SerializableThrowingFunction<MapReducer<OSMEntitySnapshot>, List<OSMEntitySnapshot>>
      mapReduce;
  private final STRtree objectsForComparison = new STRtree();

  /**
   * Basic constructor.
   *
   * <p>The oshdb conntection, bbox and tstamps are taken from the main MapReducer.</p>
   *
   * @param mapReducer MapReducer instance of the objects to select based on spatial relations
   * @param mapReduce MapReduce function that specifies features that are used for comparison
   */
  public SpatialRelation(
      MapReducer mapReducer,
      SerializableThrowingFunction<MapReducer<OSMEntitySnapshot>, List<OSMEntitySnapshot>> mapReduce
  ) {
    this.mapReducer = mapReducer;
    this.mapReduce = mapReduce;
  }

  /**
   * This method compares one main (central) OSM object to all nearby OSM objects and
   * returns all nearby objects that match the specified spatial relation.
   *
   * <p>Note on disjoint: Disjoint is implemented as neighbouring()</p>
   *
   * @param centralObject central OSM object which is compared to all nearby objects
   * @param targetRelation Type of spatial relation
   * @return Pair of the central OSM object and a list of nearby OSM objects that fulfill the
   *         specified spatial relation
   */
  private Pair<X, List<OSMEntitySnapshot>> match(
      X centralObject,
      Relation targetRelation,
      double distance
  ) throws Exception {
    // Empty result object: Nearby OSM objects that fulfill the spatial relation type to the
    // central OSM object
    List<OSMEntitySnapshot> matchingNearbyObjects = new ArrayList<>();

    // Get the geometry and ID of the central OSM object
    Geometry geom;
    Long id;
    OSHDBTimestamp timestamp;
    if (centralObject.getClass() == OSMEntitySnapshot.class) {
      geom = ((OSMEntitySnapshot) centralObject).getGeometryUnclipped();
      id = ((OSMEntitySnapshot) centralObject).getEntity().getId();
      timestamp = ((OSMEntitySnapshot) centralObject).getTimestamp();
    } else {
      try {
        geom = ((OSMContribution) centralObject).getGeometryUnclippedAfter();
        id = ((OSMContribution) centralObject).getEntityAfter().getId();
        timestamp = ((OSMContribution) centralObject).getTimestamp();
      } catch (Exception e) {
        geom = ((OSMContribution) centralObject).getGeometryUnclippedBefore();
        id = ((OSMContribution) centralObject).getEntityBefore().getId();
        timestamp = ((OSMContribution) centralObject).getTimestamp();
      }
    }

    // Convert GeometryCollection to Geometry if the object is an OSM relation
    if (geom.getClass() == GeometryCollection.class) {
      geom = geom.union();
    }

    // Create an envelope that represents the neighbourhood of the central OSM object
    Envelope geomEnvelope = geom.getEnvelopeInternal();
    double distanceInDegreeLongitude =
        Geo.convertMetricDistanceToDegreeLongitude(geom.getCentroid().getY(), distance);
    // Multiply distance by 1.2 to avoid falsely excluding nearby OSM objects
    double minLon = geomEnvelope.getMinX() - distanceInDegreeLongitude * 1.2;
    double maxLon = geomEnvelope.getMaxX() + distanceInDegreeLongitude * 1.2;
    double minLat = geomEnvelope.getMinY() - (distance / Geo.ONE_DEGREE_IN_METERS) * 1.2;
    double maxLat = geomEnvelope.getMaxY() + (distance / Geo.ONE_DEGREE_IN_METERS) * 1.2;
    Envelope neighbourhoodEnvelope = new Envelope(minLon, maxLon, minLat, maxLat);

    // Get all OSM objects in the neighbourhood of the central OSM object from STRtree
    List<OSMEntitySnapshot> nearbyObjects = this.objectsForComparison.query(neighbourhoodEnvelope);
    if (nearbyObjects.isEmpty()) {
      return Pair.of(centralObject, matchingNearbyObjects);
    }

    // Compare central OSM Object to nearby OSMEntitySnapshots
    if (centralObject.getClass() == OSMEntitySnapshot.class) {
      Long centralId = id;
      Geometry centralGeom = geom;
      OSHDBTimestamp centralTimestamp = timestamp;
      matchingNearbyObjects = nearbyObjects
          .stream()
          .filter(nearbyObject -> {
            try {
              if (nearbyObject.getTimestamp().compareTo(centralTimestamp) != 0) {
                return false;
              }
              // Skip if the nearby snapshot belongs to the same entity as the central object
              if (centralId == nearbyObject.getEntity().getId()) {
                return false;
              }
              Geometry nearbyGeom = nearbyObject.getGeometryUnclipped();
              // For OSM relations, merge geometries using union()
              if (nearbyGeom.getClass() == GeometryCollection.class) {
                nearbyGeom = nearbyGeom.union();
              }
              if (targetRelation.equals(Relation.NEIGHBOURING)) {
                return isWithinDistance(centralGeom, nearbyGeom, distance)
                    && (centralGeom.disjoint(nearbyGeom) || centralGeom.touches(nearbyGeom));
              } else {
                return relate(centralGeom, nearbyGeom).equals(targetRelation);
              }
            } catch (TopologyException | IllegalArgumentException e) {
              LOG.warn(e.getMessage());
              return false;
            }
          })
          .collect(Collectors.toList());
    } else if (centralObject.getClass() == OSMContribution.class) {

      Long centralId = id;
      Geometry centralGeom = geom;
      matchingNearbyObjects = nearbyObjects
          .stream()
          .filter(nearbyObject -> {
            try {
              // Skip if this candidate snapshot belongs to the same entity
              if (centralId == nearbyObject.getEntity().getId()) {
                return false;
              }
              Geometry nearbyGeom = nearbyObject.getGeometryUnclipped();
              // For OSM relations, merge geometries using union()
              if (nearbyGeom.getClass() == GeometryCollection.class) {
                nearbyGeom = nearbyGeom.union();
              }
              if (targetRelation.equals(Relation.NEIGHBOURING)) {
                return isWithinDistance(centralGeom, nearbyGeom, distance)
                    && (centralGeom.disjoint(nearbyGeom) || centralGeom.touches(nearbyGeom));
              } else {
                return relate(centralGeom, nearbyGeom).equals(targetRelation);
              }
            } catch (TopologyException | IllegalArgumentException e) {
              LOG.warn(e.getMessage());
              return false;
            }
          })
          .collect(Collectors.toList());
      /*
      Geometry centralGeom = geom;
      // Find neighbours of geometry
      MapReducer<OSMEntitySnapshot> subMapReducer = OSMEntitySnapshotView.on(oshdb)
          .keytables(this.oshdb)
          .areaOfInterest(new OSHDBBoundingBox(neighbourhoodEnvelope.getMinX(), neighbourhoodEnvelope.getMinY(), neighbourhoodEnvelope.getMaxX(), neighbourhoodEnvelope.getMaxY()))
          .timestamps(timestamp.toString())
          .filter(snapshot -> {
            Geometry nearbyGeom  = snapshot.getGeometryUnclipped();
                if (targetRelation.equals(relation.NEIGHBOURING)) {
                  return isWithinDistance(centralGeom, nearbyGeom , distanceInMeter)
                      && Arrays.asList(targetRelation.DISJOINT, targetRelation.TOUCHES).contains(relate(centralGeom, nearbyGeom ));
                } else if (targetRelation.equals(relation.DISJOINT)) {
                  return isWithinDistance(centralGeom, nearbyGeom , distanceInMeter) && relate(
                      centralGeom, nearbyGeom ).equals(relation.DISJOINT);
                } else {
                  return relate(centralGeom, nearbyGeom ).equals(targetRelation);
                }
              }
          );
      // Apply mapReducer given by user
      if (mapReduce != null) {
        matchingNearbyObjects =  mapReduce.apply((MapReducer<OSMEntitySnapshot>) subMapReducer);
      } else {
        matchingNearbyObjects = (List<OSMEntitySnapshot>) subMapReducer.collect();
      }
      */
    } else {
      throw new UnsupportedOperationException("match() is not implemented for this class.");
    }
    return Pair.of(centralObject, matchingNearbyObjects);
  }

  /**
   * This method compares one main (central) OSM object to all nearby OSM objects and
   * determines their respective spatial relation to the central OSM object.
   *
   * @param centralObject central OSM object which is compared to all nearby objects
   * @param targetRelation Type of spatial relation
   *
   * @return Pair of the central OSM object and a list of nearby OSM objects that fulfill the
   *         specified spatial relation
   */
  private Pair<X, List<OSMEntitySnapshot>> match(
      X centralObject,
      Relation targetRelation) throws Exception {
    return this.match(centralObject, targetRelation, 100.);
  }

  /**
   * Get objects which are in the neighbourhood of the central object.
   *
   * @return
   */
  public Pair<X, List<OSMEntitySnapshot>> neighbouring(
      X centralObject, double distance
  ) throws Exception {
    return this.match(centralObject, Relation.NEIGHBOURING, distance);
  }

  /**
   * Get objects which are overlapping of the central object.
   *
   * @return
   */
  public Pair<X, List<OSMEntitySnapshot>> overlaps(X centralObject) throws Exception {
    return this.match(centralObject, Relation.OVERLAPS);
  }

  /**
   * Get objects whose geometries are equal to the central object.
   *
   * @return
   */
  // todo: solve issue with overriding Object.equals() --> renamed to equalTo for now
  public Pair<X, List<OSMEntitySnapshot>> equalTo(X centralObject) throws Exception {
    return this.match(centralObject, Relation.EQUALS);
  }

  /**
   * Get objects which are touching to the central object.
   *
   * @return
   */
  public Pair<X, List<OSMEntitySnapshot>> touches(X centralObject) throws Exception {
    return this.match(centralObject, Relation.TOUCHES);
  }

  /**
   * Get objects which are contained in the central object.
   *
   * @return
   */
  public Pair<X, List<OSMEntitySnapshot>> contains(X centralObject) throws Exception {
    return this.match(centralObject, Relation.CONTAINS);
  }

  /**
   * Get objects which are located inside in the central object.
   *
   * @return
   */
  public Pair<X, List<OSMEntitySnapshot>> inside(X centralObject) throws Exception {
    return this.match(centralObject, Relation.INSIDE);
  }

  /**
   * Get objects which are covered by the central object.
   *
   * @return
   */
  public Pair<X, List<OSMEntitySnapshot>> covers(X centralObject) throws Exception {
    return this.match(centralObject, Relation.COVERS);
  }

  /**
   * Get objects which are covering the central object.
   *
   * @return
   */
  public Pair<X, List<OSMEntitySnapshot>> coveredBy(X centralObject) throws Exception {
    return this.match(centralObject, Relation.COVEREDBY);
  }

  /**
   * Retrieves and stores all surrounding OSM features to which the central object should be
   * compared to in an STRtree.
   *
   * @param timestampList Timestamp of the surrounding objects which are used for comparison
   *
   */
  public void getSnapshotsForComparison(OSHDBTimestampList timestampList) throws Exception {

    MapReducer<OSMEntitySnapshot> mapReducer = OSMEntitySnapshotView
        .on(this.mapReducer.oshdb)
        .keytables((OSHDBJdbc) this.mapReducer.oshdb)
        .timestamps(timestampList);

    if (this.mapReducer.getPolyFilter() != null) {
      Geometry polyfilter = this.mapReducer.getPolyFilter();
      mapReducer = mapReducer.areaOfInterest((Geometry & Polygonal) polyfilter);
    } else {
      mapReducer = mapReducer.areaOfInterest(this.mapReducer.bboxFilter);
    }
    // Apply mapReduce function given by user
    List<OSMEntitySnapshot> result;
    if (this.mapReduce != null) {
      result = this.mapReduce.apply(mapReducer);
    } else {
      result = mapReducer.collect();
    }
    // Store OSMEntitySnapshots in STRtree
    result.forEach(snapshot -> this.objectsForComparison.insert(
        snapshot.getGeometryUnclipped().getEnvelopeInternal(),
        snapshot
    ));
  }

  /**
   * Returns the type of spatial relation between two geometries as defined by Egenhofer (1991).
   *
   * <p>
   * Important note: COVERS and TOUCHES are only recognized, if the geometries share a common
   * vertex/node, not if they touch in the middle of a segment.
   * </p>
   *
   * @param geom1 Geometry 1
   * @param geom2 Geometry 2
   * @return Type of spatial relation as defined by Egenhofer (1991)
   */
  public static Relation relate(Geometry geom1, Geometry geom2) {
    // todo: implement touches with buffer
    if (geom1.disjoint(geom2)) {
      return Relation.DISJOINT;
    } else if (geom1.equalsNorm(geom2)) {
      return Relation.EQUALS;
    } else if (geom1.touches(geom2)) {
      return Relation.TOUCHES;
    } else if (geom1.covers(geom2) && geom2.intersects(geom1.getBoundary())) {
      return Relation.COVERS;
    } else if (geom1.covers(geom2) && !geom2.intersects(geom1.getBoundary())) {
      return Relation.CONTAINS;
    } else if (geom1.coveredBy(geom2) && geom1.intersects(geom2.getBoundary())) {
      return Relation.COVEREDBY;
    } else if (geom1.overlaps(geom2) || (geom1.intersects(geom2) && !geom1.within(geom2))) {
      return Relation.OVERLAPS;
    } else if (geom1.within(geom2) && !geom1.intersects(geom2.getBoundary())) {
      return Relation.INSIDE;
    } else {
      return Relation.UNKNOWN;
    }
  }

}
