package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import static org.heigit.bigspatialdata.oshdb.util.geometry.Geo.isWithinDistance;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.IntersectionMatrix;
import com.vividsolutions.jts.geom.TopologyException;
import com.vividsolutions.jts.index.strtree.STRtree;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunctionWithException;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;

/**
 * Class that selects OSM objects (objectsForSelection) based on their spatial relation to
 * other nearby OSM objects (objectsForComparison)
 *
 * Spatial relations comprise the Egenhofer relations and a neighbourhood query
 *
 *
 */
public class SpatialRelation<X, Y> {

  public enum relation {
    EQUALS, OVERLAPS, DISJOINT, CONTAINS, COVEREDBY, COVERS, TOUCHES, INSIDE, UNKNOWN, NEIGHBOURING, INTERSECTS
  }

  private OSHDBBoundingBox bbox;
  private OSHDBJdbc oshdb;
  private OSHDBTimestampList tstamps;
  private SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce;
  private final STRtree objectsForComparison = new STRtree();

  /**
   * Basic constructor
   *
   * The oshdb conntection, bbox and tstamps are taken from the main MapReducer.
   *
   * @param oshdb osdhb connection
   * @param bbox bounding box for querying nearby features to compare with
   * @param tstamps timestamps for querying nearby features to compare with
   * @param mapReduce MapReduce function that specifies features that are used for comparison
   */
  public SpatialRelation(
    OSHDBJdbc oshdb,
    OSHDBBoundingBox bbox,
    OSHDBTimestampList tstamps,
    SerializableFunctionWithException<MapReducer<Y>, List<Y>> mapReduce) {

    this.oshdb = oshdb;
    this.bbox = bbox;
    this.tstamps = tstamps;
    this.mapReduce = mapReduce;
    }

  /**
   * This method compares one main (central) OSM object to all nearby OSM objects and
   * determines their respective spatial relation to the central OSM object.
   *
   * Note on disjoint: Only disjoint objects within the specified distance are returned
   *
   * @param centralObject central OSM object which is compared to all nearby objects
   * @param targetRelation Type of spatial relation
   * @return Pair of the central OSM object and a list of nearby OSM objects that fulfill the
   * specified spatial relation
   */
  private Pair<X, List<Y>> match(
    X centralObject,
    relation targetRelation,
      double distance) throws Exception {

    // Empty result object: Nearby OSM objects that fulfill the spatial relation type to the central OSM object
    List<Y> matchingNearbyObjects = new ArrayList<>();

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
    if (geom.getClass() == GeometryCollection.class) geom = geom.union();

    // Create an envelope that represents the neighbourhood of the central OSM object
    Envelope geomEnvelope = geom.getEnvelopeInternal();
    double distanceInDegreeLongitude = Geo.convertMetricDistanceToDegreeLongitude(geom.getCentroid().getY(), distance);
    // Multiply distance by 1.2 to avoid falsely excluding nearby OSM objects
    double minLon = geomEnvelope.getMinX() - distanceInDegreeLongitude * 1.2;
    double maxLon = geomEnvelope.getMaxX() + distanceInDegreeLongitude * 1.2;
    double minLat = geomEnvelope.getMinY() - (distance / Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR) * 1.2;
    double maxLat = geomEnvelope.getMaxY() + (distance / Geo.ONE_DEGREE_IN_METERS_AT_EQUATOR) * 1.2;
    Envelope neighbourhoodEnvelope = new Envelope(minLon, maxLon, minLat, maxLat);

    // Get all OSM objects in the neighbourhood of the central OSM object from STRtree
    List<Y> nearbyObjects = this.objectsForComparison.query(neighbourhoodEnvelope);
    if (nearbyObjects.isEmpty()) return Pair.of(centralObject, matchingNearbyObjects);

    // Compare central OSM Object to nearby OSMEntitySnapshots
    if (centralObject.getClass() == OSMEntitySnapshot.class) {

      Long centralID = id;
      Geometry centralGeom = geom;
      OSHDBTimestamp centralTimestamp = timestamp;
      matchingNearbyObjects = nearbyObjects
          .stream()
          .filter(nearbyObject -> {
            try {
              OSMEntitySnapshot nearbySnapshot = (OSMEntitySnapshot) nearbyObject;
              if (nearbySnapshot.getTimestamp().compareTo(centralTimestamp) != 0) return false;
              // Skip if the nearby snapshot belongs to the same entity as the central object
              if (centralID == nearbySnapshot.getEntity().getId()) return false;
              Geometry nearbyGeom = nearbySnapshot.getGeometryUnclipped();
              // For OSM relations, merge geometries using union()
              if (nearbyGeom.getClass() == GeometryCollection.class) nearbyGeom = nearbyGeom.union();
              if (targetRelation.equals(relation.NEIGHBOURING)) {
                return isWithinDistance(centralGeom, nearbyGeom, distance)
                    && (centralGeom.disjoint(nearbyGeom) || centralGeom.touches(nearbyGeom));
              } else {
                return relate(centralGeom, nearbyGeom).equals(targetRelation);
              }
            } catch (TopologyException | IllegalArgumentException e) {
              System.out.println(e);
              return false;
            }
          })
          .collect(Collectors.toList());
    } else if (centralObject.getClass() == OSMContribution.class) {

      Long centralID = id;
      Geometry centralGeom = geom;
      matchingNearbyObjects = nearbyObjects
          .stream()
          .filter(nearbyObject -> {
            try {
              OSMEntitySnapshot nearbySnapshot = (OSMEntitySnapshot) nearbyObject;
              // Skip if this candidate snapshot belongs to the same entity
              if (centralID == nearbySnapshot.getEntity().getId()) return false;
              Geometry nearbyGeom = nearbySnapshot.getGeometryUnclipped();
              // For OSM relations, merge geometries using union()
              if (nearbyGeom.getClass() == GeometryCollection.class) nearbyGeom = nearbyGeom.union();
              if (targetRelation.equals(relation.NEIGHBOURING)) {
                return isWithinDistance(centralGeom, nearbyGeom, distance)
                    && (centralGeom.disjoint(nearbyGeom) || centralGeom.touches(nearbyGeom));
              } else {
                boolean result = relate(centralGeom, nearbyGeom).equals(targetRelation);
                return result;
              }
            } catch (TopologyException | IllegalArgumentException e) {
              System.out.println(e);
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
        matchingNearbyObjects =  mapReduce.apply((MapReducer<Y>) subMapReducer);
      } else {
        matchingNearbyObjects = (List<Y>) subMapReducer.collect();
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
   * specified spatial relation
   */
  private Pair<X, List<Y>> match(
      X centralObject,
      relation targetRelation) throws Exception {
    return this.match(centralObject, targetRelation, 100.);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> neighbouring(X centralObject, double distance) throws Exception {
    return this.match(centralObject, relation.NEIGHBOURING, distance);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> overlaps(X centralObject) throws Exception  {
    return this.match(centralObject, relation.OVERLAPS);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> intersects(X centralObject) throws Exception  {
    return this.match(centralObject, relation.INTERSECTS);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  // todo: solve issue with overriding Object.equals() --> renamed to equalTo for now
  public Pair<X, List<Y>> equalTo(X centralObject) throws Exception  {
    return this.match(centralObject, relation.EQUALS);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> touches(X centralObject) throws Exception  {
    return this.match(centralObject, relation.TOUCHES);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> contains(X centralObject) throws Exception  {
    return this.match(centralObject, relation.CONTAINS);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> inside(X centralObject) throws Exception  {
    return this.match(centralObject, relation.INSIDE);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> covers(X centralObject) throws Exception  {
    return this.match(centralObject, relation.COVERS);
  }

  /**
   * Checks if elements are located inside another object
   * @return
   */
  public Pair<X, List<Y>> coveredBy(X centralObject) throws Exception  {
    return this.match(centralObject, relation.COVEREDBY);
  }

  /**
   * Retrieves and stores all OSM objects to which the central object should be compared to in the
   * STRtree object "objectsForComparison"
   *
   */
  public void getOSMEntitySnapshotsForComparison(OSHDBTimestampList timestampList) throws Exception {

    MapReducer<OSMEntitySnapshot> mapReducer = OSMEntitySnapshotView
        .on(this.oshdb)
        .keytables(this.oshdb)
        .areaOfInterest(this.bbox)
        .timestamps(timestampList);
    // Apply mapReduce function given by user (needs to return a list)
    List<Y> result;
    if (this.mapReduce != null) {
      result = this.mapReduce.apply( (MapReducer<Y>) mapReducer);
    } else {
      result = (List<Y>) mapReducer.collect();
    }
    // Store OSMEntitySnapshots in STRtree
    result.forEach(snapshot -> {
        this.objectsForComparison
            .insert(((OSMEntitySnapshot) snapshot).getGeometryUnclipped().getEnvelopeInternal(),
                snapshot);
    });

  }

    /**
     * Returns the type of spatial relation between two geometries as defined by Egenhofer (1991)
     *
     * Important note: COVERS and TOUCHES are only recognized, if the geometries share a common
     * vertex/node, not if they touch in the middle of a segment.
     *
     * @param geom1 Geometry 1
     * @param geom2 Geometry 2
     * @return Type of spatial relation as defined by Egenhofer (1991)
     */
  public static relation relate(Geometry geom1, Geometry geom2) {

    // todo: implement touches with buffer
    if (geom1.disjoint(geom2)) {
      return relation.DISJOINT;
    } else if (geom1.equalsNorm(geom2)) {
      return relation.EQUALS;
    } else if (geom1.touches(geom2)) {
      return relation.TOUCHES;
    } else if (geom1.contains(geom2) && !geom1.getBoundary().intersects(geom2.getBoundary())) {
      return relation.CONTAINS;
    } else if (geom1.covers(geom2) && geom1.getBoundary().intersects(geom2.getBoundary())) {
      return relation.COVERS;
    } else if (geom1.coveredBy(geom2) && geom1.getBoundary().intersects(geom2.getBoundary())) {
      return relation.COVEREDBY;
    } else if (geom1.overlaps(geom2) || (geom1.intersects(geom2) && !geom1.within(geom2))) {
      return relation.OVERLAPS;
    } else if (geom1.within(geom2) && !geom1.getBoundary().intersects(geom2.getBoundary())) {
      return relation.INSIDE;
    } else {
      return relation.UNKNOWN;
    }
  }

}
