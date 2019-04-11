package org.heigit.bigspatialdata.oshdb.util.geometry;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntities;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.taginterpreter.TagInterpreter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedPolygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds JTS geometries from OSM entities.
 */
public class OSHDBGeometryBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDBGeometryBuilder.class);

  /**
   * Gets the geometry of an OSM entity at a specific timestamp.
   *
   * <p>
   * The given timestamp must be in the valid timestamp range of the given entity version:
   * <ul>
   *   <li>timestamp must be equal or bigger than entity.getTimestamp()</li>
   *   <li>timestamp must be less than the next version of this osm entity (if one exists)</li>
   * </ul>
   * </p>
   *
   * @param entity the osm entity to generate the geometry of
   * @param timestamp the timestamp for which to create the entity's geometry
   * @param areaDecider a TagInterpreter object which decides whether to generate a linear or a
   *                    polygonal geometry for the respective entity (based on its tags)
   * @return a JTS geometry object (simple features compatible, i.e. a Point, LineString, Polygon
   *         or MultiPolygon)
   */
  @Nonnull
  public static Geometry getGeometry(
      OSMEntity entity, OSHDBTimestamp timestamp, TagInterpreter areaDecider
  ) {
    GeometryFactory geometryFactory = new GeometryFactory();
    if (timestamp.compareTo(entity.getTimestamp()) < 0) {
      throw new AssertionError(
          "cannot produce geometry of entity for timestamp before this entity's version's timestamp"
      );
    }
    if (entity instanceof OSMNode) {
      OSMNode node = (OSMNode) entity;
      if (node.isVisible()) {
        return geometryFactory.createPoint(new Coordinate(node.getLongitude(), node.getLatitude()));
      } else {
        return geometryFactory.createPoint((Coordinate) null);
      }
    } else if (entity instanceof OSMWay) {
      OSMWay way = (OSMWay) entity;
      if (!way.isVisible()) {
        LOG.info("way/{} is deleted - falling back to empty (line) geometry", way.getId());
        return geometryFactory.createLineString((CoordinateSequence) null);
      }
      // todo: handle old-style multipolygons here???
      Coordinate[] coords =
          way.getRefEntities(timestamp)
              .filter(OSMEntity::isVisible)
              .map(nd -> new Coordinate(nd.getLongitude(), nd.getLatitude()))
              .toArray(Coordinate[]::new);
      if (areaDecider.isArea(entity)) {
        if (coords.length >= 4 && coords[0].equals2D(coords[coords.length - 1])) {
          return geometryFactory.createPolygon(coords);
        } else {
          LOG.warn("way/{} doesn't form a linear ring - falling back to linestring", way.getId());
        }
      }
      if (coords.length >= 2) {
        return geometryFactory.createLineString(coords);
      }
      if (coords.length == 1) {
        LOG.info("way/{} is single-noded - falling back to point geometry", way.getId());
        return geometryFactory.createPoint(coords[0]);
      } else {
        LOG.warn("way/{} with no nodes - falling back to empty (point) geometry", way.getId());
        return geometryFactory.createPoint((Coordinate) null);
      }
    } else {
      OSMRelation relation = (OSMRelation) entity;
      if (!relation.isVisible()) {
        LOG.info(
            "relation/{} is deleted - falling back to empty geometry (collection)",
            relation.getId()
        );
        return geometryFactory.createGeometryCollection(null);
      }
      if (areaDecider.isArea(entity)) {
        try {
          Geometry multipolygon = OSHDBGeometryBuilder.getMultiPolygonGeometry(
              relation, timestamp, areaDecider, geometryFactory
          );
          if (!multipolygon.isEmpty()) {
            return multipolygon;
          }
          // otherwise (empty geometry): fall back to geometry collection builder
        } catch (IllegalArgumentException e) {
          // fall back to geometry collection builder
        }
      }
      /* todo:implement multilinestring mode for stuff like route relations
       * if (areaDecider.isLine(entity)) { return getMultiLineStringGeometry(timestamp); }
       */
      return getGeometryCollectionGeometry(relation, timestamp, areaDecider, geometryFactory);
    }
  }

  private static Geometry getGeometryCollectionGeometry(
      OSMRelation relation,
      OSHDBTimestamp timestamp,
      TagInterpreter areaDecider,
      GeometryFactory geometryFactory
  ) {
    OSMMember[] relationMembers = relation.getMembers();
    Geometry[] geoms = new Geometry[relationMembers.length];
    boolean completeGeometry = true;
    for (int i = 0; i < relationMembers.length; i++) {
      OSHEntity memberOSHEntity = relationMembers[i].getEntity();
      // memberOSHEntity might be null when working on an extract with incomplete relation members
      OSMEntity memberEntity = memberOSHEntity == null ? null :
          OSHEntities.getByTimestamp(memberOSHEntity, timestamp);
      /*
      memberEntity might be null when working with redacted data, for example:
       - user 1 creates node 1 (timestamp 1)
       - user 2 creates relation 1 with node 1 as member (timestamp 2)
       - user 2 edits node 1, which now has a version 2 (timestamp 3)
       - user 1's edits are redacted -> node 1 version 1 is now hidden (version 2 isn't)
      now when requesting relation 1's geometry at timestamps between 2 and 3, memberOSHEntity
      is not null, but memberEntity is.
      */
      if (memberEntity == null) {
        geoms[i] = null;
        completeGeometry = false;
        LOG.info(
            "Member entity {}/{} of relation/{} missing, geometry could not be assembled fully.",
            relationMembers[i].getType(), relationMembers[i].getId(), relation.getId()
        );
      } else {
        geoms[i] = OSHDBGeometryBuilder.getGeometry(
            memberEntity,
            timestamp,
            areaDecider
        );
      }
    }
    if (completeGeometry) {
      return geometryFactory.createGeometryCollection(geoms);
    } else {
      return geometryFactory.createGeometryCollection(
          Arrays.stream(geoms).filter(Objects::nonNull).toArray(Geometry[]::new)
      );
    }
  }

  private static Geometry getMultiPolygonGeometry(
      OSMRelation relation,
      OSHDBTimestamp timestamp,
      TagInterpreter areaDecider,
      GeometryFactory geometryFactory
  ) {
    Stream<OSMWay> outerMembers =
        relation.getMemberEntities(timestamp, areaDecider::isMultipolygonOuterMember)
            .map(osm -> (OSMWay) osm)
            .filter(OSMEntity::isVisible);

    Stream<OSMWay> innerMembers =
        relation.getMemberEntities(timestamp, areaDecider::isMultipolygonInnerMember)
            .map(osm -> (OSMWay) osm).filter(OSMEntity::isVisible);

    OSMNode[][] outerLines =
        outerMembers
            .map(way -> way.getRefEntities(timestamp)
                .filter(OSMEntity::isVisible).toArray(OSMNode[]::new))
            .filter(line -> line.length > 0).toArray(OSMNode[][]::new);
    OSMNode[][] innerLines =
        innerMembers
            .map(way -> way.getRefEntities(timestamp)
                .filter(OSMEntity::isVisible).toArray(OSMNode[]::new))
            .filter(line -> line.length > 0).toArray(OSMNode[][]::new);

    // construct rings from polygons
    List<LinearRing> outerRings = OSHDBGeometryBuilder.join(outerLines).stream()
        .map(ring -> geometryFactory.createLinearRing(
            ring.stream().map(node -> new Coordinate(node.getLongitude(), node.getLatitude()))
                .toArray(Coordinate[]::new)))
        .collect(Collectors.toList());
    List<LinearRing> innerRings = OSHDBGeometryBuilder.join(innerLines).stream()
        .map(ring -> geometryFactory.createLinearRing(
            ring.stream().map(node -> new Coordinate(node.getLongitude(), node.getLatitude()))
                .toArray(Coordinate[]::new)))
        .collect(Collectors.toList());

    // check if there are any touching inner/outer rings
    Set<Integer> segments = new HashSet<>();
    boolean touchingRings = false;
    List<LinearRing> allRings = new ArrayList<>(innerRings.size() + outerRings.size());
    allRings.addAll(outerRings);
    allRings.addAll(innerRings);
    checkInnerTouchingRings: for (LinearRing ring : allRings) {
      int numPoints = ring.getNumPoints();
      Coordinate prevPoint = ring.getCoordinateN(0);
      for (int i = 1; i < numPoints; i++) {
        Coordinate thisPoint = ring.getCoordinateN(i);
        int segmentHashCode = prevPoint.hashCode() ^ thisPoint.hashCode();
        if (segments.contains(segmentHashCode)) {
          touchingRings = true;
          break checkInnerTouchingRings;
        }
        segments.add(segmentHashCode);
        prevPoint = thisPoint;
      }
    }

    // construct multipolygon from rings
    // todo: handle nested outers with holes (e.g. inner-in-outer-in-inner-in-outer) - worth the
    // effort? see below for a possibly much easier implementation.
    List<Polygon> polys = outerRings.stream().map(outer -> {
      PreparedGeometry outerPolygon = new PreparedPolygon(geometryFactory.createPolygon(outer));
      // todo: check for inners containing other inners -> inner-in-outer-in-inner-in-outer case
      return geometryFactory.createPolygon(
          outer,
          innerRings.stream().filter(outerPolygon::contains).toArray(LinearRing[]::new)
      );
    }).collect(Collectors.toList());

    // todo: what to do with unmatched inner rings??
    Geometry result;
    if (polys.size() == 1) {
      result = polys.get(0);
    } else {
      result = geometryFactory.createMultiPolygon(polys.toArray(new Polygon[polys.size()]));
    }
    if (touchingRings) {
      // try to clean up gometry by calling buffer(0).
      // see https://locationtech.github.io/jts/jts-faq.html#G1
      result = result.buffer(0);
    }
    return result;
  }

  // helper that joins adjacent osm ways into linear rings
  private static List<List<OSMNode>> join(OSMNode[][] lines) {
    // make a (mutable) copy of the polygons array
    List<List<OSMNode>> ways = new LinkedList<>();
    for (OSMNode[] line : lines) {
      ways.add(new LinkedList<>(Arrays.asList(line)));
    }
    List<List<OSMNode>> joined = new LinkedList<>();

    while (!ways.isEmpty()) {
      List<OSMNode> current = ways.remove(0);
      joined.add(current);
      while (!ways.isEmpty()) {
        long firstId = current.get(0).getId();
        long lastId = current.get(current.size() - 1).getId();
        if (firstId == lastId) {
          break; // ring is complete -> we're done
        }
        boolean joinable = false;
        for (int i = 0; i < ways.size(); i++) {
          List<OSMNode> what = ways.get(i);
          if (lastId == what.get(0).getId()) {
            // end of partial ring matches to start of current line
            what.remove(0);
            current.addAll(what);
            ways.remove(i);
            joinable = true;
            break;
          } else if (firstId == what.get(what.size() - 1).getId()) {
            // start of partial ring matches end of current line
            what.remove(what.size() - 1);
            current.addAll(0, what);
            ways.remove(i);
            joinable = true;
            break;
          } else if (lastId == what.get(what.size() - 1).getId()) {
            // end of partial ring matches end of current line
            what.remove(what.size() - 1);
            current.addAll(Lists.reverse(what));
            ways.remove(i);
            joinable = true;
            break;
          } else if (firstId == what.get(0).getId()) {
            // start of partial ring matches start of current line
            what.remove(0);
            current.addAll(0, Lists.reverse(what));
            ways.remove(i);
            joinable = true;
            break;
          }
        }
        if (!joinable) {
          // Invalid geometry (dangling way, unclosed ring)
          break;
        }
      }
    }

    return joined;
  }

  public static <T extends OSMEntity> Geometry getGeometryClipped(T entity, OSHDBTimestamp timestamp,
      TagInterpreter areaDecider, OSHDBBoundingBox clipBbox) {
    Geometry geom = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    return Geo.clip(geom, clipBbox);
  }

  public static <P extends Geometry & Polygonal, T extends OSMEntity> Geometry getGeometryClipped(
      T entity, OSHDBTimestamp timestamp, TagInterpreter areaDecider, P clipPoly) {
    Geometry geom = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    return Geo.clip(geom, clipPoly);
  }
  
  /**
   * Converts a OSHDBBoundingBox to a rectangular polygon.
   *
   * <p>
   * Will return a polygon with exactly 4 vertices even for point or line-like BoundingBox.
   * Nevertheless, for degenerate bounding boxes (width and/or height of 0) the result might
   * not pass the {@link Geometry#isRectangle() Geometry.isRectangle} test.
   * </p>
   *
   * @param bbox The BoundingBox the polygon should be created for.
   * @return a rectangular Polygon
   */
  public static Polygon getGeometry(OSHDBBoundingBox bbox) {
    assert bbox != null : "a bounding box is not allowed to be null";
    
    GeometryFactory gf = new GeometryFactory();
        
    Coordinate sw = new Coordinate(bbox.getMinLon(), bbox.getMinLat());
    Coordinate se = new Coordinate(bbox.getMaxLon(), bbox.getMinLat());
    Coordinate nw = new Coordinate(bbox.getMaxLon(), bbox.getMaxLat());
    Coordinate ne = new Coordinate(bbox.getMinLon(), bbox.getMaxLat());

    Coordinate[] cordAr = {sw, se, nw, ne, sw};

    return gf.createPolygon(cordAr);
  }
  
  public static OSHDBBoundingBox boundingBoxOf(Envelope envelope) {
    return new OSHDBBoundingBox(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY());
  }

}
