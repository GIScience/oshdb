package org.heigit.bigspatialdata.oshdb.util.geometry;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedPolygon;
import org.locationtech.jts.index.strtree.STRtree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds JTS geometries from OSM entities.
 */
public class OSHDBGeometryBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(OSHDBGeometryBuilder.class);

  private OSHDBGeometryBuilder() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Gets the geometry of an OSM entity at a specific timestamp.
   *
   * <p>
   * The given timestamp must be in the valid timestamp range of the given entity version:
   * </p>
   * <ul>
   *   <li>timestamp must be equal or bigger than entity.getTimestamp()</li>
   *   <li>timestamp must be less than the next version of this osm entity (if one exists)</li>
   * </ul>
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
              .filter(Objects::nonNull)
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
    Stream<Stream<OSMNode>> outerLines = waysToLines(
        relation.getMemberEntities(timestamp, areaDecider::isMultipolygonOuterMember),
        timestamp
    );

    Stream<Stream<OSMNode>> innerLines = waysToLines(
        relation.getMemberEntities(timestamp, areaDecider::isMultipolygonInnerMember),
        timestamp
    );

    // construct rings from polygons
    List<LinearRing> outerRings = OSHDBGeometryBuilder.buildRings(outerLines).stream()
        .map(ring -> geometryFactory.createLinearRing(
            ring.stream().map(node -> new Coordinate(node.getLongitude(), node.getLatitude()))
                .toArray(Coordinate[]::new)))
        .collect(Collectors.toList());
    List<LinkedList<OSMNode>> innerRingsNodes = OSHDBGeometryBuilder.buildRings(innerLines);
    // check if there are any touching inner/outer rings, merge any
    mergeTouchingRings(innerRingsNodes);
    List<LinearRing> innerRings = innerRingsNodes.stream()
        .map(ring -> geometryFactory.createLinearRing(
            ring.stream().map(node -> new Coordinate(node.getLongitude(), node.getLatitude()))
                .toArray(Coordinate[]::new)))
        .collect(Collectors.toList());

    // construct multipolygon from rings
    // todo: handle nested outers with holes (e.g. inner-in-outer-in-inner-in-outer) - worth the
    // effort? see below for a possibly much easier implementation.
    Geometry result;
    if (outerRings.size() == 1) {
      result = geometryFactory.createPolygon(
          outerRings.get(0),
          innerRings.toArray(new LinearRing[0])
      );
    } else {
      STRtree innersTree = new STRtree();
      innerRings.forEach(inner -> innersTree.insert(inner.getEnvelopeInternal(), inner));
      Polygon[] polys = outerRings.stream().map(outer -> {
        // todo: check for inners containing other inners -> inner-in-outer-in-inner-in-outer case
        try {
          return constructMultipolygonPart(
              geometryFactory,
              innersTree,
              geometryFactory.createPolygon(outer)
          );
        } catch (TopologyException e) {
          // try again with buffer(0) on outer ring
          Geometry buffered = geometryFactory.createPolygon(outer).buffer(0);
          if (buffered instanceof Polygon) {
            return constructMultipolygonPart(
                geometryFactory,
                innersTree,
                (Polygon) buffered
            );
          } else {
            return null;
          }
        }
      })
      .filter(Objects::nonNull)
      .toArray(Polygon[]::new);
      // todo: what to do with unmatched inner rings??
      result = geometryFactory.createMultiPolygon(polys);
    }
    return result;
  }

  private static Stream<Stream<OSMNode>> waysToLines(Stream<OSMEntity> members, OSHDBTimestamp timestamp) {
    return members
        .map(osm -> (OSMWay) osm)
        .filter(Objects::nonNull)
        .filter(OSMEntity::isVisible)
        .map(way -> way.getRefEntities(timestamp)
            .filter(Objects::nonNull)
            .filter(OSMEntity::isVisible)
        );
  }

  private static void mergeTouchingRings(List<LinkedList<OSMNode>> ringsNodes) {
    Map<Segment, LinkedList<OSMNode>> ringSegments = new HashMap<>();
    List<LinkedList<OSMNode>> mergedRings = new LinkedList<>();
    for (LinkedList<OSMNode> ringNodes : ringsNodes) {
      List<Segment> thisRingSegments = new ArrayList<>(ringNodes.size() - 1);
      int numNodes = ringNodes.size();
      long prevNodeId = ringNodes.get(0).getId();
      for (int i = 1; i < numNodes; i++) {
        long thisNodeId = ringNodes.get(i).getId();
        Segment segment = new Segment(prevNodeId, thisNodeId);
        if (!ringSegments.containsKey(segment)) {
          thisRingSegments.add(segment);
        } else {
          // merge this ring with the previous one
          LinkedList<OSMNode> targetNodes = ringSegments.get(segment);
          ringSegments.values().remove(targetNodes);
          cutAtSegment(targetNodes, segment);
          cutAtSegment(ringNodes, segment);
          mergeSegmentsToRing(targetNodes, ringNodes);
          // clean up
          // add merged segments to thisRingSements
          thisRingSegments.clear();
          for (int j = 1; j < targetNodes.size(); j++) {
            thisRingSegments.add(
                new Segment(targetNodes.get(j - 1).getId(), targetNodes.get(j).getId())
            );
          }
          // mark merged ring as to be removed
          mergedRings.add(ringNodes);
          ringNodes = targetNodes;
          break;
        }
        prevNodeId = thisNodeId;
      }
      for (Segment thisRingSegment : thisRingSegments) {
        ringSegments.put(thisRingSegment, ringNodes);
      }
    }
    ringsNodes.removeAll(mergedRings);
  }

  private static void cutAtSegment(LinkedList<OSMNode> ring, Segment segment) {
    ring.remove(0);
    while (true) {
      if (ring.getFirst().getId() == segment.id1 && ring.getLast().getId() == segment.id2
          || ring.getFirst().getId() == segment.id2 && ring.getLast().getId() == segment.id1) {
        return;
      }
      ring.add(ring.removeFirst());
    }
  }

  private static void mergeSegmentsToRing(LinkedList<OSMNode> target, LinkedList<OSMNode> source) {
    if (target.getFirst().getId() == source.getFirst().getId()) {
      Collections.reverse(source);
    }
    // clean shared segments
    while (source.size() > 1 && target.size() > 1
        && source.getFirst().getId() == target.getLast().getId()
        && source.get(1).getId() == target.get(target.size() - 2).getId()) {
      source.removeFirst();
      target.removeLast();
    }
    while (source.size() > 1 && target.size() > 1
        && source.getLast().getId() == target.getFirst().getId()
        && source.get(source.size() - 2).getId() == target.get(1).getId()) {
      source.removeLast();
      target.removeFirst();
    }
    // merge partial rings to new complete one
    source.removeFirst();
    target.addAll(source);
  }

  private static Polygon constructMultipolygonPart(
      GeometryFactory geometryFactory,
      STRtree inners,
      Polygon outer
  ) throws TopologyException {
    PreparedGeometry outerPolygon = new PreparedPolygon(outer);
    @SuppressWarnings("unchecked") // JTS returns raw types, but they are actually LinearRings
        List<LinearRing> innerCandidates = inners.query(outer.getEnvelopeInternal());
    return geometryFactory.createPolygon(
        (LinearRing) outer.getExteriorRing(),
        innerCandidates.stream().filter(outerPolygon::contains).toArray(LinearRing[]::new)
    );
  }

  // helper that joins adjacent osm ways into linear rings
  private static List<LinkedList<OSMNode>> buildRings(Stream<Stream<OSMNode>> lines) {
    // make a (mutable) copy of the polygons array
    List<LinkedList<OSMNode>> ways = lines
        .map(line -> line.collect(Collectors.toCollection(LinkedList::new)))
        .filter(nodesList -> !nodesList.isEmpty())
        .collect(Collectors.toCollection(LinkedList::new));

    List<LinkedList<OSMNode>> joined = new LinkedList<>();
    while (!ways.isEmpty()) {
      LinkedList<OSMNode> current = ways.remove(0);
      joined.add(current);
      while (!ways.isEmpty()) {
        long firstId = current.getFirst().getId();
        long lastId = current.getLast().getId();
        if (firstId == lastId) {
          break; // ring is complete -> we're done
        }
        boolean joinable = false;
        for (int i = 0; i < ways.size(); i++) {
          LinkedList<OSMNode> what = ways.get(i);
          if (lastId == what.getFirst().getId()) {
            // end of partial ring matches to start of current line
            what.removeFirst();
            current.addAll(what);
            ways.remove(i);
            joinable = true;
            break;
          } else if (firstId == what.getLast().getId()) {
            // start of partial ring matches end of current line
            what.removeLast();
            current.addAll(0, what);
            ways.remove(i);
            joinable = true;
            break;
          } else if (lastId == what.getLast().getId()) {
            // end of partial ring matches end of current line
            what.removeLast();
            current.addAll(Lists.reverse(what));
            ways.remove(i);
            joinable = true;
            break;
          } else if (firstId == what.getFirst().getId()) {
            // start of partial ring matches start of current line
            what.removeFirst();
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
  public static Polygon getGeometry(@Nonnull OSHDBBoundingBox bbox) {
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

  private static class Segment {
    long id1;
    long id2;

    Segment(long id1, long id2) {
      this.id1 = id1;
      this.id2 = id2;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof Segment) {
        Segment otherSegment = (Segment) other;
        return otherSegment.id1 == this.id1 && otherSegment.id2 == this.id2
            || otherSegment.id1 == this.id2 && otherSegment.id2 == this.id1;
      } else {
        return super.equals(other);
      }
    }

    @Override
    public int hashCode() {
      return ((int) this.id1) + ((int) this.id2);
    }
  }
}
