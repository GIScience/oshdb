package org.heigit.bigspatialdata.oshdb.util.geometry;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.geotools.geometry.jts.JTS;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Polygonal;

/**
 *
 */
public class OSHDbGeometryBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDbGeometryBuilder.class);

  private OSHDbGeometryBuilder() {}

  // gets the geometry of this object at a specific timestamp
  public static <T extends OSMEntity> Geometry getGeometry(T entity, OSHDBTimestamp timestamp,
      TagInterpreter areaDecider) {
    if (entity instanceof OSMNode) {
      OSMNode node = (OSMNode) entity;
      GeometryFactory geometryFactory = new GeometryFactory();
      return geometryFactory.createPoint(new Coordinate(node.getLongitude(), node.getLatitude()));
    } else if (entity instanceof OSMWay) {
      OSMWay way = (OSMWay) entity;
      // todo: handle old-style multipolygons here???
      GeometryFactory geometryFactory = new GeometryFactory();
      Coordinate[] coords =
          way.getRefEntities(timestamp).filter(node -> node != null && node.isVisible())
              .map(nd -> new Coordinate(nd.getLongitude(), nd.getLatitude()))
              .toArray(Coordinate[]::new);
      if (areaDecider.isLine(entity)) {
        if (coords.length < 2) {
          return null; // better: "invalid line geometry" exception?
        }
        return geometryFactory.createLineString(coords);
      } else {
        if (coords.length < 4) {
          return null; // better: "invalid polygon geometry" exception?
        }
        return geometryFactory.createPolygon(coords);
      }
    }
    OSMRelation relation = (OSMRelation) entity;
    if (areaDecider.isArea(entity)) {
      return OSHDbGeometryBuilder.getMultiPolygonGeometry(relation, timestamp, areaDecider);
    }
    /*
     * if (areaDecider.isLine(entity)) { return getMultiLineStringGeometry(timestamp); }
     */
    GeometryFactory geometryFactory = new GeometryFactory();
    Geometry[] geoms = new Geometry[relation.getMembers().length];
    for (int i = 0; i < relation.getMembers().length; i++) {
      try {
        geoms[i] = OSHDbGeometryBuilder.getGeometry(
            relation.getMembers()[i].getEntity().getByTimestamp(timestamp), timestamp, areaDecider);
      } catch (NullPointerException ex) {
        LOG.warn("Member entity of relation/{} missing, geometry could not be created.",
            entity.getId());
        return null;
      }
    }
    return geometryFactory.createGeometryCollection(geoms);
  }

  private static Geometry getMultiPolygonGeometry(OSMRelation entity, OSHDBTimestamp timestamp,
      TagInterpreter tagInterpreter) {
    GeometryFactory geometryFactory = new GeometryFactory();

    Stream<OSMWay> outerMembers =
        entity.getMemberEntities(timestamp, tagInterpreter::isMultipolygonOuterMember)
            .map(osm -> (OSMWay) osm).filter(way -> way != null && way.isVisible());

    Stream<OSMWay> innerMembers =
        entity.getMemberEntities(timestamp, tagInterpreter::isMultipolygonInnerMember)
            .map(osm -> (OSMWay) osm).filter(way -> way != null && way.isVisible());

    OSMNode[][] outerLines =
        outerMembers
            .map(way -> way.getRefEntities(timestamp)
                .filter(node -> node != null && node.isVisible()).toArray(OSMNode[]::new))
            .filter(line -> line.length > 0).toArray(OSMNode[][]::new);
    OSMNode[][] innerLines =
        innerMembers
            .map(way -> way.getRefEntities(timestamp)
                .filter(node -> node != null && node.isVisible()).toArray(OSMNode[]::new))
            .filter(line -> line.length > 0).toArray(OSMNode[][]::new);

    // construct rings from polygons
    List<LinearRing> outerRings = OSHDbGeometryBuilder.join(outerLines).stream()
        .map(ring -> geometryFactory.createLinearRing(
            ring.stream().map(node -> new Coordinate(node.getLongitude(), node.getLatitude()))
                .toArray(Coordinate[]::new)))
        .collect(Collectors.toList());
    List<LinearRing> innerRings = OSHDbGeometryBuilder.join(innerLines).stream()
        .map(ring -> geometryFactory.createLinearRing(
            ring.stream().map(node -> new Coordinate(node.getLongitude(), node.getLatitude()))
                .toArray(Coordinate[]::new)))
        .collect(Collectors.toList());

    // construct multipolygon from rings
    // todo: handle nested outers with holes (e.g. inner-in-outer-in-inner-in-outer) - worth the
    // effort? see below for a possibly much easier implementation.
    List<Polygon> polys = outerRings.stream().map(outer -> {
      Polygon outerPolygon = new Polygon(outer, null, geometryFactory);
      List<LinearRing> matchingInners = innerRings.stream()
          .filter(ring -> ring.within(outerPolygon)).collect(Collectors.toList());
      // todo: check for inners containing other inners -> inner-in-outer-in-inner-in-outer case
      return new Polygon(outer, matchingInners.toArray(new LinearRing[matchingInners.size()]),
          geometryFactory);
    }).collect(Collectors.toList());

    // todo: what to do with unmatched inner rings??
    if (polys.size() == 1) {
      return polys.get(0);
    } else {
      return new MultiPolygon(polys.toArray(new Polygon[polys.size()]), geometryFactory);
    }
  }

  // helper that joins adjacent osm ways into linear rings
  private static List<List<OSMNode>> join(OSMNode[][] lines) {
    // make a (mutable) copy of the polygons array
    List<List<OSMNode>> ways = new LinkedList<>();
    for (int i = 0; i < lines.length; i++) {
      ways.add(new LinkedList<>(Arrays.asList(lines[i])));
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
          if (lastId == what.get(0).getId()) { // end of partial ring matches to start of current
                                               // line
            what.remove(0);
            current.addAll(what);
            ways.remove(i);
            joinable = true;
            break;
          } else if (firstId == what.get(what.size() - 1).getId()) { // start of partial ring
                                                                     // matches end of current line
            what.remove(what.size() - 1);
            current.addAll(0, what);
            ways.remove(i);
            joinable = true;
            break;
          } else if (lastId == what.get(what.size() - 1).getId()) { // end of partial ring matches
                                                                    // end of current line
            what.remove(what.size() - 1);
            current.addAll(Lists.reverse(what));
            ways.remove(i);
            joinable = true;
            break;
          } else if (firstId == what.get(0).getId()) { // start of partial ring matches start of
                                                       // current line
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
    Geometry geom = OSHDbGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    if (geom == null) {
      return null;
    }
    return Geo.clip(geom, clipBbox);
  }

  public static <P extends Geometry & Polygonal, T extends OSMEntity> Geometry getGeometryClipped(
      T entity, OSHDBTimestamp timestamp, TagInterpreter areaDecider, P clipPoly) {
    Geometry geom = OSHDbGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    if (geom == null) {
      return null;
    }
    return Geo.clip(geom, clipPoly);
  }
  
 
 /**
  * returns JTS geometry object for convenience
  *
  * @return com.vividsolutions.jts.geom.Geometry
  */
 public static Polygon getGeometry(OSHDBBoundingBox bbox) {
   return JTS.toGeometry(new Envelope(bbox.getMinLon(), bbox.getMaxLon(), bbox.getMinLat(), bbox.getMaxLat()));
 }
 
 
 public static OSHDBBoundingBox boundingBoxOf(Envelope envelope){
   return new OSHDBBoundingBox(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY());
 }

}
