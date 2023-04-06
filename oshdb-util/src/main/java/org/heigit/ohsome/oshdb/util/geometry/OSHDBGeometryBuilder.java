package org.heigit.ohsome.oshdb.util.geometry;

import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.OSHDBBoundable;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.osm.OSMCoordinates;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
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
      OSMEntity entity,
      OSHDBTimestamp timestamp,
      TagInterpreter areaDecider
  ) {
    return OSHDBGeometryBuilderInternal.getGeometry(entity, timestamp, null, areaDecider);
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
  public static Polygon getGeometry(@Nonnull OSHDBBoundable bbox) {
    assert bbox != null : "a bounding box is not allowed to be null";

    GeometryFactory gf = new GeometryFactory();

    Coordinate sw = getCoordinate(bbox.getMinLongitude(), bbox.getMinLatitude());
    Coordinate se = getCoordinate(bbox.getMaxLongitude(), bbox.getMinLatitude());
    Coordinate nw = getCoordinate(bbox.getMaxLongitude(), bbox.getMaxLatitude());
    Coordinate ne = getCoordinate(bbox.getMinLongitude(), bbox.getMaxLatitude());

    Coordinate[] cordAr = {sw, se, nw, ne, sw};

    return gf.createPolygon(cordAr);
  }

  public static Coordinate getCoordinate(OSMNode node) {
    return getCoordinate(node.getLon(), node.getLat());
  }

  /**
   * Creates a new instance of jts Coordinate from lon, lat in osm-coordinate system.
   *
   * @param osmLon Longitude in osm-coordinate system
   * @param osmLat Latitude in osm-coordinate system
   * @return new Coordinate instance
   */
  public static Coordinate getCoordinate(int osmLon, int osmLat) {
    return new Coordinate(OSMCoordinates.toWgs84(osmLon), OSMCoordinates.toWgs84(osmLat));
  }


  /**
   * Builds the geometry of an OSM entity at the given timestamp, clipped to the given bounding box.
   *
   * @param entity the osm entity to generate the geometry of
   * @param timestamp  the timestamp for which to create the entity's geometry
   * @param areaDecider a TagInterpreter object which decides whether to generate a linear or a
   *                    polygonal geometry for the respective entity (based on its tags)
   * @param clipBbox the bounding box to clip the resulting geometry to
   * @return a JTS geometry object (simple features compatible, i.e. a Point, LineString, Polygon
   *         or MultiPolygon)
   */
  public static Geometry getGeometryClipped(
      OSMEntity entity,
      OSHDBTimestamp timestamp,
      TagInterpreter areaDecider,
      OSHDBBoundingBox clipBbox
  ) {
    Geometry geom = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    return Geo.clip(geom, clipBbox);
  }

  /**
   * Builds the geometry of an OSM entity at the given timestamp, clipped to the given polygon.
   *
   * @param entity the osm entity to generate the geometry of
   * @param timestamp  the timestamp for which to create the entity's geometry
   * @param areaDecider a TagInterpreter object which decides whether to generate a linear or a
   *                    polygonal geometry for the respective entity (based on its tags)
   * @param clipPoly a polygon to clip the resulting geometry to
   * @param <P> either {@link Polygon} or {@link org.locationtech.jts.geom.MultiPolygon}
   * @return a JTS geometry object (simple features compatible, i.e. a Point, LineString, Polygon
   *         or MultiPolygon)
   */
  public static <P extends Geometry & Polygonal> Geometry getGeometryClipped(
      OSMEntity entity, OSHDBTimestamp timestamp, TagInterpreter areaDecider, P clipPoly
  ) {
    Geometry geom = OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider);
    return Geo.clip(geom, clipPoly);
  }

  /**
   * Converts a JTS bounding box ("envelope") to an OSHDBBoundingBox object.
   *
   * @param envelope the bounding box object to convert
   * @return the same bounding box as an OSHDBBoundingBox object
   */
  public static OSHDBBoundingBox boundingBoxOf(Envelope envelope) {
    return OSHDBBoundingBox.bboxWgs84Coordinates(
        envelope.getMinX(),
        envelope.getMinY(),
        envelope.getMaxX(),
        envelope.getMaxY()
    );
  }

}
