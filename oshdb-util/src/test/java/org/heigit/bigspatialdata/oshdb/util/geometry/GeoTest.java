package org.heigit.bigspatialdata.oshdb.util.geometry;

import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeoTest {
  private GeometryFactory gf = new GeometryFactory();

  private Coordinate[] constructCoordinates(double ...coordValues) {
    Coordinate[] coords = new Coordinate[coordValues.length / 2];
    for (int i = 0;  i < coordValues.length / 2; i++) {
      coords[i] = new Coordinate(coordValues[i * 2], coordValues[i * 2 + 1]);
    }
    return coords;
  }

  private LinearRing constructRing(double ...coordValues) {
    return gf.createLinearRing(constructCoordinates(coordValues));
  }

  // Geo.areaOf

  @Test
  public void testAreaPolygon() {
    LinearRing outer = constructRing(
        0, 0,
        0, 1,
        1, 1,
        1, 0,
        0, 0
    );
    LinearRing inner = constructRing(
        0.5, 0.5,
        0.5, 0.6,
        0.6, 0.6,
        0.6, 0.5,
        0.5, 0.5
    );
    // compared with result from geojson.io, allow 5% error to compensate for different
    // geometry calculation parameters (earth "radius", etc.)
    Geometry poly = gf.createPolygon(outer);
    assertEquals(1.0, 12391399902.0 / Geo.areaOf(poly), 0.05);
    // check that poly with whole is actually ~1% smaller in size.
    Geometry polyWithInner = gf.createPolygon(outer, new LinearRing[] { inner });
    assertEquals(0.99, Geo.areaOf(polyWithInner) / Geo.areaOf(poly), 0.0001);
  }

  @Test
  public void testAreaMultiPolygon() {
    Polygon poly1 = gf.createPolygon(constructRing(
        0, 0,
        0, 1,
        1, 1,
        1, 0,
        0, 0
    ));
    Polygon poly2 = gf.createPolygon(constructRing(
        2, 0,
        2, 1,
        3, 1,
        3, 0,
        2, 0
    ));
    // check that multipolygon is 200% larger in size than single poly.
    MultiPolygon mpoly = gf.createMultiPolygon(new Polygon[] { poly1, poly2 });
    assertEquals(2.0, Geo.areaOf(mpoly) / Geo.areaOf(poly1), 0.0001);
  }

  @Test
  public void testAreaGeometryCollection() {
    Polygon poly1 = gf.createPolygon(constructRing(
        0, 0,
        0, 1,
        1, 1,
        1, 0,
        0, 0
    ));
    Polygon poly2 = gf.createPolygon(constructRing(
        2, 0,
        2, 1,
        3, 1,
        3, 0,
        2, 0
    ));
    // check that collection is 200% larger in size than single poly.
    GeometryCollection gcoll = gf.createGeometryCollection(new Geometry[]{ poly1, poly2 });
    assertEquals(2.0, Geo.areaOf(gcoll) / Geo.areaOf(poly1), 0.0001);
    // check that collections with non-polygon members are ignored.
    GeometryCollection gcoll2 = gf.createGeometryCollection(new Geometry[]{
        poly1,
        gf.createPoint(new Coordinate(0, 0)),
        poly2.getExteriorRing()
    });
    assertEquals(1.0, Geo.areaOf(gcoll2) / Geo.areaOf(poly1), 0.0001);
  }

  @Test
  public void testAreaOther() {
    // other geometry types: area should be returned as zero
    // point
    assertEquals(0.0, Geo.areaOf(gf.createPoint(new Coordinate(0, 0))), 1E-22);
    // multi point
    assertEquals(0.0, Geo.areaOf(gf.createMultiPoint(new Point[]{
        gf.createPoint(new Coordinate(0, 0)),
        gf.createPoint(new Coordinate(1, 1))
    })), 1E-22);
    // linestring
    assertEquals(0.0, Geo.areaOf(gf.createLineString(
        constructCoordinates(0, 0, 1, 1, 0, 1, 0, 0)
    )), 1E-22);
    // multi linestring
    assertEquals(0.0, Geo.areaOf(gf.createMultiLineString(new LineString[]{
        gf.createLineString(constructCoordinates(0, 0, 1, 1, 0, 1, 0, 0)),
        gf.createLineString(constructCoordinates(1, 1, 2, 2, 1, 2, 1, 1))
    })), 1E-22);
  }


  // Geo.lengthOf

  @Test
  public void testLengthLineString() {
    LineString line = gf.createLineString(constructCoordinates(
        0, 0,
        1, 1
    ));
    // compared with result from geojson.io, allow 5% error to compensate for different
    // geometry calculation parameters (earth "radius", etc.)
    assertEquals(1.0, 157425.5 / Geo.lengthOf(line), 0.05);
  }

  @Test
  public void testLengthMultiLineString() {
    LineString line1 = gf.createLineString(constructCoordinates(
        0, 0,
        1, 1
    ));
    LineString line2 = gf.createLineString(constructCoordinates(
        1, 1,
        2, 0
    ));
    // check that multilinestring is 200% larger in size than single line.
    MultiLineString mline = gf.createMultiLineString(new LineString[] { line1, line2 });
    assertEquals(2.0, Geo.lengthOf(mline) / Geo.lengthOf(line1), 0.0001);
  }

  @Test
  public void testLengthGeometryCollection() {
    LineString line1 = gf.createLineString(constructCoordinates(
        0, 0,
        1, 1
    ));
    LineString line2 = gf.createLineString(constructCoordinates(
        1, 1,
        2, 0
    ));
    Polygon poly1 = gf.createPolygon(constructRing(
        0, 0,
        0, 1,
        1, 1,
        1, 0,
        0, 0
    ));
    // check that collection is 200% larger in size than single poly.
    GeometryCollection gcoll = gf.createGeometryCollection(new Geometry[]{ line1, line2 });
    assertEquals(2.0, Geo.lengthOf(gcoll) / Geo.lengthOf(line1), 0.0001);
    // check that collections with non-polygon members are ignored.
    GeometryCollection gcoll2 = gf.createGeometryCollection(new Geometry[]{
        line1,
        gf.createPoint(new Coordinate(0, 0)),
        poly1
    });
    assertEquals(1.0, Geo.lengthOf(gcoll2) / Geo.lengthOf(line1), 0.0001);
  }

  @Test
  public void testLengthOther() {
    // other geometry types: area should be returned as zero
    // point
    assertEquals(0.0, Geo.lengthOf(gf.createPoint(new Coordinate(0, 0))), 1E-22);
    // multi point
    assertEquals(0.0, Geo.lengthOf(gf.createMultiPoint(new Point[]{
        gf.createPoint(new Coordinate(0, 0)),
        gf.createPoint(new Coordinate(1, 1))
    })), 1E-22);
    // polygon
    assertEquals(0.0, Geo.lengthOf(gf.createPolygon(
        constructRing(0, 0, 1, 1, 0, 1, 0, 0)
    )), 1E-22);
    // multi polygon
    assertEquals(0.0, Geo.lengthOf(gf.createMultiPolygon(new Polygon[]{
        gf.createPolygon(constructRing(0, 0, 1, 1, 0, 1, 0, 0)),
        gf.createPolygon(constructRing(1, 1, 2, 2, 1, 2, 1, 1))
    })), 1E-22);
  }
}
