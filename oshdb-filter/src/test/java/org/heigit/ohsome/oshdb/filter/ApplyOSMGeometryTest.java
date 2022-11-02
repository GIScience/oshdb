package org.heigit.ohsome.oshdb.filter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Streams;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Tests the application of filters to OSM geometries.
 */
class ApplyOSMGeometryTest extends FilterTest {
  private final GeometryFactory gf = new GeometryFactory();

  private Polygon getBoundingBoxPolygon(
      double minLon, double minLat, double maxLon, double maxLat) {
    return OSHDBGeometryBuilder.getGeometry(OSHDBBoundingBox.bboxWgs84Coordinates(
        minLon, minLat, maxLon, maxLat));
  }

  @Test
  void testGeometryTypeFilterPoint() {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression.applyOSMGeometry(createTestOSMEntityNode(), gf.createPoint()));
    // negated
    assertFalse(expression.negate().applyOSMGeometry(createTestOSMEntityNode(), gf.createPoint()));
  }

  @Test
  void testGeometryTypeFilterLine() {
    FilterExpression expression = parser.parse("geometry:line");
    OSMEntity validWay = createTestOSMEntityWay(new long[]{1, 2, 3, 4, 1});
    assertTrue(expression.applyOSMGeometry(validWay, gf.createLineString()));
    assertFalse(expression.applyOSMGeometry(validWay, gf.createPolygon()));
  }

  @Test
  void testGeometryTypeFilterPolygon() {
    FilterExpression expression = parser.parse("geometry:polygon");
    OSMEntity validWay = createTestOSMEntityWay(new long[]{1, 2, 3, 4, 1});
    assertTrue(expression.applyOSMGeometry(validWay, gf.createPolygon()));
    assertFalse(expression.applyOSMGeometry(validWay, gf.createLineString()));
    OSMEntity validRelationA = createTestOSMEntityRelation("type", "multipolygon");
    assertTrue(expression.applyOSMGeometry(validRelationA, gf.createPolygon()));
    assertFalse(expression.applyOSMGeometry(validRelationA, gf.createGeometryCollection()));
    OSMEntity validRelationB = createTestOSMEntityRelation("type", "boundary");
    assertTrue(expression.applyOSMGeometry(validRelationB, gf.createPolygon()));
    assertFalse(expression.applyOSMGeometry(validRelationB, gf.createGeometryCollection()));
  }

  @Test
  void testGeometryTypeFilterOther() {
    FilterExpression expression = parser.parse("geometry:other");
    OSMEntity validRelation = createTestOSMEntityRelation();
    assertTrue(expression.applyOSMGeometry(validRelation, gf.createGeometryCollection()));
    assertFalse(expression.applyOSMGeometry(validRelation, gf.createPolygon()));
    assertFalse(expression.applyOSMGeometry(validRelation, gf.createMultiPoint()));
    assertFalse(expression.applyOSMGeometry(validRelation, gf.createMultiLineString()));
    assertFalse(expression.applyOSMGeometry(validRelation, gf.createMultiPolygon()));

    // also ways can result in GeometryCollections after a clipping operation!
    OSMEntity validWay = createTestOSMEntityWay(new long[]{1, 2, 3, 4, 1});
    assertTrue(expression.applyOSMGeometry(validWay, gf.createGeometryCollection()));
    assertFalse(expression.applyOSMGeometry(validWay, gf.createPolygon()));
    assertFalse(expression.applyOSMGeometry(validWay, gf.createMultiPoint()));
    assertFalse(expression.applyOSMGeometry(validWay, gf.createMultiLineString()));
    assertFalse(expression.applyOSMGeometry(validWay, gf.createMultiPolygon()));
  }

  @Test
  void testAndOperator() {
    FilterExpression expression = parser.parse("geometry:point and name=*");
    assertTrue(expression.applyOSMGeometry(
        createTestOSMEntityNode("name", "FIXME"),
        gf.createPoint()
    ));
    assertFalse(expression.applyOSMGeometry(
        createTestOSMEntityWay(new long[] {}, "name", "FIXME"),
        gf.createLineString()
    ));
    assertFalse(expression.applyOSMGeometry(
        createTestOSMEntityNode(),
        gf.createPoint()
    ));
  }

  @Test
  void testOrOperator() {
    FilterExpression expression = parser.parse("geometry:point or geometry:polygon");
    assertTrue(expression.applyOSMGeometry(
        createTestOSMEntityNode(),
        gf.createPoint()
    ));
    assertTrue(expression.applyOSMGeometry(
        createTestOSMEntityWay(new long[] {1, 2, 3, 4, 1}),
        gf.createPolygon()
    ));
    assertFalse(expression.applyOSMGeometry(
        createTestOSMEntityWay(new long[] {1, 2, 3, 4, 1}),
        gf.createLineString()
    ));
  }

  @Test
  void testGeometryFilterArea() {
    FilterExpression expression = parser.parse("area:(1..2)");
    OSMEntity entity = createTestOSMEntityWay(new long[] {1, 2, 3, 4, 1});
    assertFalse(expression.applyOSMGeometry(entity,
        // approx 0.3m²
        getBoundingBoxPolygon(0, 0, 5E-6, 5E-6)
    ));
    assertTrue(expression.applyOSMGeometry(entity,
        // approx 1.2m²
        getBoundingBoxPolygon(0, 0, 1E-5, 1E-5)
    ));
    assertFalse(expression.applyOSMGeometry(entity,
        // approx 4.9m²
        getBoundingBoxPolygon(0, 0, 2E-5, 2E-5)
    ));
    // negated
    assertFalse(expression.negate().applyOSMGeometry(entity,
        // approx 1.2m²
        getBoundingBoxPolygon(0, 0, 1E-5, 1E-5)
    ));
    assertTrue(expression.negate().applyOSMGeometry(entity,
        // approx 0.3m²
        OSHDBGeometryBuilder
            .getGeometry(OSHDBBoundingBox.bboxWgs84Coordinates(0, 0, 5E-6, 5E-6))
    ));
  }

  @Test
  void testGeometryFilterLength() {
    FilterExpression expression = parser.parse("length:(1..2)");
    OSMEntity entity = createTestOSMEntityWay(new long[] {1, 2});
    assertFalse(expression.applyOSMGeometry(entity,
        // approx 0.6m
        gf.createLineString(new Coordinate[] {
            new Coordinate(0, 0),
            new Coordinate(5E-6, 0)
        })
    ));
    assertTrue(expression.applyOSMGeometry(entity,
        // approx 1.1m
        gf.createLineString(new Coordinate[] {
            new Coordinate(0, 0),
            new Coordinate(1E-5, 0)
        })
    ));
    assertFalse(expression.applyOSMGeometry(entity,
        // approx 2.2m
        gf.createLineString(new Coordinate[] {
            new Coordinate(0, 0),
            new Coordinate(2E-5, 0)
        })
    ));
    // negated
    assertTrue(expression.negate().applyOSMGeometry(entity,
        // approx 0.6m
        gf.createLineString(new Coordinate[] {
            new Coordinate(0, 0),
            new Coordinate(5E-6, 0)
        })
    ));
  }

  @Test
  void testGeometryFilterPerimeter() {
    FilterExpression expression = parser.parse("perimeter:(4..5)");
    OSMEntity entity = createTestOSMEntityWay(new long[] {1, 2, 3, 4, 1});
    assertFalse(expression.applyOSMGeometry(entity,
        // square with approx 0.6m edge length
        getBoundingBoxPolygon(0, 0, 5E-6, 5E-6)
    ));
    assertTrue(expression.applyOSMGeometry(entity,
        // square with approx 1.1m edge length
        getBoundingBoxPolygon(0, 0, 1E-5, 1E-5)
    ));
    assertFalse(expression.applyOSMGeometry(entity,
        // square with approx 2.2m edge length
        getBoundingBoxPolygon(0, 0, 2E-5, 2E-5)
    ));
    // negated
    assertTrue(expression.negate().applyOSMGeometry(entity,
        // square with approx 0.6m edge length
        getBoundingBoxPolygon(0, 0, 5E-6, 5E-6)
    ));
  }

  @Test
  void testGeometryFilterVertices() {
    FilterExpression expression = parser.parse("geometry.vertices:(11..13)");
    // point
    assertFalse(expression.applyOSMGeometry(
        createTestOSMEntityNode("natural", "tree"),
        gf.createPoint(new Coordinate(0, 0))));
    // lines
    BiConsumer<Integer, Consumer<Boolean>> testLineN = (n, tester) -> {
      var entity = createTestOSMEntityWay(LongStream.rangeClosed(1, n).toArray());
      var coords = LongStream.rangeClosed(1, n)
          .mapToObj(i -> new Coordinate(i, i)).toArray(Coordinate[]::new);
      var geometry = gf.createLineString(coords);
      tester.accept(expression.applyOSMGeometry(entity, geometry));
    };
    testLineN.accept(10, Assertions::assertFalse);
    testLineN.accept(11, Assertions::assertTrue);
    testLineN.accept(12, Assertions::assertTrue);
    testLineN.accept(13, Assertions::assertTrue);
    testLineN.accept(14, Assertions::assertFalse);
    // polygons
    BiConsumer<Integer, Consumer<Boolean>> testPolyonN = (n, tester) -> {
      var entity = createTestOSMEntityWay(LongStream.rangeClosed(1, n).toArray());
      var coords = Streams.concat(
          LongStream.rangeClosed(1, n - 1).mapToObj(i -> new Coordinate(i, Math.pow(i, 2))),
          Stream.of(new Coordinate(1, 1))
      ).toArray(Coordinate[]::new);
      var geometry = gf.createPolygon(coords);
      tester.accept(expression.applyOSMGeometry(entity, geometry));
    };
    testPolyonN.accept(10, Assertions::assertFalse);
    testPolyonN.accept(11, Assertions::assertTrue);
    testPolyonN.accept(12, Assertions::assertTrue);
    testPolyonN.accept(13, Assertions::assertTrue);
    testPolyonN.accept(14, Assertions::assertFalse);
    // polygon with hole
    BiConsumer<Integer, Consumer<Boolean>> testPolyonWithHoleN = (n, tester) -> {
      var entity = createTestOSMEntityRelation("type", "multipolygon");
      n -= 5; // outer shell is a simple bbox with 5 points
      var innerCoords = gf.createLinearRing(Streams.concat(
          LongStream.rangeClosed(1, n - 1).mapToObj(i -> new Coordinate(i, Math.pow(i, 2))),
          Stream.of(new Coordinate(1, 1))
      ).toArray(Coordinate[]::new));
      var geometry = gf.createPolygon(
          OSHDBGeometryBuilder.getGeometry(OSHDBBoundingBox
              .bboxWgs84Coordinates(-80, -80, 80, 80)).getExteriorRing(),
          new LinearRing[] { innerCoords });
      tester.accept(expression.applyOSMGeometry(entity, geometry));
    };
    testPolyonWithHoleN.accept(10, Assertions::assertFalse);
    testPolyonWithHoleN.accept(11, Assertions::assertTrue);
    testPolyonWithHoleN.accept(12, Assertions::assertTrue);
    testPolyonWithHoleN.accept(13, Assertions::assertTrue);
    testPolyonWithHoleN.accept(14, Assertions::assertFalse);
    // multi polygon
    BiConsumer<Integer, Consumer<Boolean>> testMultiPolyonN = (n, tester) -> {
      var entity = createTestOSMEntityRelation("type", "multipolygon");
      n -= 5; // outer shell 2 is a simple bbox with 5 points
      var coords = Streams.concat(
          LongStream.rangeClosed(1, n - 1).mapToObj(i -> new Coordinate(i, Math.pow(i, 2))),
          Stream.of(new Coordinate(1, 1))
      ).toArray(Coordinate[]::new);
      var geometry = gf.createMultiPolygon(new Polygon[] {
          getBoundingBoxPolygon(-2, -2, -1, -1),
          gf.createPolygon(coords) });
      tester.accept(expression.applyOSMGeometry(entity, geometry));
    };
    testMultiPolyonN.accept(10, Assertions::assertFalse);
    testMultiPolyonN.accept(11, Assertions::assertTrue);
    testMultiPolyonN.accept(12, Assertions::assertTrue);
    testMultiPolyonN.accept(13, Assertions::assertTrue);
    testMultiPolyonN.accept(14, Assertions::assertFalse);
  }

  @Test
  void testGeometryFilterOuters() {
    FilterExpression expression = parser.parse("geometry.outers:1");
    OSMEntity entity = createTestOSMEntityRelation("type", "multipolygon");
    assertFalse(expression.applyOSMGeometry(entity, gf.createMultiPolygon(new Polygon[] {
        getBoundingBoxPolygon(1, 1, 2, 2),
        getBoundingBoxPolygon(3, 3, 4, 4)
    })));
    assertTrue(expression.applyOSMGeometry(entity, gf.createMultiPolygon(new Polygon[] {
        getBoundingBoxPolygon(1, 1, 2, 2)
    })));
    assertTrue(expression.applyOSMGeometry(entity,
        getBoundingBoxPolygon(1, 1, 2, 2)
    ));
    // range
    expression = parser.parse("geometry.outers:(2..)");
    assertTrue(expression.applyOSMGeometry(entity, gf.createMultiPolygon(new Polygon[] {
        getBoundingBoxPolygon(1, 1, 2, 2),
        getBoundingBoxPolygon(3, 3, 4, 4)
    })));
  }

  @Test
  void testGeometryFilterInners() {
    FilterExpression expression = parser.parse("geometry.inners:0");
    OSMEntity entity = createTestOSMEntityRelation("type", "multipolygon");
    assertTrue(expression.applyOSMGeometry(entity,
        getBoundingBoxPolygon(1, 1, 2, 2)
    ));
    assertFalse(expression.applyOSMGeometry(entity, gf.createPolygon(
        OSHDBGeometryBuilder.getGeometry(
            OSHDBBoundingBox.bboxWgs84Coordinates(0, 0, 10, 10)).getExteriorRing(),
        new LinearRing[] { OSHDBGeometryBuilder.getGeometry(
            OSHDBBoundingBox.bboxWgs84Coordinates(1, 1, 2, 2)).getExteriorRing()
        })));
    assertTrue(expression.applyOSMGeometry(entity, gf.createMultiPolygon(new Polygon[] {
        getBoundingBoxPolygon(1, 1, 2, 2)
    })));
    assertFalse(expression.applyOSMGeometry(entity, gf.createMultiPolygon(new Polygon[] {
        gf.createPolygon(
            OSHDBGeometryBuilder.getGeometry(
                OSHDBBoundingBox.bboxWgs84Coordinates(0, 0, 10, 10)).getExteriorRing(),
            new LinearRing[] { OSHDBGeometryBuilder.getGeometry(
                OSHDBBoundingBox.bboxWgs84Coordinates(1, 1, 2, 2)).getExteriorRing()
            })
    })));
    // range
    expression = parser.parse("geometry.inners:(1..)");
    assertTrue(expression.applyOSMGeometry(entity, gf.createPolygon(
        OSHDBGeometryBuilder.getGeometry(
            OSHDBBoundingBox.bboxWgs84Coordinates(0, 0, 10, 10)).getExteriorRing(),
        new LinearRing[] { OSHDBGeometryBuilder.getGeometry(
            OSHDBBoundingBox.bboxWgs84Coordinates(1, 1, 2, 2)).getExteriorRing()
        })));
  }

  private final String regular32gon = "POLYGON ((1.0000004 0, 0.9807856 0.1950904, "
      + "0.9238799 0.3826836, 0.8314699 0.5555704, 0.707107 0.707107, 0.5555704 0.8314699, "
      + "0.3826836 0.9238799, 0.1950904 0.9807856, 0 1.0000004, -0.1950904 0.9807856, "
      + "-0.3826836 0.9238799, -0.5555704 0.8314699, -0.707107 0.707107, -0.8314699 0.5555704, "
      + "-0.9238799 0.3826836, -0.9807856 0.1950904, -1.0000004 0, -0.9807856 -0.1950904, "
      + "-0.9238799 -0.3826836, -0.8314699 -0.5555704, -0.707107 -0.707107, -0.5555704 -0.8314699, "
      + "-0.3826836 -0.9238799, -0.1950904 -0.9807856, 0 -1.0000004, 0.1950904 -0.9807856, "
      + "0.3826836 -0.9238799, 0.5555704 -0.8314699, 0.707107 -0.707107, 0.8314699 -0.5555704, "
      + "0.9238799 -0.3826836, 0.9807856 -0.1950904, 1.0000004 0))";

  @Test
  void testGeometryFilterRoundness() throws ParseException {
    FilterExpression expression = parser.parse("geometry.roundness:(0.8..)");
    OSMEntity entity = createTestOSMEntityWay(new long[] {});
    assertFalse(expression.applyOSMGeometry(entity,
        getBoundingBoxPolygon(0, 0, 1, 1)
    ));
    var reader = new WKTReader();
    assertTrue(expression.applyOSMGeometry(entity, reader.read(regular32gon)));
  }

  @Test
  void testGeometryFilterSqareness() throws ParseException {
    FilterExpression expression = parser.parse("geometry.squareness:(0.8..)");
    OSMEntity entity = createTestOSMEntityWay(new long[] {});
    assertTrue(expression.applyOSMGeometry(entity,
        getBoundingBoxPolygon(0, 0, 1, 1)
    ));
    var reader = new WKTReader();
    assertFalse(expression.applyOSMGeometry(entity, reader.read(regular32gon)));
  }
}
