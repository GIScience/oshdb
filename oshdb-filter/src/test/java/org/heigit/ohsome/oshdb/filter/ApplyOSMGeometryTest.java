package org.heigit.ohsome.oshdb.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Tests the parsing of filters and the application to OSM entities.
 */
public class ApplyOSMGeometryTest extends FilterTest {
  private final GeometryFactory gf = new GeometryFactory();

  @Test
  public void testGeometryTypeFilterPoint() {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression.applyOSMGeometry(createTestOSMEntityNode(), gf.createPoint()));
  }

  @Test
  public void testGeometryTypeFilterLine() {
    FilterExpression expression = parser.parse("geometry:line");
    OSMEntity validWay = createTestOSMEntityWay(new long[]{1, 2, 3, 4, 1});
    assertTrue(expression.applyOSMGeometry(validWay, gf.createLineString()));
    assertFalse(expression.applyOSMGeometry(validWay, gf.createPolygon()));
  }

  @Test
  public void testGeometryTypeFilterPolygon() {
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
  public void testGeometryTypeFilterOther() {
    FilterExpression expression = parser.parse("geometry:other");
    OSMEntity validRelation = createTestOSMEntityRelation();
    assertTrue(expression.applyOSMGeometry(validRelation, gf.createGeometryCollection()));
    assertFalse(expression.applyOSMGeometry(validRelation, gf.createPolygon()));
    assertFalse(expression.applyOSMGeometry(validRelation, gf.createMultiPoint()));
    assertFalse(expression.applyOSMGeometry(validRelation, gf.createMultiLineString()));
    assertFalse(expression.applyOSMGeometry(validRelation, gf.createMultiPolygon()));
  }

  @Test
  public void testAndOperator() {
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
  public void testOrOperator() {
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
  public void testGeometryFilterArea() {
    FilterExpression expression = parser.parse("area:(1..2)");
    OSMEntity entity = createTestOSMEntityWay(new long[] {1, 2, 3, 4, 1});
    assertFalse(expression.applyOSMGeometry(entity,
        // approx 0.3m²
        OSHDBGeometryBuilder.getGeometry(new OSHDBBoundingBox(0, 0, 5E-6, 5E-6))
    ));
    assertTrue(expression.applyOSMGeometry(entity,
        // approx 1.2m²
        OSHDBGeometryBuilder.getGeometry(new OSHDBBoundingBox(0, 0, 1E-5, 1E-5))
    ));
    assertFalse(expression.applyOSMGeometry(entity,
        // approx 4.9m²
        OSHDBGeometryBuilder.getGeometry(new OSHDBBoundingBox(0, 0, 2E-5, 2E-5))
    ));
  }

  @Test
  public void testGeometryFilterLength() {
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
  }
}
