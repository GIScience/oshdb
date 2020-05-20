package org.heigit.ohsome.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Test class for the ohsome-filter package.
 *
 * <p>Tests the parsing of filters and the application to OSM entities.</p>
 */
public class ApplyOSMGeometryTest extends FilterTest {
  GeometryFactory gf = new GeometryFactory();

  @Test
  public void testGeometryTypeFilterPoint() {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression.applyOSMGeometry(createTestEntityNode(), gf.createPoint()));
  }

  @Test
  public void testGeometryTypeFilterLine() {
    FilterExpression expression = parser.parse("geometry:line");
    OSMEntity validWay = createTestEntityWay(new long[]{1, 2, 3, 4, 1});
    assertTrue(expression.applyOSMGeometry(validWay, gf.createLineString()));
    assertFalse(expression.applyOSMGeometry(validWay, gf.createPolygon()));
  }

  @Test
  public void testGeometryTypeFilterPolygon() {
    FilterExpression expression = parser.parse("geometry:polygon");
    OSMEntity validWay = createTestEntityWay(new long[]{1, 2, 3, 4, 1});
    assertTrue(expression.applyOSMGeometry(validWay, gf.createPolygon()));
    assertFalse(expression.applyOSMGeometry(validWay, gf.createLineString()));
    OSMEntity validRelation = createTestEntityRelation("type", "multipolygon");
    assertTrue(expression.applyOSMGeometry(validRelation, gf.createPolygon()));
    assertFalse(expression.applyOSMGeometry(validRelation, gf.createGeometryCollection()));
  }

  @Test
  public void testGeometryTypeFilterOther() {
    FilterExpression expression = parser.parse("geometry:other");
    OSMEntity validRelation = createTestEntityRelation();
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
        createTestEntityNode("name", "FIXME"),
        gf.createPoint()
    ));
    assertFalse(expression.applyOSMGeometry(
        createTestEntityWay(new long[] {}, "name", "FIXME"),
        gf.createLineString()
    ));
    assertFalse(expression.applyOSMGeometry(
        createTestEntityNode(),
        gf.createPoint()
    ));
  }

  @Test
  public void testOrOperator() {
    FilterExpression expression = parser.parse("geometry:point or geometry:polygon");
    assertTrue(expression.applyOSMGeometry(
        createTestEntityNode(),
        gf.createPoint()
    ));
    assertTrue(expression.applyOSMGeometry(
        createTestEntityWay(new long[] {1,2,3,4,1}),
        gf.createPolygon()
    ));
    assertFalse(expression.applyOSMGeometry(
        createTestEntityWay(new long[] {1,2,3,4,1}),
        gf.createLineString()
    ));
  }
}
