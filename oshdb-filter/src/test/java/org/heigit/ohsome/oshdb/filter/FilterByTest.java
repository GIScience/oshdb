package org.heigit.ohsome.oshdb.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Tests helper methods for creating filters from lambda functions.
 */
public class FilterByTest extends FilterTest {
  private final GeometryFactory gf = new GeometryFactory();
  private final OSMNode testOSMEntity = createTestOSMEntityNode();
  private final OSHNode testOSHEntity = createTestOSHEntityNode(testOSMEntity);

  public FilterByTest() throws IOException {
  }

  @Test
  public void testFilterByOSMEntity() {
    FilterExpression expression;
    expression = Filter.byOSMEntity(e -> e.getVersion() == 1);
    assertTrue(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
    expression = Filter.byOSMEntity(e -> e.getVersion() != 1);
    assertFalse(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
  }

  @Test
  public void testFilterByOSHEntity() {
    FilterExpression expression;
    expression = Filter.byOSHEntity(e -> e.getId() == 1);
    assertTrue(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
    expression = Filter.byOSHEntity(e -> e.getId() != 1);
    assertFalse(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
  }

  @Test
  public void testFilterByCombined() {
    FilterExpression expression;
    expression = Filter.by(e -> e.getId() == 1, e -> e.getVersion() == 1);
    assertTrue(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
    expression = Filter.by(e -> e.getId() != 1, e -> e.getVersion() == 1);
    assertFalse(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
    expression = Filter.by(e -> e.getId() == 1, e -> e.getVersion() != 1);
    assertFalse(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
  }

  @Test
  public void testFilterByCombinedWithGeom() {
    final Geometry emptyPoint = gf.createPoint(new Coordinate(0, 0));
    FilterExpression expression;
    expression = Filter.by(e -> true, e -> true, (e, g) -> g.get() instanceof Point);
    assertTrue(expression.applyOSMGeometry(testOSMEntity, () -> emptyPoint));
    assertTrue(expression.applyOSMGeometry(testOSMEntity, emptyPoint));
    expression = Filter.by(e -> true, e -> true, (e, g) -> g.get() instanceof Polygon);
    assertFalse(expression.applyOSMGeometry(testOSMEntity, () -> emptyPoint));
    assertFalse(expression.applyOSMGeometry(testOSMEntity, emptyPoint));
  }
}
