package org.heigit.ohsome.oshdb.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;

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
  public void testFilterByOSHEntity() {
    FilterExpression expression;
    expression = Filter.byOSHEntity(e -> e.getId() == 1);
    assertTrue(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
    expression = expression.negate();
    assertFalse(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
  }

  @Test
  public void testFilterByOSMEntity() {
    FilterExpression expression;
    expression = Filter.byOSMEntity(e -> e.getVersion() == 1);
    assertTrue(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
    expression = expression.negate();
    assertFalse(expression.applyOSH(testOSHEntity) && expression.applyOSM(testOSMEntity));
  }

  @Test
  public void testFilterByOSMEntityApplyGeometryFallback() {
    FilterExpression expression = Filter.byOSMEntity(e -> e.getVersion() == 1);
    assertTrue(expression.applyOSMGeometry(testOSMEntity, gf.createPoint()));
    assertFalse(expression.negate().applyOSMGeometry(testOSMEntity, gf.createPoint()));
  }
}
