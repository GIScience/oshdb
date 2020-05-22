package org.heigit.ohsome.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHWayImpl;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.junit.Test;

/**
 * Test class for the ohsome-filter package.
 *
 * <p>Tests the parsing of filters and the application to OSM entities.</p>
 */
public class ApplyOSHTest extends FilterTest {
  private OSHNode createTestEntityNode(OSMNode ...versions) throws IOException {
    return OSHNodeImpl.build(Arrays.asList(versions));
  }
  private OSHWay createTestEntityWay(OSMWay...versions) throws IOException {
    return OSHWayImpl.build(Arrays.asList(versions), Collections.emptyList());
  }
  private OSHRelation createTestEntityRelation(OSMRelation ...versions) throws IOException {
    return OSHRelationImpl.build(Arrays.asList(versions), Collections.emptyList(), Collections.emptyList());
  }

  @Test
  public void testTagFilterEquals() throws IOException {
    FilterExpression expression = parser.parse("highway=residential");
    // matching tag (exact match)
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "residential"))
    ));
    // matching tag (partial match)
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "track"))
    ));
    // 2 versions where one matches
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "track"),
        createTestEntityNode("building", "yes"))
    ));
    // no match
    assertFalse(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("building", "yes"))
    ));
  }

  @Test
  public void testTagFilterEqualsAny() throws IOException {
    FilterExpression expression = parser.parse("highway=*");
    // matching tag
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "residential"))
    ));
    // 2 versions where one matches
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "track"),
        createTestEntityNode("building", "yes"))
    ));
    // no match
    assertFalse(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("building", "yes"))
    ));
  }

  @Test
  public void testTagFilterNotEquals() throws IOException {
    FilterExpression expression = parser.parse("highway!=residential");
    // matching tag
    assertFalse(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "residential"))
    ));
    // 2 versions where one matches
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "residential"),
        createTestEntityNode("building", "yes"))
    ));
    // no match
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("building", "yes"))
    ));
  }

  @Test
  public void testTagFilterNotEqualsAny() throws IOException {
    FilterExpression expression = parser.parse("highway!=*");
    // matching tag
    assertFalse(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "residential"))
    ));
    // 2 versions where one matches
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "residential"),
        createTestEntityNode("building", "yes"))
    ));
    // no match
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("building", "yes"))
    ));
  }

  @Test
  public void testTagFilterEqualsAnyOf() throws IOException {
    FilterExpression expression = parser.parse("highway in (residential, track)");
    // matching tag
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "residential")
    )));
    // matching tag key only
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "primary")
    )));
    // no matching key
    assertFalse(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("building", "yes")
    )));
    // one exact matching in versions
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("building", "yes"),
        createTestEntityNode("highway", "track")
    )));
    // one partial matching in versions – should return true, even though no version actually matches
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("building", "yes"),
        createTestEntityNode("highway", "primary")
    )));
  }

  @Test
  public void testTypeFilter() throws IOException {
    assertTrue(parser.parse("type:node").applyOSH(createTestEntityNode(
        super.createTestEntityNode()
    )));
    assertFalse(parser.parse("type:way").applyOSH(createTestEntityNode(
        super.createTestEntityNode()
    )));
  }

  @Test
  public void testAndOperator() throws IOException {
    FilterExpression expression = parser.parse("highway=* and name=*");
    // exact match
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode(
            "highway", "residential",
            "name", "FIXME"
        )
    )));
    // one version matches
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode(
            "highway", "residential"
        ),
        createTestEntityNode(
            "highway", "residential",
            "name", "FIXME"
        )
    )));
    // no match, but OSH contains both tag keys because different versions have each one of them
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode(
            "highway", "residential"
        ),
        createTestEntityNode(
            "name", "FIXME"
        )
    )));
    // no match
    assertFalse(expression.applyOSH(createTestEntityNode(
        createTestEntityNode(
            "highway", "residential"
        )
    )));
    assertFalse(expression.applyOSH(createTestEntityNode(
        createTestEntityNode(
            "name", ""
        )
    )));
    assertFalse(expression.applyOSH(createTestEntityNode(
        super.createTestEntityNode()
    )));
  }

  @Test
  public void testOrOperator() throws IOException {
    FilterExpression expression = parser.parse("highway=* or name=*");
    // exact match
    assertTrue(expression.applyOSH(createTestEntityNode(
        createTestEntityNode("highway", "residential")
    )));
    // one version matches
    assertTrue(expression.applyOSH(createTestEntityNode(
        super.createTestEntityNode(),
        createTestEntityNode(
            "name", "FIXME"
        )
    )));
    // no match
    assertFalse(expression.applyOSH(createTestEntityNode(
        super.createTestEntityNode()
    )));
  }

  @Test
  public void testGeometryTypeFilterPoint() throws IOException {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression.applyOSH(createTestEntityNode(
        super.createTestEntityNode()
    )));
    assertFalse(expression.applyOSH(createTestEntityWay(
        createTestEntityWay(new long[] {})
    )));
    assertFalse(expression.applyOSH(createTestEntityRelation(
        super.createTestEntityRelation()
    )));
  }

  @Test
  public void testGeometryTypeFilterLine() throws IOException {
    FilterExpression expression = parser.parse("geometry:line");
    assertTrue(expression.applyOSH(createTestEntityWay(
        createTestEntityWay(new long[] {})
    )));
    assertTrue(expression.applyOSH(createTestEntityWay(
        createTestEntityWay(new long[] {1,2,3,4,1})
    )));
    assertFalse(expression.applyOSH(createTestEntityNode(
        super.createTestEntityNode()
    )));
    assertFalse(expression.applyOSH(createTestEntityRelation(
        super.createTestEntityRelation()
    )));
  }

  @Test
  public void testGeometryTypeFilterPolygon() throws IOException {
    FilterExpression expression = parser.parse("geometry:polygon");
    assertTrue(expression.applyOSH(createTestEntityWay(
        createTestEntityWay(new long[] {1,2,3,4,1})
    )));
    assertTrue(expression.applyOSH(createTestEntityRelation(
        createTestEntityRelation("type", "multipolygon")
    )));
    assertFalse(expression.applyOSH(createTestEntityNode(
        super.createTestEntityNode()
    )));
  }

  @Test
  public void testGeometryTypeFilterOther() throws IOException {
    FilterExpression expression = parser.parse("geometry:other");
    assertFalse(expression.applyOSH(createTestEntityWay(
        createTestEntityWay(new long[] {})
    )));
    assertFalse(expression.applyOSH(createTestEntityNode(
        super.createTestEntityNode()
    )));
    assertTrue(expression.applyOSH(createTestEntityRelation(
        super.createTestEntityRelation()
    )));
  }
}
