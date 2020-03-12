package org.heigit.ohsome.filter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTag;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Test class for the ohsome-filter package.
 *
 * <p>Tests the parsing of filters and the application to OSM entities.</p>
 */
public class ParserAndApplyOSMTest {
  private FilterParser parser;
  private TagTranslator tagTranslator;

  @Before
  public void setup() throws SQLException, ClassNotFoundException, OSHDBKeytablesNotFoundException {
    Class.forName("org.h2.Driver");
    this.tagTranslator = new TagTranslator(DriverManager.getConnection(
        "jdbc:h2:./src/test/resources/keytables;ACCESS_MODE_DATA=r",
        "sa", ""
    ));
    this.parser = new FilterParser(this.tagTranslator);
  }

  @After
  public void teardown() throws SQLException {
    this.tagTranslator.getConnection().close();
  }

  private int[] createTestTags(String ...keyValues) {
    ArrayList<Integer> tags = new ArrayList<>(keyValues.length);
    for (int i = 0; i < keyValues.length; i += 2) {
      OSHDBTag t = tagTranslator.getOSHDBTagOf(keyValues[i], keyValues[i + 1]);
      tags.add(t.getKey());
      tags.add(t.getValue());
    }
    return tags.stream().mapToInt(x -> x).toArray();
  }

  private OSMEntity createTestEntityNode(String ...keyValues) {
    return new OSMNode(1, 1, null, 1, 1, createTestTags(keyValues), 0, 0);
  }

  private OSMEntity createTestEntityWay(long[] nodeIds, String ...keyValues) {
    OSMMember[] refs = new OSMMember[nodeIds.length];
    for (int i = 0; i < refs.length; i++) {
      refs[i] = new OSMMember(nodeIds[i], OSMType.NODE, 0);
    }
    return new OSMWay(1, 1, null, 1, 1, createTestTags(keyValues), refs);
  }

  private OSMEntity createTestEntityRelation(String ...keyValues) {
    return new OSMRelation(1, 1, null, 1, 1, createTestTags(keyValues), new OSMMember[] {});
  }

  @Test
  public void testTagFilterEquals() {
    FilterExpression expression = parser.parse("highway=residential");
    assertTrue(expression instanceof TagFilterEquals);
    assertTrue(expression.applyOSM(createTestEntityNode("highway", "residential")));
    assertFalse(expression.applyOSM(createTestEntityNode("highway", "track")));
    assertFalse(expression.applyOSM(createTestEntityNode("building", "yes")));
  }

  @Test
  public void testTagFilterStrings() {
    // tag key with colon; quoted string as value
    assertTrue(parser.parse("addr:street=\"Hauptstraße\"") instanceof TagFilter);
    // whitespace (in string; between key and value, single quotes
    assertTrue(parser.parse("name = \"Colorado River\"") instanceof TagFilter);

    // "Allowed characters are: the letters `a-z` and `A-Z`, digits, underscore, dashes and colons."
    assertTrue(parser.parse("name=a0_-:") instanceof TagFilter);
  }

  @Test
  public void testTagFilterEqualsAny() {
    FilterExpression expression = parser.parse("highway=*");
    assertTrue(expression instanceof TagFilterEqualsAny);
    assertTrue(expression.applyOSM(createTestEntityNode("highway", "residential")));
    assertFalse(expression.applyOSM(createTestEntityNode("building", "yes")));
  }

  @Test
  public void testTagFilterNotEquals() {
    FilterExpression expression = parser.parse("highway!=residential");
    assertTrue(expression instanceof TagFilterNotEquals);
    assertFalse(expression.applyOSM(createTestEntityNode("highway", "residential")));
    assertTrue(expression.applyOSM(createTestEntityNode("highway", "track")));
    assertTrue(expression.applyOSM(createTestEntityNode("building", "yes")));
  }

  @Test
  public void testTagFilterNotEqualsAny() {
    FilterExpression expression = parser.parse("highway!=*");
    assertTrue(expression instanceof TagFilterNotEqualsAny);
    assertFalse(expression.applyOSM(createTestEntityNode("highway", "track")));
    assertTrue(expression.applyOSM(createTestEntityNode("building", "yes")));
  }

  @Test
  public void testTypeFilter() {
    assertTrue(parser.parse("type:node") instanceof TypeFilter);
    assertTrue(parser.parse("type:node").applyOSM(createTestEntityNode()));
    assertFalse(parser.parse("type:way").applyOSM(createTestEntityNode()));
  }

  @Test
  public void testAndOperator() {
    FilterExpression expression = parser.parse("highway=residential and name=*");
    assertTrue(expression instanceof AndOperator);
    assertTrue(((AndOperator) expression).getLeftOperand() instanceof TagFilter);
    assertTrue(((AndOperator) expression).getRightOperand() instanceof TagFilter);
    assertTrue(expression.applyOSM(createTestEntityNode(
        "highway", "residential",
        "name", "FIXME"
    )));
    assertFalse(expression.applyOSM(createTestEntityNode(
        "highway", "residential"
    )));
  }

  @Test
  public void testOrOperator() {
    FilterExpression expression = parser.parse("highway=residential or name=*");
    assertTrue(expression instanceof OrOperator);
    assertTrue(((OrOperator) expression).getLeftOperand() instanceof TagFilter);
    assertTrue(((OrOperator) expression).getRightOperand() instanceof TagFilter);
    assertTrue(expression.applyOSM(createTestEntityNode("highway", "residential")));
    assertTrue(expression.applyOSM(createTestEntityNode("name", "FIXME")));
    assertFalse(expression.applyOSM(createTestEntityNode("building", "yes")));
  }

  @Test
  public void testGeometryTypeFilterPoint() {
    FilterExpression expression = parser.parse("geometry:point");
    assertTrue(expression instanceof GeometryTypeFilter);
    // test expression.applyOSM
    assertTrue(expression.applyOSM(createTestEntityNode()));
    assertFalse(expression.applyOSM(createTestEntityWay(new long[] {})));
    assertFalse(expression.applyOSM(createTestEntityRelation()));
    // test expression.applyOSMGeometry
    GeometryFactory gf = new GeometryFactory();
    assertTrue(expression.applyOSMGeometry(createTestEntityNode(), gf.createPoint()));
  }

  @Test
  public void testGeometryTypeFilterLine() {
    FilterExpression expression = parser.parse("geometry:line");
    assertTrue(expression instanceof GeometryTypeFilter);
    // test expression.applyOSM
    assertTrue(expression.applyOSM(createTestEntityWay(new long[] {})));
    assertTrue(expression.applyOSM(createTestEntityWay(new long[] {1,2,3,4,1})));
    assertFalse(expression.applyOSM(createTestEntityNode()));
    assertFalse(expression.applyOSM(createTestEntityRelation()));
    // test expression.applyOSMGeometry
    GeometryFactory gf = new GeometryFactory();
    OSMEntity validWay = createTestEntityWay(new long[]{1, 2, 3, 4, 1});
    assertTrue(expression.applyOSMGeometry(validWay, gf.createLineString()));
    assertFalse(expression.applyOSMGeometry(validWay, gf.createPolygon()));
  }

  @Test
  public void testGeometryTypeFilterPolygon() {
    FilterExpression expression = parser.parse("geometry:polygon");
    assertTrue(expression instanceof GeometryTypeFilter);
    // test expression.applyOSM
    assertTrue(expression.applyOSM(createTestEntityWay(new long[] {1,2,3,1})));
    assertFalse(expression.applyOSM(createTestEntityWay(new long[] {1,2,3,4})));
    assertFalse(expression.applyOSM(createTestEntityWay(new long[] {1,2,1})));
    assertTrue(expression.applyOSM(createTestEntityRelation("type", "multipolygon")));
    assertFalse(expression.applyOSM(createTestEntityRelation()));
    assertFalse(expression.applyOSM(createTestEntityNode()));
    // test expression.applyOSMGeometry
    GeometryFactory gf = new GeometryFactory();
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
    assertTrue(expression instanceof GeometryTypeFilter);
    // test expression.applyOSM
    assertFalse(expression.applyOSM(createTestEntityWay(new long[] {})));
    assertFalse(expression.applyOSM(createTestEntityNode()));
    assertTrue(expression.applyOSM(createTestEntityRelation()));
    // test expression.applyOSMGeometry
    GeometryFactory gf = new GeometryFactory();
    OSMEntity validRelation = createTestEntityRelation();
    assertTrue(expression.applyOSMGeometry(validRelation, gf.createGeometryCollection()));
    assertFalse(expression.applyOSMGeometry(validRelation, gf.createPolygon()));
  }
}
