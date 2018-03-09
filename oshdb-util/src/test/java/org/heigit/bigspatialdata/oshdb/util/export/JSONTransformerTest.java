package org.heigit.bigspatialdata.oshdb.util.export;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.json.simple.parser.ParseException;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONTransformerTest {

  private static final Logger LOG = LoggerFactory.getLogger(JSONTransformerTest.class);
  public static final int USER_A = 1;
  public static final int USER_B = 2;
  public static final int[] TAGS_A = new int[] {1, 1};
  public static final int[] TAGS_B = new int[] {2, 2};
  public static final long[] LONLAT_A = new long[] {86756350l, 494186210l};
  public static final long[] LONLAT_B = new long[] {87153340l, 494102830l};

  public JSONTransformerTest() {}

  @Test
  public void testTransform_4args_101() throws SQLException, OSHDBKeytablesNotFoundException {
    int[] properties = {1, 2};
    OSMEntity entity = new OSMNode(1L, 1, new OSHDBTimestamp(0L), 1L, 1, properties, 1000000000L, 1000000000L);
    OSHDBTimestamp timestamp = new OSHDBTimestamp(1L);
    TagTranslator tt = new TagTranslator(
        DriverManager.getConnection("jdbc:h2:./src/test/resources/test-data", "sa", ""));
    String expResult =
        "{\"type\":\"Feature\",\"id\":\"node/1@1970-01-01T00:00:01Z\",\"properties\":{\"@type\":\"node\",\"@id\":1,\"@visible\":true,\"@version\":1,\"@changeset\":1,\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@geomtimestamp\":\"1970-01-01T00:00:01Z\",\"@uid\":1,\"building\":\"residential\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[100.0,100.0]}}";
    String result = JSONTransformer
        .transform(entity, timestamp, tt, new FakeTagInterpreter())
        .toString();
    assertEquals(expResult, result);

  }

  @Test
  public void testTransform_4args_102()
      throws SQLException, ClassNotFoundException, IOException, ParseException, OSHDBKeytablesNotFoundException {
    List<OSMNode> versions = new ArrayList<>(1);
    versions.add(new OSMNode(123l, 1, new OSHDBTimestamp(0l), 1l, USER_A, TAGS_A, LONLAT_A[0], LONLAT_A[1]));
    OSHNode hnode = OSHNode.build(versions);
    OSMMember part = new OSMMember(1L, OSMType.NODE, 0, hnode);
    OSMRelation instance =
        new OSMRelation(1L, 1, new OSHDBTimestamp(0L), 1L, 1, new int[] {1, 2}, new OSMMember[] {part, part});
    TagTranslator tt = new TagTranslator(
        DriverManager.getConnection("jdbc:h2:./src/test/resources/test-data", "sa", ""));
    String expResult =
        "{\"type\":\"Feature\",\"id\":\"relation/1@1970-01-01T00:00:01Z\",\"properties\":{\"@type\":\"relation\",\"@id\":1,\"@visible\":true,\"@version\":1,\"@changeset\":1,\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@geomtimestamp\":\"1970-01-01T00:00:01Z\",\"@uid\":1,\"building\":\"residential\",\"members\":[{\"type\":\"NODE\",\"ref\":1,\"role\":\"outer\"},{\"type\":\"NODE\",\"ref\":1,\"role\":\"outer\"}]},\"geometry\":{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Point\",\"coordinates\":[8.675635,49.418620999999995]},{\"type\":\"Point\",\"coordinates\":[8.675635,49.418620999999995]}]}}";

    String result = JSONTransformer
        .transform(instance, new OSHDBTimestamp(1L), tt,
            new DefaultTagInterpreter(DriverManager.getConnection(
                "jdbc:h2:./src/test/resources/test-data", "sa", ""
            )))
        .toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testTransform_4args_103()
      throws SQLException, ClassNotFoundException, IOException, OSHDBKeytablesNotFoundException {
    List<OSMNode> versions = new ArrayList<>(1);
    versions.add(new OSMNode(123l, 1, new OSHDBTimestamp(0l), 0l, USER_A, TAGS_A, LONLAT_A[0], LONLAT_A[1]));
    OSHNode hnode = OSHNode.build(versions);
    OSMMember part = new OSMMember(1L, OSMType.NODE, 1, hnode);
    OSMWay instance = new OSMWay(1L, 1, new OSHDBTimestamp(0L), 1L, 1, new int[] {1, 2}, new OSMMember[] {part, part});
    TagTranslator tt = new TagTranslator(
        DriverManager.getConnection("jdbc:h2:./src/test/resources/test-data", "sa", ""));
    String expResult =
        "{\"type\":\"Feature\",\"id\":\"way/1@1970-01-01T00:00:01Z\",\"properties\":{\"@type\":\"way\",\"@id\":1,\"@visible\":true,\"@version\":1,\"@changeset\":1,\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@geomtimestamp\":\"1970-01-01T00:00:01Z\",\"@uid\":1,\"building\":\"residential\",\"refs\":[1,1]},\"geometry\":{\"type\":\"LineString\",\"coordinates\":[[8.675635,49.418620999999995],[8.675635,49.418620999999995]]}}";

    String result = JSONTransformer
        .transform(instance, new OSHDBTimestamp(1L), tt, new FakeTagInterpreter())
        .toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testMultiTransform() throws SQLException, OSHDBKeytablesNotFoundException {
    int[] properties = {1, 2};
    OSMNode instance = new OSMNode(1L, 1, new OSHDBTimestamp(1L), 1L, 1, properties, 1000000000L, 1000000000L);
    List<Pair<? extends OSMEntity, OSHDBTimestamp>> osmObjects = new ArrayList<>(2);
    osmObjects.add(new ImmutablePair<>(instance, new OSHDBTimestamp(1L)));
    osmObjects.add(new ImmutablePair<>(instance, new OSHDBTimestamp(2L)));
    TagInterpreter areaDecider = new FakeTagInterpreter();
    TagTranslator tt = new TagTranslator(
        DriverManager.getConnection("jdbc:h2:./src/test/resources/test-data", "sa", ""));
    String expResult =
        "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"id\":\"node/1@1970-01-01T00:00:01Z\",\"properties\":{\"@type\":\"node\",\"@id\":1,\"@visible\":true,\"@version\":1,\"@changeset\":1,\"@timestamp\":\"1970-01-01T00:00:01Z\",\"@geomtimestamp\":\"1970-01-01T00:00:01Z\",\"@uid\":1,\"building\":\"residential\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[100.0,100.0]}},{\"type\":\"Feature\",\"id\":\"node/1@1970-01-01T00:00:02Z\",\"properties\":{\"@type\":\"node\",\"@id\":1,\"@visible\":true,\"@version\":1,\"@changeset\":1,\"@timestamp\":\"1970-01-01T00:00:01Z\",\"@geomtimestamp\":\"1970-01-01T00:00:02Z\",\"@uid\":1,\"building\":\"residential\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[100.0,100.0]}}]}";
    String result = JSONTransformer.multiTransform(osmObjects, tt, areaDecider).toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testTransform_4args_2()
      throws IOException, ClassNotFoundException, SQLException, OSHDBKeytablesNotFoundException {

    List<OSMNode> versions = new ArrayList<>(2);

    versions.add(new OSMNode(123l, 2, new OSHDBTimestamp(0l), 46l, 1, TAGS_A, LONLAT_A[0], LONLAT_A[1]));
    versions.add(new OSMNode(123l, 1, new OSHDBTimestamp(1l), 47l, 2, TAGS_B, LONLAT_B[0], LONLAT_B[1]));

    OSHNode instance = OSHNode.build(versions);

    Class.forName("org.h2.Driver");
    TagTranslator tt = new TagTranslator(DriverManager
        .getConnection("jdbc:h2:./src/test/resources/test-data;ACCESS_MODE_DATA=r", "sa", ""));
    String expResult =
        "{\"type\":\"FeatureCollection\",\"features\":[{\"type\":\"Feature\",\"id\":\"node/123@1970-01-01T00:00:00Z\",\"properties\":{\"@type\":\"node\",\"@id\":123,\"@visible\":true,\"@version\":2,\"@changeset\":46,\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@geomtimestamp\":\"1970-01-01T00:00:00Z\",\"@uid\":1,\"building\":\"house\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[8.675635,49.418620999999995]}},{\"type\":\"Feature\",\"id\":\"node/123@1970-01-01T00:00:01Z\",\"properties\":{\"@type\":\"node\",\"@id\":123,\"@visible\":true,\"@version\":1,\"@changeset\":47,\"@timestamp\":\"1970-01-01T00:00:01Z\",\"@geomtimestamp\":\"1970-01-01T00:00:01Z\",\"@uid\":2,\"highway\":\"unclassified\"},\"geometry\":{\"type\":\"Point\",\"coordinates\":[8.715334,49.410283]}}]}";
    String result = JSONTransformer
        .transform(instance, tt, new FakeTagInterpreter()).toString();
    assertEquals(expResult, result);
  }

  @Test
  public void testTransform_3args()
      throws IOException, SQLException, OSHDBKeytablesNotFoundException {
    List<OSHNode> hosmNodes = new ArrayList<>(3);
    for (int i = 0; i < 3; i++) {
      List<OSMNode> versions = new ArrayList<>(2);
      versions.add(new OSMNode(i + 1, 1, new OSHDBTimestamp(0), 1l, 1, new int[] {1, 2}, 86809727l - 1000000 * i,
          494094984l - 1000000 * i));
      versions.add(new OSMNode(i + 1, 2, new OSHDBTimestamp(0), 1l, 1, new int[] {1, 2}, 0L, 0L));
      hosmNodes.add(OSHNode.build(versions));
    }

    GridOSHNodes instance = GridOSHNodes.rebase(2, 2, 100, 100000l, 86000000, 490000000, hosmNodes);
    TagTranslator tt = new TagTranslator(
        DriverManager.getConnection("jdbc:h2:./src/test/resources/test-data", "sa", ""));
    String expResult = "{\"type\":\"FeatureCollection\"," + "\"features\":[{"
        + "\"type\":\"Feature\",\"id\":\"node/1@1970-01-01T00:00:00Z\",\"properties\":{\"@type\":\"node\",\"@id\":1,\"@visible\":true,\"@version\":2,\"@changeset\":1,\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@geomtimestamp\":\"1970-01-01T00:00:00Z\",\"@uid\":1,\"building\":\"residential\"},"
        + "\"geometry\":{\"type\":\"Point\",\"coordinates\":[0.0,0.0]}},{"
        + "\"type\":\"Feature\",\"id\":\"node/2@1970-01-01T00:00:00Z\",\"properties\":{\"@type\":\"node\",\"@id\":2,\"@visible\":true,\"@version\":2,\"@changeset\":1,\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@geomtimestamp\":\"1970-01-01T00:00:00Z\",\"@uid\":1,\"building\":\"residential\"},"
        + "\"geometry\":{\"type\":\"Point\",\"coordinates\":[0.0,0.0]}},{"
        + "\"type\":\"Feature\",\"id\":\"node/3@1970-01-01T00:00:00Z\",\"properties\":{\"@type\":\"node\",\"@id\":3,\"@visible\":true,\"@version\":2,\"@changeset\":1,\"@timestamp\":\"1970-01-01T00:00:00Z\",\"@geomtimestamp\":\"1970-01-01T00:00:00Z\",\"@uid\":1,\"building\":\"residential\"},"
        + "\"geometry\":{\"type\":\"Point\",\"coordinates\":[0.0,0.0]}}]}";
    String result = JSONTransformer
        .transform(instance, tt, new FakeTagInterpreter()).toString();
    assertEquals(expResult, result);

  }

}

class FakeTagInterpreter implements TagInterpreter {
  @Override
  public boolean isArea(OSMEntity entity) { return false; }
  @Override
  public boolean isLine(OSMEntity entity) { return false; }
  @Override
  public boolean hasInterestingTagKey(OSMEntity osm) { return false; }
  @Override
  public boolean isMultipolygonOuterMember(OSMMember osmMember) { return false; }
  @Override
  public boolean isMultipolygonInnerMember(OSMMember osmMember) { return false; }
  @Override
  public boolean isOldStyleMultipolygon(OSMRelation osmRelation) { return false; }
}