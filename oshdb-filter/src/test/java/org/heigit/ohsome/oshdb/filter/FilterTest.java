package org.heigit.ohsome.oshdb.filter;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;


/**
 * Tests the parsing of filters and the application to OSM entities.
 */
abstract class FilterTest {
  protected FilterParser parser;
  protected TagTranslator tagTranslator;

  @BeforeEach
  public void setup() throws SQLException, ClassNotFoundException, OSHDBKeytablesNotFoundException {
    Class.forName("org.h2.Driver");
    this.tagTranslator = new TagTranslator(DriverManager.getConnection(
        "jdbc:h2:./src/test/resources/keytables;ACCESS_MODE_DATA=r",
        "sa", ""
    ));
    this.parser = new FilterParser(this.tagTranslator);
  }

  @AfterEach
  public void teardown() throws SQLException {
    this.tagTranslator.getConnection().close();
  }

  protected int[] createTestTags(String... keyValues) {
    ArrayList<Integer> tags = new ArrayList<>(keyValues.length);
    for (int i = 0; i < keyValues.length; i += 2) {
      OSHDBTag t = tagTranslator.getOSHDBTagOf(keyValues[i], keyValues[i + 1]);
      tags.add(t.getKey());
      tags.add(t.getValue());
    }
    return tags.stream().mapToInt(x -> x).toArray();
  }

  protected OSMNode createTestOSMEntityNode(String... keyValues) {
    return createTestOSMEntityNode(1, 1, keyValues);
  }

  protected OSMNode createTestOSMEntityNode(long changesetId, int userId, String... keyValues) {
    return OSM.node(1, 1, 0L, changesetId, userId, createTestTags(keyValues), 0, 0);
  }

  protected OSMWay createTestOSMEntityWay(long[] nodeIds, String... keyValues) {
    return createTestOSMEntityWay(1, 1, nodeIds, keyValues);
  }

  protected OSMWay createTestOSMEntityWay(
      long changesetId, int userId, long[] nodeIds, String... keyValues) {
    OSMMember[] refs = new OSMMember[nodeIds.length];
    for (int i = 0; i < refs.length; i++) {
      refs[i] = new OSMMember(nodeIds[i], OSMType.NODE, 0);
    }
    return OSM.way(1, 1, 0L, changesetId, userId, createTestTags(keyValues), refs);
  }

  protected OSMRelation createTestOSMEntityRelation(String... keyValues) {
    return createTestOSMEntityRelation(1, 1, keyValues);
  }

  protected OSMRelation createTestOSMEntityRelation(
      long changesetId, int userId, String... keyValues) {
    return OSM.relation(1, 1, 0L, changesetId, userId, createTestTags(keyValues),
        new OSMMember[] {});
  }

  protected OSHNode createTestOSHEntityNode(OSMNode... versions) throws IOException {
    return OSHNodeImpl.build(Arrays.asList(versions));
  }

  protected OSHWay createTestOSHEntityWay(OSMWay...versions) throws IOException {
    return createTestOSHEntityWay(versions, new OSHNode[] {});
  }

  protected OSHWay createTestOSHEntityWay(
      OSMWay[] versions, OSHNode[] referencedNodes) throws IOException {
    return OSHWayImpl.build(Arrays.asList(versions), Arrays.asList(referencedNodes));
  }

  protected OSHRelation createTestOSHEntityRelation(OSMRelation... versions) throws IOException {
    return createTestOSHEntityRelation(versions, new OSHNode[] {}, new OSHWay[] {});
  }

  protected OSHRelation createTestOSHEntityRelation(
      OSMRelation[] versions, OSHNode[] nodes, OSHWay[] ways) throws IOException {
    return OSHRelationImpl.build(Arrays.asList(versions), Arrays.asList(nodes),
        Arrays.asList(ways));
  }
}
