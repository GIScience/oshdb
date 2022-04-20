package org.heigit.ohsome.oshdb.grid;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.List;
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
import org.junit.jupiter.api.Test;

/**
 * General {@link GridOSHRelations} tests case.
 *
 */
public class GridOSHRelationsTest {

  @Test
  public void test() throws IOException {
    var node100 = buildOSHNode(node(100L, 1, 1L, 0L, 123, tags(1, 2), 494094984, 86809727));
    var node102 = buildOSHNode(node(102L, 1, 1L, 0L, 123, tags(2, 1), 494094984, 86809727));
    var node104 = buildOSHNode(node(104L, 1, 1L, 0L, 123, tags(2, 4), 494094984, 86809727));

    var way200 = buildOSHWay(asList(node100, node104),
            way(200, 1, 3333L, 4444L, 23, tags(1, 2), mn(100, 0), mn(104, 0)));
    var way202 = buildOSHWay(asList(node100, node102),
            way(202, 1, 3333L, 4444L, 23, tags(1, 2), mn(100, 0), mn(102, 0)));

    var relation300 = buildOSHRelation(asList(node100, node102), asList(),
        rel(300, 1, 3333L, 4444L, 23, tags(), mn(100, 0), mn(102, 0)),
        rel(300, 2, 3333L, 4444L, 23, tags(1, 2), mn(100, 0), mn(102, 0)));

    var relation301 = buildOSHRelation(asList(), asList(way200, way202),
        rel(301, 1, 3333L, 4444L, 23, tags(), mw(200, 1), mw(202, 1)),
        rel(301, 2, 3333L, 4444L, 23, tags(1, 2), mw(200, 1), mw(202, 1)));

    long cellId = 2;
    int cellLevel = 2;
    var grid = GridOSHRelations.compact(cellId, cellLevel, 0, 0, 0, 0,
        asList(relation300, relation301));;
    assertEquals(cellId, grid.getId());
    assertEquals(cellLevel, grid.getLevel());
    assertEquals(2, Iterables.size(grid.getEntities()));
    var itrExpected = asList(relation300, relation301).iterator();
    var itrActual = grid.getEntities().iterator();
    while (itrExpected.hasNext()) {
      assertEquals(true, itrActual.hasNext());
      assertEntityEquals(itrExpected.next(), (OSHRelation) itrActual.next());
    }
    assertEquals(false, itrActual.hasNext());
  }

  private static OSMNode node(long id, int version, long timestamp, long changeset,
      int userId, int[] tags, int longitude, int latitude) {
    return OSM.node(id, version, timestamp, changeset, userId, tags, longitude, latitude);
  }

  private static OSHNode buildOSHNode(OSMNode... versions) throws IOException {
    return OSHNodeImpl.build(asList(versions));
  }

  private static OSMWay way(long id, int version, long timestamp, long changeset,
      int userId, int[] tags, OSMMember... refs) {
    return OSM.way(id, version, timestamp, changeset, userId, tags, refs);
  }

  private static OSHWay buildOSHWay(List<OSHNode> nodes, OSMWay... versions) throws IOException {
    return OSHWayImpl.build(asList(versions), nodes);
  }

  private static OSMRelation rel(long id, int version, long timestamp, long changeset,
      int userId, int[] tags, OSMMember... refs) {
    return OSM.relation(id, version, timestamp, changeset, userId, tags, refs);
  }

  private static OSHRelation buildOSHRelation(List<OSHNode> nodes, List<OSHWay> ways,
      OSMRelation... versions) throws IOException {
    return OSHRelationImpl.build(asList(versions), nodes, ways);
  }

  private static void assertEntityEquals(OSHRelation a, OSHRelation b) {
    assertEquals(a.getId(), b.getId());
    var aitr = a.getVersions().iterator();
    var bitr = b.getVersions().iterator();
    while (aitr.hasNext()) {
      assertEquals(true, bitr.hasNext());
      assertEquals(aitr.next(), bitr.next());
    }
    assertEquals(false, bitr.hasNext());
  }

  private static int[] tags(int... kvs) {
    return kvs;
  }

  private static OSMMember mn(long id, int role) {
    return new OSMMember(id, OSMType.NODE, role);
  }

  private static OSMMember mw(long id, int role) {
    return new OSMMember(id, OSMType.WAY, role);
  }
}
