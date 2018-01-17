package org.heigit.bigspatialdata.oshdb.grid;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import static org.junit.Assert.assertEquals;

import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.junit.Test;

public class GridOSHRelationsTest {

  @Test
  public void test() throws IOException {
    OSHNode node100 = buildHOSMNode(
            Arrays.asList(new OSMNode(100l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{1, 2}, 494094984l, 86809727l)));
    OSHNode node102 = buildHOSMNode(
            Arrays.asList(new OSMNode(102l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{2, 1}, 494094984l, 86809727l)));
    OSHNode node104 = buildHOSMNode(
            Arrays.asList(new OSMNode(104l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{2, 4}, 494094984l, 86809727l)));

    OSHWay way200 = buildHOSMWay(Arrays.asList(new OSMWay(200, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)})), Arrays.asList(node100, node104));
    OSHWay way202 = buildHOSMWay(Arrays.asList(new OSMWay(202, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(102, OSMType.NODE, 0)})), Arrays.asList(node100, node102));

    OSHRelation relation300 = OSHRelation.build(Arrays.asList(//
            new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0, null), new OSMMember(102, OSMType.NODE, 0, null)}), //
            new OSMRelation(300, 2, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0, null), new OSMMember(102, OSMType.NODE, 0, null)})), //
            Arrays.asList(node100, node102), Arrays.asList());

    OSHRelation relation301 = OSHRelation.build(Arrays.asList(//
            new OSMRelation(301, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(200, OSMType.WAY, 1, null), new OSMMember(202, OSMType.WAY, 1, null)}), //
            new OSMRelation(301, 2, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 2}, new OSMMember[]{new OSMMember(200, OSMType.WAY, 1, null), new OSMMember(202, OSMType.WAY, 1, null)})), //
            Arrays.asList(), Arrays.asList(way200, way202));

    long cellId = 1;
    int cellLevel = 2;
    long baseId = 1234;

    GridOSHRelations hosmCell = GridOSHRelations.compact(cellId, cellLevel, baseId, 0, 0, 0, Arrays.asList(relation300, relation301));

    /*hosmCell.forEach(osh -> {
      OSHRelation oshRelation = (OSHRelation) osh;
      try {
        System.out.printf("%d (%s) %d\n", oshRelation.getId(), print((List<OSHEntity>) (List) oshRelation.getNodes()), oshRelation.getWays().size());
      } catch (IOException e) {
        e.printStackTrace();
      }
    });*/
    // todo: actually assert something in this test
  }

  private String print(List<OSHEntity> entity) {
    if (entity.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for (OSHEntity hosm : entity) {
      sb.append(hosm.getId());

      sb.append(',');
    }
    return sb.toString();
  }

  static OSHNode buildHOSMNode(List<OSMNode> versions) {
    try {
      return OSHNode.build(versions);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  static OSHWay buildHOSMWay(List<OSMWay> versions, List<OSHNode> nodes) {
    try {
      return OSHWay.build(versions, nodes);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Test
  public void testToString() throws IOException {
    OSHNode node100 = buildHOSMNode(
            Arrays.asList(new OSMNode(100l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{1, 2}, 494094984l, 86809727l)));
    OSHNode node102 = buildHOSMNode(
            Arrays.asList(new OSMNode(102l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{2, 1}, 494094984l, 86809727l)));
    OSHNode node104 = buildHOSMNode(
            Arrays.asList(new OSMNode(104l, 1, new OSHDBTimestamp(1l), 0l, 123, new int[]{2, 4}, 494094984l, 86809727l)));

    OSHWay way200 = buildHOSMWay(Arrays.asList(new OSMWay(200, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(104, OSMType.NODE, 0)})), Arrays.asList(node100, node104));
    OSHWay way202 = buildHOSMWay(Arrays.asList(new OSMWay(202, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0), new OSMMember(102, OSMType.NODE, 0)})), Arrays.asList(node100, node102));

    OSHRelation relation300 = OSHRelation.build(Arrays.asList(//
            new OSMRelation(300, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0, null), new OSMMember(102, OSMType.NODE, 0, null)}), //
            new OSMRelation(300, 2, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 2}, new OSMMember[]{new OSMMember(100, OSMType.NODE, 0, null), new OSMMember(102, OSMType.NODE, 0, null)})), //
            Arrays.asList(node100, node102), Arrays.asList());

    OSHRelation relation301 = OSHRelation.build(Arrays.asList(//
            new OSMRelation(301, 1, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{}, new OSMMember[]{new OSMMember(200, OSMType.WAY, 1, null), new OSMMember(202, OSMType.WAY, 1, null)}), //
            new OSMRelation(301, 2, new OSHDBTimestamp(3333l), 4444l, 23, new int[]{1, 2}, new OSMMember[]{new OSMMember(200, OSMType.WAY, 1, null), new OSMMember(202, OSMType.WAY, 1, null)})), //
            Arrays.asList(), Arrays.asList(way200, way202));

    long cellId = 2;
    int cellLevel = 2;
    long baseId = 1234;

    GridOSHRelations instance = GridOSHRelations.compact(cellId, cellLevel, baseId, 0, 0, 0, Arrays.asList(relation300, relation301));

    String expResult = "Grid-Cell of OSHRelations ID:2 Level:2 BBox:(-90.000000,0.000000),(-0.000000,90.000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }

}
