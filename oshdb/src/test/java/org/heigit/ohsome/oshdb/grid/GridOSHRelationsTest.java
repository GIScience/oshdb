package org.heigit.ohsome.oshdb.grid;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osh.OSHRelation;
import org.heigit.ohsome.oshdb.osh.OSHWay;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.junit.Test;

public class GridOSHRelationsTest {

  //  @Test
  //  public void test() throws IOException {
  //    OSHNode node100 = buildOSHNode(
  //        Arrays.asList(
  //            new OSMNode(100L, 1, 1L, 0L, 123, new int[] {1, 2}, 494094984L, 86809727L)));
  //    OSHNode node102 = buildOSHNode(
  //        Arrays.asList(
  //            new OSMNode(102L, 1, 1L, 0L, 123, new int[] {2, 1}, 494094984L, 86809727L)));
  //    OSHNode node104 = buildOSHNode(
  //        Arrays.asList(
  //            new OSMNode(104L, 1, 1L, 0L, 123, new int[] {2, 4}, 494094984L, 86809727L)));
  //
  //    OSHWay way200 =
  //        buildOSHWay(
  //            Arrays
  //                .asList(
  //                    new OSMWay(200, 1, 3333L, 4444L, 23, new int[] {1, 2},
  //                        new OSMMember[] {new OSMMember(100, OSMType.NODE, 0),
  //                            new OSMMember(104, OSMType.NODE, 0)})),
  //            Arrays.asList(node100, node104));
  //    OSHWay way202 =
  //        buildOSHWay(
  //            Arrays
  //                .asList(
  //                    new OSMWay(202, 1, 3333L, 4444L, 23, new int[] {1, 2},
  //                        new OSMMember[] {new OSMMember(100, OSMType.NODE, 0),
  //                            new OSMMember(102, OSMType.NODE, 0)})),
  //            Arrays.asList(node100, node102));
  //
  //    OSHRelation relation300 = OSHRelationImpl.build(Arrays.asList(//
  //        new OSMRelation(300, 1, 3333L, 4444L, 23, new int[] {},
  //            new OSMMember[] {new OSMMember(100, OSMType.NODE, 0, null),
  //                new OSMMember(102, OSMType.NODE, 0, null)}), //
  //        new OSMRelation(300, 2, 3333L, 4444L, 23, new int[] {1, 2},
  //            new OSMMember[] {new OSMMember(100, OSMType.NODE, 0, null),
  //                new OSMMember(102, OSMType.NODE, 0, null)})), //
  //        Arrays.asList(node100, node102), Arrays.asList());
  //
  //    OSHRelation relation301 = OSHRelationImpl.build(Arrays.asList(//
  //        new OSMRelation(301, 1, 3333L, 4444L, 23, new int[] {},
  //            new OSMMember[] {new OSMMember(200, OSMType.WAY, 1, null),
  //                new OSMMember(202, OSMType.WAY, 1, null)}), //
  //        new OSMRelation(301, 2, 3333L, 4444L, 23, new int[] {1, 2},
  //            new OSMMember[] {new OSMMember(200, OSMType.WAY, 1, null),
  //                new OSMMember(202, OSMType.WAY, 1, null)})), //
  //        Arrays.asList(), Arrays.asList(way200, way202));
  //
  //    long cellId = 1;
  //    int cellLevel = 2;
  //    long baseId = 1234;
  //
  //    GridOSHRelations hosmCell = GridOSHRelations.compact(cellId, cellLevel, baseId, 0, 0, 0,
  //        Arrays.asList(relation300, relation301));
  //
  //    /*
  //     * hosmCell.forEach(osh -> { OSHRelation oshRelation = (OSHRelation) osh; try {
  //     * System.out.printf("%d (%s) %d\n", oshRelation.getId(), print((List<OSHEntity>) (List)
  //     * oshRelation.getNodes()), oshRelation.getWays().size()); } catch (IOException e) {
  //     * e.printStackTrace(); } });
  //     */
  //    // todo: actually assert something in this test
  //  }

  static OSHNode buildOSHNode(List<OSMNode> versions) {
    try {
      return OSHNodeImpl.build(versions);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  static OSHWay buildOSHWay(List<OSMWay> versions, List<OSHNode> nodes) {
    try {
      return OSHWayImpl.build(versions, nodes);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Test
  public void testToString() throws IOException {
    OSHNode node100 = buildOSHNode(
        Arrays.asList(new OSMNode(100L, 1, 1L, 0L, 123, new int[] {1, 2}, 494094984L, 86809727L)));
    OSHNode node102 = buildOSHNode(
        Arrays.asList(new OSMNode(102L, 1, 1L, 0L, 123, new int[] {2, 1}, 494094984L, 86809727L)));
    OSHNode node104 = buildOSHNode(
        Arrays.asList(new OSMNode(104L, 1, 1L, 0L, 123, new int[] {2, 4}, 494094984L, 86809727L)));

    OSHWay way200 =
        buildOSHWay(
            Arrays
                .asList(
                    new OSMWay(200, 1, 3333L, 4444L, 23, new int[] {1, 2},
                        new OSMMember[] {new OSMMember(100, OSMType.NODE, 0),
                            new OSMMember(104, OSMType.NODE, 0)})),
            Arrays.asList(node100, node104));
    OSHWay way202 =
        buildOSHWay(
            Arrays
                .asList(
                    new OSMWay(202, 1, 3333L, 4444L, 23, new int[] {1, 2},
                        new OSMMember[] {new OSMMember(100, OSMType.NODE, 0),
                            new OSMMember(102, OSMType.NODE, 0)})),
            Arrays.asList(node100, node102));

    OSHRelation relation300 = OSHRelationImpl.build(Arrays.asList(//
        new OSMRelation(300, 1, 3333L, 4444L, 23, new int[] {},
            new OSMMember[] {new OSMMember(100, OSMType.NODE, 0, null),
                new OSMMember(102, OSMType.NODE, 0, null)}), //
        new OSMRelation(300, 2, 3333L, 4444L, 23, new int[] {1, 2},
            new OSMMember[] {new OSMMember(100, OSMType.NODE, 0, null),
                new OSMMember(102, OSMType.NODE, 0, null)})), //
        Arrays.asList(node100, node102), Arrays.asList());

    OSHRelation relation301 = OSHRelationImpl.build(Arrays.asList(//
        new OSMRelation(301, 1, 3333L, 4444L, 23, new int[] {},
            new OSMMember[] {new OSMMember(200, OSMType.WAY, 1, null),
                new OSMMember(202, OSMType.WAY, 1, null)}), //
        new OSMRelation(301, 2, 3333L, 4444L, 23, new int[] {1, 2},
            new OSMMember[] {new OSMMember(200, OSMType.WAY, 1, null),
                new OSMMember(202, OSMType.WAY, 1, null)})), //
        Arrays.asList(), Arrays.asList(way200, way202));

    long cellId = 2;
    int cellLevel = 2;
    long baseId = 1234;

    GridOSHRelations instance = GridOSHRelations.compact(cellId, cellLevel, baseId, 0, 0, 0,
        Arrays.asList(relation300, relation301));

    String expResult =
        "Grid-Cell of OSHRelations ID:2 Level:2 BBox:(-90.000000,0.000000),(-0.000000,90.000000)";
    String result = instance.toString();
    assertEquals(expResult, result);
  }
}
