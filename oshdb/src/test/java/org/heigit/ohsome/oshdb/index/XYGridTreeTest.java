package org.heigit.ohsome.oshdb.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.util.CellId;
import org.junit.jupiter.api.Test;

@SuppressWarnings("checkstyle:abbreviationAsWordInName")
class XYGridTreeTest {

  @Test
  void testGetIds() {
    double longitude = 0.1;
    double latitude = 0.1;
    XYGridTree instance = new XYGridTree(4);
    Iterator<CellId> result = instance.getIds(longitude, latitude).iterator();
    Set<CellId> cellIds = Sets.newHashSet(result);
    assertEquals(5, cellIds.size());
    assertTrue(cellIds.contains(new CellId(0, 0L)));
    assertTrue(cellIds.contains(new CellId(1, 1L)));
    assertTrue(cellIds.contains(new CellId(2, 6L)));
    assertTrue(cellIds.contains(new CellId(3, 20L)));
    assertTrue(cellIds.contains(new CellId(4, 72L)));
  }

  @Test
  void testGetInsertId() {
    OSHDBBoundingBox bbox = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, -90.0, 179.0, 90.0);
    XYGridTree instance = new XYGridTree(4);
    CellId expResult = new CellId(2, 2L);
    CellId result = instance.getInsertId(bbox);
    assertEquals(expResult.getId(), result.getId());
    assertEquals(expResult.getZoomLevel(), result.getZoomLevel());

    bbox = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, -90.0, 0.1, 90.0);
    instance = new XYGridTree(4);
    expResult = new CellId(2, 2L);
    result = instance.getInsertId(bbox);
    assertEquals(expResult.getId(), result.getId());
    assertEquals(expResult.getZoomLevel(), result.getZoomLevel());

    bbox = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, -90.0, 179.0, -89.9);
    instance = new XYGridTree(4);
    expResult = new CellId(2, 2L);
    result = instance.getInsertId(bbox);
    assertEquals(expResult.getId(), result.getId());
    assertEquals(expResult.getZoomLevel(), result.getZoomLevel());
  }

  @Test
  void testBbox2CellIds_BoundingBox_boolean() {
    HashSet<CellId> expectedCellIds = new HashSet<>(4);
    expectedCellIds.add(new CellId(3, 20L));
    expectedCellIds.add(new CellId(2, 6L));
    expectedCellIds.add(new CellId(1, 1L));
    expectedCellIds.add(new CellId(0, 0L));

    OSHDBBoundingBox bbox = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 0.0, 44.9, 44.9);
    boolean enlarge = false;
    XYGridTree instance = new XYGridTree(3);
    for (CellId now : instance.bbox2CellIds(bbox, enlarge)) {
      assertEquals(true, expectedCellIds.remove(now));
    }
    assertEquals(0, expectedCellIds.size());
  }

  @Test
  void testBbox2CellIds_BoundingBox2_boolean() {
    HashSet<CellId> expectedCellIds = new HashSet<>(16);
    expectedCellIds.add(new CellId(3, 12L));
    expectedCellIds.add(new CellId(3, 11L));
    expectedCellIds.add(new CellId(3, 13L));
    expectedCellIds.add(new CellId(3, 19L));
    expectedCellIds.add(new CellId(3, 20L));
    expectedCellIds.add(new CellId(3, 21L));
    expectedCellIds.add(new CellId(3, 27L));
    expectedCellIds.add(new CellId(3, 28L));
    expectedCellIds.add(new CellId(3, 29L));
    expectedCellIds.add(new CellId(2, 1L));
    expectedCellIds.add(new CellId(2, 2L));
    expectedCellIds.add(new CellId(2, 5L));
    expectedCellIds.add(new CellId(2, 6L));
    expectedCellIds.add(new CellId(1, 1L));
    expectedCellIds.add(new CellId(1, 0L));
    expectedCellIds.add(new CellId(0, 0L));

    OSHDBBoundingBox bbox = OSHDBBoundingBox.bboxWgs84Coordinates(0.0, 0.0, 89, 89);
    boolean enlarge = true;
    XYGridTree instance = new XYGridTree(3);
    for (CellId now : instance.bbox2CellIds(bbox, enlarge)) {
      assertEquals(true, expectedCellIds.remove(now));
    }
    assertEquals(0, expectedCellIds.size());
  }

}
