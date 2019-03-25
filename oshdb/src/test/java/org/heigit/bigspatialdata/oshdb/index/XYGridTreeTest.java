package org.heigit.bigspatialdata.oshdb.index;

import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class XYGridTreeTest {

  @Test
  public void testGetIds() {
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
  public void testGetInsertId() {
    OSHDBBoundingBox bbox = new OSHDBBoundingBox(0.0, -90.0, 179.0, 90.0);
    XYGridTree instance = new XYGridTree(4);
    CellId expResult = new CellId(2, 2L);
    CellId result = instance.getInsertId(bbox);
    assertEquals(expResult.getId(), result.getId());
    assertEquals(expResult.getZoomLevel(), result.getZoomLevel());

    bbox = new OSHDBBoundingBox(0.0, -90.0, 0.1, 90.0);
    instance = new XYGridTree(4);
    expResult = new CellId(2, 2L);
    result = instance.getInsertId(bbox);
    assertEquals(expResult.getId(), result.getId());
    assertEquals(expResult.getZoomLevel(), result.getZoomLevel());

    bbox = new OSHDBBoundingBox(0.0, -90.0, 179.0, -89.9);
    instance = new XYGridTree(4);
    expResult = new CellId(2, 2L);
    result = instance.getInsertId(bbox);
    assertEquals(expResult.getId(), result.getId());
    assertEquals(expResult.getZoomLevel(), result.getZoomLevel());
  }

  @Test
  public void testBbox2CellIds_BoundingBox_boolean() {
    OSHDBBoundingBox BBOX = new OSHDBBoundingBox(0.0, 0.0, 44.9, 44.9);
    boolean enlarge = false;
    XYGridTree instance = new XYGridTree(3);
    HashSet<CellId> expectedCellIds = new HashSet<>(4);
    expectedCellIds.add(new CellId(3, 20L));
    expectedCellIds.add(new CellId(2, 6L));
    expectedCellIds.add(new CellId(1, 1L));
    expectedCellIds.add(new CellId(0, 0L));

    for (CellId now : instance.bbox2CellIds(BBOX, enlarge)) {
      assertEquals(true, expectedCellIds.remove(now));
    }
    assertEquals(0, expectedCellIds.size());
  }

  @Test
  public void testBbox2CellIds_BoundingBox2_boolean() {
    OSHDBBoundingBox bbox = new OSHDBBoundingBox(0.0, 0.0, 89, 89);
    boolean enlarge = true;
    XYGridTree instance = new XYGridTree(3);
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

    for (CellId now : instance.bbox2CellIds(bbox, enlarge)) {
      assertEquals(true, expectedCellIds.remove(now));
    }
    assertEquals(0, expectedCellIds.size());
  }

  @Test
  public void testBbox2CellIds_BoundingBox3_boolean() {
    //bounding box of cell 11 - 1625137
    OSHDBBoundingBox BBOX = new OSHDBBoundingBox(8.6132812, 49.3945312, 8.7890623, 49.5703123);
    boolean enlarge = false;
    XYGridTree instance = new XYGridTree(15);
    HashSet<CellId> expectedCellIds = new HashSet<>(4);
    CellId cellId = new CellId(11, 1625137L);
    
    expectedCellIds.add(cellId);

    int co = 0;
    for (CellId now : instance.bbox2CellIds(BBOX, enlarge)) {
      if (now.getZoomLevel() == 11) {
        //should only return one result in zoom level 11
        assertEquals("Did not expect to find this cell in the results: "+now.toString(),true, expectedCellIds.contains(now));
      } else if (now.getZoomLevel() == 12) {
        co += 1;
      }
    }
    //should have exactly 4 children
    assertEquals(4, co);
  }
  
    @Test
  public void testBbox2CellIds_BoundingBox4_boolean() {
    //bounding box of cell 11 - 1625137
    OSHDBBoundingBox BBOX = new OSHDBBoundingBox(8.6132812, 49.3945312, 8.7890623, 49.5703123);

    CellId cellId = new CellId(11, 1625137L);
    OSHDBBoundingBox boundingBox = XYGrid.getBoundingBox(cellId);
    
    assertEquals(BBOX, boundingBox);
  }

}
