/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.heigit.bigspatialdata.hosmdb.util;

import java.util.HashSet;
import java.util.Iterator;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class XYGridTreeTest {

  @Test
  public void testGetIds() throws CellId.cellIdExeption {
    System.out.println("getIds");
    double longitude = 0.0;
    double latitude = 0.0;
    XYGridTree instance = new XYGridTree(4);
    CellId expResult = new CellId(2, 6L);
    Iterator<CellId> result = instance.getIds(longitude, latitude);
    result.next();
    result.next();
    CellId compare = result.next();
    assertEquals(expResult.getId(), compare.getId());
    assertEquals(expResult.getZoomLevel(), compare.getZoomLevel());
  }

  @Test
  public void testGetInsertId() throws CellId.cellIdExeption {
    System.out.println("getInsertId");
    BoundingBox bbox = new BoundingBox(0.0, 179.0, -90.0, 90.0);
    XYGridTree instance = new XYGridTree(4);
    CellId expResult = new CellId(2, 2L);
    CellId result = instance.getInsertId(bbox);
    assertEquals(expResult.getId(), result.getId());
    assertEquals(expResult.getZoomLevel(), result.getZoomLevel());
  }

  @Test
  public void testBbox2CellIds_MultiDimensionalNumericData_boolean() throws CellId.cellIdExeption {
    System.out.println("bbox2CellIds");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(0.0, 44.9), new NumericRange(0.0, 44.9)});
    boolean enlarge = false;
    XYGridTree instance = new XYGridTree(3);
    HashSet<CellId> expectedCellIds = new HashSet<>(4);
    expectedCellIds.add(new CellId(3, 20L));
    expectedCellIds.add(new CellId(2, 6L));
    expectedCellIds.add(new CellId(1, 1L));
    expectedCellIds.add(new CellId(0, 0L));

    Iterator<CellId> result = instance.bbox2CellIds(BBOX, enlarge);

    while (result.hasNext()) {
      CellId now = result.next();
      System.out.println(now.getId());
      System.out.println(now.getZoomLevel());
      assertEquals(true, expectedCellIds.remove(now));
    }
    assertEquals(0, expectedCellIds.size());
  }

  @Test
  public void testBbox2CellIds_BoundingBox_boolean() throws CellId.cellIdExeption {
    System.out.println("bbox2CellIds");
    BoundingBox bbox = new BoundingBox(0.0, 89, 0.0, 89);
    boolean enlarge = true;
    XYGridTree instance = new XYGridTree(3);
    HashSet<CellId> expectedCellIds = new HashSet<>(17);
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

    Iterator<CellId> result = instance.bbox2CellIds(bbox, enlarge);

    while (result.hasNext()) {
      CellId now = result.next();
      System.out.println(now.getId());
      System.out.println(now.getZoomLevel());
      assertEquals(true, expectedCellIds.remove(now));
    }
    assertEquals(0, expectedCellIds.size());
  }

}
