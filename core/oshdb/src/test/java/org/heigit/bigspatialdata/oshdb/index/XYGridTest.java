package org.heigit.bigspatialdata.oshdb.index;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class XYGridTest {
  private static final int MAXZOOM = OSHDB.MAXZOOM;
  private XYGrid zero;
  private XYGrid two;
  private XYGrid thirty;

  @Before
  public void setUp() {
    zero = new XYGrid(0);
    two = new XYGrid(2);
    thirty = new XYGrid(30);
  }

  @Test
  public void testGetId_double_double() {
    double longitude = 0.0;
    double latitude = 0.0;
    XYGrid instance = new XYGrid(2);
    long expResult = 6L;
    long result = instance.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void testnegneg181_neg91_2() {
    // Testing Coordinates: -181, -91, zoom 2
    double longitude = -181.0;
    double latitude = -91.0;

    Long expResult = (long) -1;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void testneg180_neg90_0() {
    // Testing Coordinates: -180, -90, zoom 0
    double longitude = -180.0;
    double latitude = -90.0;

    Long expResult = (long) 0;
    Long result = zero.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test180_90_0() {
    // Testing Coordinates: 180, 90, zoom 0
    double longitude = 180.0;
    double latitude = 90.0;

    Long expResult = (long) 0;
    Long result = zero.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test179_90_0() {
    // Testing Coordinates: 179, 90, zoom 0
    double longitude = 179.0;
    double latitude = 90.0;

    Long expResult = (long) 0;
    Long result = zero.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void testneg180_neg90_2() {
    // Testing Coordinates: -180, -90, zoom 2
    double longitude = -180.0;
    double latitude = -90.0;

    Long expResult = (long) 0;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test180_90_2() {
    // Testing Coordinates: 180, 90, zoom 2
    double longitude = 180.0;
    double latitude = 90.0;

    Long expResult = (long) 4;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test179_90_2() {
    // Testing Coordinates: 179, 90, zoom 2
    double longitude = 179.99999999999;
    double latitude = 90.0;

    Long expResult = (long) 7;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void testneg180_neg90_31() {
    // Testing Coordinates: -180, -90, zoom 31
    double longitude = -180.0;
    double latitude = -90.0;
    int zoom = 31;
    XYGrid testclass = new XYGrid(zoom);

    Long expResult = 0L;
    Long result = testclass.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test180_90_neg1() {
    // Testing Coordinates: 180, 90, zoom -1
    double longitude = 180.0;
    double latitude = 90.0;
    int zoom = -1;
    XYGrid testclass = new XYGrid(zoom);

    Long expResult = 0L;
    Long result = testclass.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void testneg180_neg90_30() {
    // Testing Coordinates: -180, -90, zoom 30
    double longitude = -180.0;
    double latitude = -90.0;

    Long expResult = (long) 0;
    Long result = thirty.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test180_90_30() {
    // Testing Coordinates: 180, 90, zoom 30
    double longitude = 180.0;
    double latitude = 90.0;

    Long expResult = 576460751229681664L;
    Long result = thirty.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test179_90_30() {
    // Testing Coordinates: 179, 90, zoom 30
    double longitude = 179.999999999;
    double latitude = 90.0;

    Long expResult = 576460752303423487L;
    Long result = thirty.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetId_NumericRange_NumericRange() {
    NumericRange longitudes = new NumericRange(-10.0, 10.0);
    NumericRange latitudes = new NumericRange(-10.0, 10.0);
    XYGrid instance = new XYGrid(2);
    long expResult = 1L;
    long result = instance.getId(longitudes, latitudes);
    assertEquals(expResult, result);

    longitudes = new NumericRange(10.0, -9.0);
    latitudes = new NumericRange(-10.0, 10.0);
    instance = new XYGrid(2);
    expResult = 2L;
    result = instance.getId(longitudes, latitudes);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetCellWidth() {
    double expResult = 90;

    double result = two.getCellWidth();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetCellDimensions() {
    long cellId = 0L;
    MultiDimensionalNumericData expResult = new BasicNumericDataset(new NumericData[]{new NumericRange(-180.0, -90.00000000001), new NumericRange(-90.0, -0.00000000001)});
    MultiDimensionalNumericData result = two.getCellDimensions(cellId);
    org.junit.Assert.assertTrue(Arrays.equals(result.getMaxValuesPerDimension(), expResult.getMaxValuesPerDimension()) && Arrays.equals(result.getMinValuesPerDimension(), expResult.getMinValuesPerDimension()));

    cellId = 6L;
    expResult = new BasicNumericDataset(new NumericData[]{new NumericRange(0.0, 89.99999999999), new NumericRange(0.0, 90.0)});
    result = two.getCellDimensions(cellId);
    org.junit.Assert.assertTrue(Arrays.equals(result.getMaxValuesPerDimension(), expResult.getMaxValuesPerDimension()) && Arrays.equals(result.getMinValuesPerDimension(), expResult.getMinValuesPerDimension()));

    cellId = 7L;
    expResult = new BasicNumericDataset(new NumericData[]{new NumericRange(90.0, 179.99999999999), new NumericRange(0.0, 90.0)});
    result = two.getCellDimensions(cellId);
    org.junit.Assert.assertTrue(Arrays.equals(result.getMaxValuesPerDimension(), expResult.getMaxValuesPerDimension()) && Arrays.equals(result.getMinValuesPerDimension(), expResult.getMinValuesPerDimension()));

    cellId = 0L;
    expResult = new BasicNumericDataset(new NumericData[]{new NumericRange(-180.0, 179.99999999999), new NumericRange(-90.0, 90.0)});
    result = zero.getCellDimensions(cellId);
    org.junit.Assert.assertTrue(Arrays.equals(result.getMaxValuesPerDimension(), expResult.getMaxValuesPerDimension()) && Arrays.equals(result.getMinValuesPerDimension(), expResult.getMinValuesPerDimension()));

    cellId = 0L;
    expResult = new BasicNumericDataset(new NumericData[]{new NumericRange(-180.0, -0.00000000001), new NumericRange(-90.0, 90.0)});
    XYGrid instance = new XYGrid(1);
    result = instance.getCellDimensions(cellId);
    org.junit.Assert.assertTrue(Arrays.equals(result.getMaxValuesPerDimension(), expResult.getMaxValuesPerDimension()) && Arrays.equals(result.getMinValuesPerDimension(), expResult.getMinValuesPerDimension()));
  }

  @Test
  public void testEqualsEpsilon() {
    double a = 0.00000000001;
    double b = 0.00000000002;
    boolean expResult = true;
    boolean result = XYGrid.equalsEpsilon(a, b);
    assertEquals(expResult, result);

    a = 0.0000000001;
    b = 0.0000000002;
    expResult = false;
    result = XYGrid.equalsEpsilon(a, b);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetEstimatedIdCount() {
    MultiDimensionalNumericData data = new BasicNumericDataset(new NumericData[]{new NumericRange(0, 89), new NumericRange(0, 89)});
    long expResult = 1L;
    long result = two.getEstimatedIdCount(data);
    org.junit.Assert.assertTrue(Math.abs(expResult - result) <= two.getLevel());

    data = new BasicNumericDataset(new NumericData[]{new NumericRange(-89.0, 89.0), new NumericRange(-90.0, 90.0)});
    expResult = 4L;
    result = two.getEstimatedIdCount(data);
    org.junit.Assert.assertTrue(Math.abs(expResult - result) <= two.getLevel());

    data = new BasicNumericDataset(new NumericData[]{new NumericRange(0.0, 0.000005364), new NumericRange(0.0, 0.000005364)});
    expResult = 256L;
    result = thirty.getEstimatedIdCount(data);
    org.junit.Assert.assertTrue(Math.abs(expResult - result) <= two.getLevel());
  }

  @Test
  public void testGetLevel() {
    int expResult = 2;
    int result = two.getLevel();
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2Ids() {
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-180, 180), new NumericRange(-90, 90)});
    Set<Pair<Long, Long>> result = zero.bbox2CellIdRanges(BBOX, false);
    assertEquals(1, result.size());
    Pair<Long, Long> interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());

    BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-180, 180), new NumericRange(-90, 90)});
    result = two.bbox2CellIdRanges(BBOX, false);
    assertEquals(2, result.size());
    interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals(3, interval.getRight().longValue());

    BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-10, 10), new NumericRange(-10, 10)});
    result = zero.bbox2CellIdRanges(BBOX, false);
    assertEquals(1, result.size());
    interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());

    BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(179, 89), new NumericRange(0, 5)});
    result = zero.bbox2CellIdRanges(BBOX, false);
    assertEquals(1, result.size());
    interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());

    BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-10, 10), new NumericRange(-10, 10)});
    TreeSet<Long> expectedCellIds = new TreeSet<>();
    expectedCellIds.add(1L);
    expectedCellIds.add(2L);
    expectedCellIds.add(5L);
    expectedCellIds.add(6L);
    result = two.bbox2CellIdRanges(BBOX, false);
    for (Pair<Long, Long> interval2 : result) {
      for (long cellId = interval2.getLeft(); cellId <= interval2.getRight(); cellId++) {
        assertEquals(true, expectedCellIds.remove(cellId));
      }
    }
    assertEquals(0, expectedCellIds.size());

    BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-180, 89), new NumericRange(0, 5)});
    expectedCellIds = new TreeSet<>();
    expectedCellIds.add(4L);
    expectedCellIds.add(5L);
    expectedCellIds.add(6L);

    result = two.bbox2CellIdRanges(BBOX, false);
    for (Pair<Long, Long> interval2 : result) {
      for (long cellId = interval2.getLeft(); cellId <= interval2.getRight(); cellId++) {
        assertEquals(true, expectedCellIds.remove(cellId));
      }
    }
    assertEquals(0, expectedCellIds.size());

    BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(90, 89), new NumericRange(-90, -1)});
    expectedCellIds = new TreeSet<>();
    expectedCellIds.add(0L);
    expectedCellIds.add(1L);
    expectedCellIds.add(2L);
    expectedCellIds.add(3L);

    result = two.bbox2CellIdRanges(BBOX, false);
    for (Pair<Long, Long> interval2 : result) {
      for (long cellId = interval2.getLeft(); cellId <= interval2.getRight(); cellId++) {
        assertEquals(true, expectedCellIds.remove(cellId));
      }
    }
    assertEquals(0, expectedCellIds.size());

    result = two.bbox2CellIdRanges(two.getCellDimensions(0), false);
    assertEquals(1, result.size());
    interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());

    // test performance for maximum sized BBOX
    BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-180, 180), new NumericRange(-90, 90)});
    int expResult = 2048;
    result = new XYGrid(MAXZOOM).bbox2CellIdRanges(BBOX, true);
    assertEquals(expResult, result.size());
    interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals(4095, interval.getRight().longValue());
  }

  @Test
  public void testGetNeighbours() throws CellId.cellIdExeption {
    CellId center = new CellId(2, 6L);
    Set<Pair<Long, Long>> expResult = new TreeSet<>();
    expResult.add(new ImmutablePair<>(1L, 3L));
    expResult.add(new ImmutablePair<>(5L, 7L));
    expResult.add(new ImmutablePair<>(-1L, -1L));
    Set<Pair<Long, Long>> result = two.getNeighbours(center);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetBoundingBox() throws CellId.cellIdExeption{
    BoundingBox result=XYGrid.getBoundingBox(new CellId(2,2));
    BoundingBox expResult=new BoundingBox(0,90,-90,0-1e-11);
    assertEquals(expResult.toString(),result.toString());
  }
}
