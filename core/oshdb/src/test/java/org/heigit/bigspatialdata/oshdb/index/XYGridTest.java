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
    System.out.println("getId");
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
    System.out.println("getId");
    NumericRange longitudes = new NumericRange(-10.0, 10.0);
    NumericRange latitudes = new NumericRange(-10.0, 10.0);
    XYGrid instance = new XYGrid(2);
    long expResult = 1L;
    long result = instance.getId(longitudes, latitudes);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetId_NumericRange_NumericRangeII() {
    System.out.println("getId");
    NumericRange longitudes = new NumericRange(10.0, -9.0);
    NumericRange latitudes = new NumericRange(-10.0, 10.0);
    XYGrid instance = new XYGrid(2);
    long expResult = 2L;
    long result = instance.getId(longitudes, latitudes);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetCellWidth() {
    System.out.println("getCellWidth");
    double expResult = 90;

    double result = two.getCellWidth();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  public void testGetCellDimensions() {
    System.out.println("getCellDimensions");
    long cellId = 0L;
    MultiDimensionalNumericData expResult = new BasicNumericDataset(new NumericData[]{new NumericRange(180.0, -90.00000000001), new NumericRange(-90.0, -0.00000000001)});

    MultiDimensionalNumericData result = two.getCellDimensions(cellId);
    org.junit.Assert.assertTrue(Arrays.equals(result.getMaxValuesPerDimension(), expResult.getMaxValuesPerDimension()) && Arrays.equals(result.getMinValuesPerDimension(), expResult.getMinValuesPerDimension()));
  }

  @Test
  public void testGetCellDimensionsII() {
    System.out.println("getCellDimensions");
    long cellId = 6L;
    MultiDimensionalNumericData expResult = new BasicNumericDataset(new NumericData[]{new NumericRange(0.0, 89.99999999999), new NumericRange(0.0, 90.0)});

    MultiDimensionalNumericData result = two.getCellDimensions(cellId);
    org.junit.Assert.assertTrue(Arrays.equals(result.getMaxValuesPerDimension(), expResult.getMaxValuesPerDimension()) && Arrays.equals(result.getMinValuesPerDimension(), expResult.getMinValuesPerDimension()));
  }

  @Test
  public void testGetCellDimensionsIII() {
    System.out.println("getCellDimensions");
    long cellId = 7L;
    MultiDimensionalNumericData expResult = new BasicNumericDataset(new NumericData[]{new NumericRange(90.0, 179.99999999999), new NumericRange(0.0, 90.0)});

    MultiDimensionalNumericData result = two.getCellDimensions(cellId);
    org.junit.Assert.assertTrue(Arrays.equals(result.getMaxValuesPerDimension(), expResult.getMaxValuesPerDimension()) && Arrays.equals(result.getMinValuesPerDimension(), expResult.getMinValuesPerDimension()));
  }

  @Test
  public void testGetCellDimensionsV() {
    System.out.println("getCellDimensions");
    long cellId = 0L;
    MultiDimensionalNumericData expResult = new BasicNumericDataset(new NumericData[]{new NumericRange(180.0, 179.99999999999), new NumericRange(-90.0, 90.0)});

    MultiDimensionalNumericData result = zero.getCellDimensions(cellId);
    org.junit.Assert.assertTrue(Arrays.equals(result.getMaxValuesPerDimension(), expResult.getMaxValuesPerDimension()) && Arrays.equals(result.getMinValuesPerDimension(), expResult.getMinValuesPerDimension()));
  }

  @Test
  public void testGetCellDimensionsVI() {
    System.out.println("getCellDimensions");
    long cellId = 0L;
    MultiDimensionalNumericData expResult = new BasicNumericDataset(new NumericData[]{new NumericRange(180.0, -0.00000000001), new NumericRange(-90.0, 90.0)});
    XYGrid instance = new XYGrid(1);

    MultiDimensionalNumericData result = instance.getCellDimensions(cellId);
    org.junit.Assert.assertTrue(Arrays.equals(result.getMaxValuesPerDimension(), expResult.getMaxValuesPerDimension()) && Arrays.equals(result.getMinValuesPerDimension(), expResult.getMinValuesPerDimension()));
  }

  @Test
  public void testEqualsEpsilon() {
    System.out.println("equalsEpsilon");
    double a = 0.00000000001;
    double b = 0.00000000002;
    boolean expResult = true;
    boolean result = XYGrid.equalsEpsilon(a, b);
    assertEquals(expResult, result);
  }

  @Test
  public void testEqualsEpsilonII() {
    System.out.println("equalsEpsilon");
    double a = 0.0000000001;
    double b = 0.0000000002;
    boolean expResult = false;
    boolean result = XYGrid.equalsEpsilon(a, b);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetEstimatedIdCount() {
    System.out.println("getEstimatedIdCount");
    MultiDimensionalNumericData data = new BasicNumericDataset(new NumericData[]{new NumericRange(0, 89), new NumericRange(0, 89)});

    long expResult = 1L;
    long result = two.getEstimatedIdCount(data);
    org.junit.Assert.assertTrue(Math.abs(expResult - result) <= two.getLevel());
  }

  @Test
  public void testGetEstimatedIdCountII() {
    System.out.println("getEstimatedIdCount");
    MultiDimensionalNumericData data = new BasicNumericDataset(new NumericData[]{new NumericRange(-89.0, 89.0), new NumericRange(-90.0, 90.0)});

    long expResult = 4L;
    long result = two.getEstimatedIdCount(data);
    org.junit.Assert.assertTrue(Math.abs(expResult - result) <= two.getLevel());
  }

  @Test
  public void testGetEstimatedIdCountIII() {
    System.out.println("getEstimatedIdCount");
    MultiDimensionalNumericData data = new BasicNumericDataset(new NumericData[]{new NumericRange(0.0, 0.000005364), new NumericRange(0.0, 0.000005364)});

    long expResult = 256L;
    long result = thirty.getEstimatedIdCount(data);
    org.junit.Assert.assertTrue(Math.abs(expResult - result) <= two.getLevel());
  }

  @Test
  public void testGetLevel() {
    System.out.println("getLevel");
    int expResult = 2;
    int result = two.getLevel();
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2Ids() {
    System.out.println("bbox2CellIdRanges");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-180, 180), new NumericRange(-90, 90)});

    Set<Pair<Long, Long>> result = zero.bbox2CellIdRanges(BBOX, false);
    assertEquals(1, result.size());
    Pair<Long, Long> interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());
  }

  @Test
  public void testBbox2IdsI() {
    System.out.println("bbox2CellIdRanges");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-10, 10), new NumericRange(-10, 10)});

    Set<Pair<Long, Long>> result = zero.bbox2CellIdRanges(BBOX, false);
    assertEquals(1, result.size());
    Pair<Long, Long> interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());
  }

  @Test
  public void testBbox2IdsII() {
    System.out.println("bbox2CellIdRanges");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(179, 89), new NumericRange(0, 5)});

    Set<Pair<Long, Long>> result = zero.bbox2CellIdRanges(BBOX, false);
    assertEquals(1, result.size());
    Pair<Long, Long> interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());
  }

  @Test
  public void testBbox2IdsIII() {
    System.out.println("bbox2CellIdRanges");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-10, 10), new NumericRange(-10, 10)});

    TreeSet<Long> expectedCellIds = new TreeSet<>();
    expectedCellIds.add(1L);
    expectedCellIds.add(2L);
    expectedCellIds.add(5L);
    expectedCellIds.add(6L);

    Set<Pair<Long, Long>> result = two.bbox2CellIdRanges(BBOX, false);
    for (Pair<Long, Long> interval : result) {
      for (long cellId = interval.getLeft(); cellId <= interval.getRight(); cellId++) {
        assertEquals(true, expectedCellIds.remove(cellId));
      }
    }
    assertEquals(0, expectedCellIds.size());
  }

  @Test
  public void testBbox2IdsIV() {
    System.out.println("bbox2CellIdRanges");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(180, 89), new NumericRange(0, 5)});

    TreeSet<Long> expectedCellIds = new TreeSet<>();
    expectedCellIds.add(4L);
    expectedCellIds.add(5L);
    expectedCellIds.add(6L);

    Set<Pair<Long, Long>> result = two.bbox2CellIdRanges(BBOX, false);
    for (Pair<Long, Long> interval : result) {
      for (long cellId = interval.getLeft(); cellId <= interval.getRight(); cellId++) {
        assertEquals(true, expectedCellIds.remove(cellId));
      }
    }
    assertEquals(0, expectedCellIds.size());
  }

  @Test
  public void testBbox2IdsV() {
    System.out.println("bbox2CellIdRanges");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(90, 89), new NumericRange(-90, -1)});

    TreeSet<Long> expectedCellIds = new TreeSet<>();
    expectedCellIds.add(0L);
    expectedCellIds.add(1L);
    expectedCellIds.add(2L);
    expectedCellIds.add(3L);

    Set<Pair<Long, Long>> result = two.bbox2CellIdRanges(BBOX, false);
    for (Pair<Long, Long> interval : result) {
      for (long cellId = interval.getLeft(); cellId <= interval.getRight(); cellId++) {
        assertEquals(true, expectedCellIds.remove(cellId));
      }
    }
    assertEquals(0, expectedCellIds.size());
  }

  @Test
  public void testBbox2IdsVI() {
    System.out.println("bbox2CellIdRanges");

    Set<Pair<Long, Long>> result = two.bbox2CellIdRanges(two.getCellDimensions(0), false);
    assertEquals(1, result.size());
    Pair<Long, Long> interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());
  }

  @Test
  public void testBbox2IdsVII() {
    System.out.println("test performance for maximum sized BBOX");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-180, 180), new NumericRange(-90, 90)});
    int expResult = 2048;
    Set<Pair<Long, Long>> result = new XYGrid(MAXZOOM).bbox2CellIdRanges(BBOX, true);
    assertEquals(expResult, result.size());
  }

  @Test
  public void testGetNeighbours() throws CellId.cellIdExeption {
    System.out.println("getNeighbours");
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
