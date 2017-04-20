package org.heigit.bigspatialdata.hosmdb.util;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import mil.nga.giat.geowave.core.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class XYGridTest {

  private static final Logger LOG = Logger.getLogger(XYGridTest.class.getName());
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
    LOG.log(Level.INFO, "Testing Coordinates: -181, -91, zoom 2");
    double longitude = -181.0;
    double latitude = -91.0;

    Long expResult = (long) -1;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void testneg180_neg90_0() {
    LOG.log(Level.INFO, "Testing Coordinates: -180, -90, zoom 0");
    double longitude = -180.0;
    double latitude = -90.0;

    Long expResult = (long) 0;
    Long result = zero.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test180_90_0() {
    LOG.log(Level.INFO, "Testing Coordinates: 180, 90, zoom 0");
    double longitude = 180.0;
    double latitude = 90.0;

    Long expResult = (long) 0;
    Long result = zero.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test179_90_0() {
    LOG.log(Level.INFO, "Testing Coordinates: 179, 90, zoom 0");
    double longitude = 179.0;
    double latitude = 90.0;

    Long expResult = (long) 0;
    Long result = zero.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void testneg180_neg90_2() {
    LOG.log(Level.INFO, "Testing Coordinates: -180, -90, zoom 2");
    double longitude = -180.0;
    double latitude = -90.0;

    Long expResult = (long) 0;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test180_90_2() {
    LOG.log(Level.INFO, "Testing Coordinates: 180, 90, zoom 2");
    double longitude = 180.0;
    double latitude = 90.0;

    Long expResult = (long) 4;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test179_90_2() {
    LOG.log(Level.INFO, "Testing Coordinates: 179, 90, zoom 2");
    double longitude = 179.99999999999;
    double latitude = 90.0;

    Long expResult = (long) 7;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void testneg180_neg90_31() {
    LOG.log(Level.INFO, "Testing Coordinates: -180, -90, zoom 31");
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
    LOG.log(Level.INFO, "Testing Coordinates: 180, 90, zoom -1");
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
    LOG.log(Level.INFO, "Testing Coordinates: -180, -90, zoom 30");
    double longitude = -180.0;
    double latitude = -90.0;

    Long expResult = (long) 0;
    Long result = thirty.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test180_90_30() {
    LOG.log(Level.INFO, "Testing Coordinates: 180, 90, zoom 30");
    double longitude = 180.0;
    double latitude = 90.0;

    Long expResult = 576460751229681664L;
    Long result = thirty.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void test179_90_30() {
    LOG.log(Level.INFO, "Testing Coordinates: 179, 90, zoom 30");
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
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-180, 180), new NumericRange(-90, 90)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    SortedSet<Long> result = zero.bbox2Ids(BBOX, false);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsI() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-10, 10), new NumericRange(-10, 10)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    SortedSet<Long> result = zero.bbox2Ids(BBOX, false);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsII() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(179, 89), new NumericRange(0, 5)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    SortedSet<Long> result = zero.bbox2Ids(BBOX, false);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsIII() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-10, 10), new NumericRange(-10, 10)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(1L);
    expResult.add(2L);
    expResult.add(5L);
    expResult.add(6L);
    SortedSet<Long> result = two.bbox2Ids(BBOX, false);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsIV() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(180, 89), new NumericRange(0, 5)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(4L);
    expResult.add(5L);
    expResult.add(6L);
    SortedSet<Long> result = two.bbox2Ids(BBOX, false);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsV() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(90, 89), new NumericRange(-90, -1)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    expResult.add(1L);
    expResult.add(2L);
    expResult.add(3L);
    SortedSet<Long> result = two.bbox2Ids(BBOX, false);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsVI() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-180, 180), new NumericRange(-90, 90)});

    TreeSet<Long> expResult = null;
    SortedSet<Long> result = thirty.bbox2Ids(BBOX, false);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsVII() {
    System.out.println("bbox2Ids");
    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    SortedSet<Long> result = two.bbox2Ids(two.getCellDimensions(0), false);
    assertEquals(expResult, result);
  }
  
   @Test
  public void testBbox2Ids2() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-180, 180), new NumericRange(-90, 90)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    SortedSet<Long> result = zero.bbox2Ids(BBOX,true);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsI2() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-10, 10), new NumericRange(-10, 10)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    SortedSet<Long> result = zero.bbox2Ids(BBOX,true);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsII2() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(179, 89), new NumericRange(0, 5)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    SortedSet<Long> result = zero.bbox2Ids(BBOX,true);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsIII2() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-10, 10), new NumericRange(-10, 10)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    expResult.add(1L);
    expResult.add(2L);
    expResult.add(4L);
    expResult.add(5L);
    expResult.add(6L);
    SortedSet<Long> result = two.bbox2Ids(BBOX,true);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsIV2() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(180, 89), new NumericRange(0, 5)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    expResult.add(1L);
    expResult.add(2L);
    expResult.add(4L);
    expResult.add(5L);
    expResult.add(6L);
    SortedSet<Long> result = two.bbox2Ids(BBOX,true);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsV2() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(90, 89), new NumericRange(-90, -1)});

    TreeSet<Long> expResult = new TreeSet<>();
    expResult.add(0L);
    expResult.add(1L);
    expResult.add(2L);
    expResult.add(3L);
    SortedSet<Long> result = two.bbox2Ids(BBOX,true);
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2IdsVI2() {
    System.out.println("bbox2Ids");
    MultiDimensionalNumericData BBOX = new BasicNumericDataset(new NumericData[]{new NumericRange(-180, 180), new NumericRange(-90, 90)});

    TreeSet<Long> expResult = null;
    SortedSet<Long> result = thirty.bbox2Ids(BBOX,true);
    assertEquals(expResult, result);
  }

}
