package org.heigit.bigspatialdata.oshdb.index;

import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Moritz Schott <m.schott@stud.uni-heidelberg.de>
 */
public class XYGridTest {

  private static final int MAXZOOM = OSHDB.MAXZOOM;
  private static final Logger LOG = LoggerFactory.getLogger(XYGridTest.class);
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
    double longitude = 180.0 - OSHDB.GEOM_PRECISION;
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
    LOG.info("This should throw a warning:!");
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
    LOG.info("This should throw a warning:!");
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
    double longitude = 180.0-OSHDB.GEOM_PRECISION;
    double latitude = 90.0;

    Long expResult = 576460752303423487L;
    Long result = thirty.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetId_BoundingBox() {
    OSHDBBoundingBox bbx = new OSHDBBoundingBox(-10.0, -10.0, 10.0, 10.0);
    XYGrid instance = new XYGrid(2);
    long expResult = 1L;
    long result = instance.getId(bbx);
    assertEquals(expResult, result);

    OSHDBBoundingBox bbx2 = new OSHDBBoundingBox(10.0, -10.0, -9.0, 10.0);
    instance = new XYGrid(2);
    expResult = 2L;
    result = instance.getId(bbx2);
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
    OSHDBBoundingBox expResult = new OSHDBBoundingBox(-180.0, -90.0, -90.0 - OSHDB.GEOM_PRECISION, 0.0 - OSHDB.GEOM_PRECISION);
    OSHDBBoundingBox result = two.getCellDimensions(cellId);
    assertEquals(expResult, result);

    cellId = 6L;
    expResult = new OSHDBBoundingBox(0.0, 0.0, 90.0 - OSHDB.GEOM_PRECISION, 90.0);
    result = two.getCellDimensions(cellId);
    assertEquals(expResult, result);

    cellId = 7L;
    expResult = new OSHDBBoundingBox(90.0, 0.0, 180.0 - OSHDB.GEOM_PRECISION, 90.0);
    result = two.getCellDimensions(cellId);
    assertEquals(expResult, result);

    cellId = 0L;
    expResult = new OSHDBBoundingBox(-180.0, -90.0, 180.0 - OSHDB.GEOM_PRECISION, 90.0);
    result = zero.getCellDimensions(cellId);
    assertEquals(expResult, result);

    cellId = 0L;
    expResult = new OSHDBBoundingBox(-180.0, -90.0, 0.0 - OSHDB.GEOM_PRECISION, 90.0);
    XYGrid instance = new XYGrid(1);
    result = instance.getCellDimensions(cellId);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetEstimatedIdCount() {
    OSHDBBoundingBox data = new OSHDBBoundingBox(0, 0, 89, 89);
    long expResult = 1L;
    long result = two.getEstimatedIdCount(data);
    assertEquals(expResult, result);

    data = new OSHDBBoundingBox(-89.0, -90.0, 89.0, 90.0);
    expResult = 2L;
    result = two.getEstimatedIdCount(data);
    assertEquals(expResult, result);

    data = new OSHDBBoundingBox(0.0, 0.0, 0.0000053, 0.0000053);
    expResult = 16L;
    result = thirty.getEstimatedIdCount(data);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetLevel() {
    int expResult = 2;
    int result = two.getLevel();
    assertEquals(expResult, result);
  }

  @Test
  public void testBbox2Ids() {
    OSHDBBoundingBox BBOX = new OSHDBBoundingBox(-180, -90, 180, 90);
    Set<Pair<Long, Long>> result = zero.bbox2CellIdRanges(BBOX, false);

    assertEquals(1, result.size());
    Pair<Long, Long> interval = result.iterator().next();

    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());

    BBOX = new OSHDBBoundingBox(-180, -90, 180, 90);
    result = two.bbox2CellIdRanges(BBOX, false);

    assertEquals(2, result.size());
    interval = result.iterator().next();

    assertEquals(0, interval.getLeft().longValue());
    assertEquals(3, interval.getRight().longValue());

    BBOX = new OSHDBBoundingBox(-10, -10, 10, 10);
    result = zero.bbox2CellIdRanges(BBOX, false);

    assertEquals(1, result.size());
    interval = result.iterator().next();

    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());

    BBOX = new OSHDBBoundingBox(179, 0, 89, 5);
    result = zero.bbox2CellIdRanges(BBOX, false);

    assertEquals(1, result.size());
    interval = result.iterator().next();

    assertEquals(0, interval.getLeft().longValue());
    assertEquals(0, interval.getRight().longValue());

    BBOX = new OSHDBBoundingBox(-10, -10, 10, 10);
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

    BBOX = new OSHDBBoundingBox(-180, 0, 89, 5);
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

    BBOX = new OSHDBBoundingBox(90, -90, 89, -1);
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
    BBOX = new OSHDBBoundingBox(-180, -90, 180, 90);
    int expResult = (int)Math.pow(2,MAXZOOM)/2;
    LOG.info("If this throws a warning because of the maximum zoomlevel, we have to change XYGrid-Code:");
    result = new XYGrid(MAXZOOM).bbox2CellIdRanges(BBOX, true);
    assertEquals(expResult, result.size());
    interval = result.iterator().next();
    assertEquals(0, interval.getLeft().longValue());
    assertEquals((int)Math.pow(2,MAXZOOM)-1, interval.getRight().longValue());
  }

  @Test
  public void testGetNeighbours() {
    CellId center = new CellId(2, 6L);
    Set<Pair<Long, Long>> expResult = new TreeSet<>();
    expResult.add(new ImmutablePair<>(1L, 3L));
    expResult.add(new ImmutablePair<>(5L, 7L));
    expResult.add(new ImmutablePair<>(-1L, -1L));
    Set<Pair<Long, Long>> result = two.getNeighbours(center);
    assertEquals(expResult, result);
  }

  @Test
  public void testGetBoundingBox() {
    OSHDBBoundingBox result = XYGrid.getBoundingBox(new CellId(2, 2));
    OSHDBBoundingBox expResult = new OSHDBBoundingBox(0.0, -90.0, 90.0, 0.0 - OSHDB.GEOM_PRECISION);
    assertEquals(expResult.toString(), result.toString());
  }
}
