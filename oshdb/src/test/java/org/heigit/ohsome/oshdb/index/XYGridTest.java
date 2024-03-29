package org.heigit.ohsome.oshdb.index;

import static org.heigit.ohsome.oshdb.OSHDBBoundingBox.bboxWgs84Coordinates;
import static org.heigit.ohsome.oshdb.osm.OSMCoordinates.GEOM_PRECISION;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import java.util.TreeSet;
import org.heigit.ohsome.oshdb.OSHDB;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.index.XYGrid.IdRange;
import org.heigit.ohsome.oshdb.util.CellId;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:abbreviationAsWordInName")
class XYGridTest {

  private static final int MAXZOOM = OSHDB.MAXZOOM;
  private static final Logger LOG = LoggerFactory.getLogger(XYGridTest.class);
  private final XYGrid zero = new XYGrid(0);
  private final XYGrid two = new XYGrid(2);
  private final XYGrid thirty = new XYGrid(30);

  @Test
  void testGetId_double_double() {
    double longitude = 0.0;
    double latitude = 0.0;
    XYGrid instance = new XYGrid(2);
    long expResult = 6L;
    long result = instance.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void testnegneg181_neg91_2() {
    // Testing Coordinates: -181, -91, zoom 2
    double longitude = -181.0;
    double latitude = -91.0;

    Long expResult = (long) -1;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void testneg180_neg90_0() {
    // Testing Coordinates: -180, -90, zoom 0
    double longitude = -180.0;
    double latitude = -90.0;

    Long expResult = (long) 0;
    Long result = zero.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void test180_90_0() {
    // Testing Coordinates: 180, 90, zoom 0
    double longitude = 180.0;
    double latitude = 90.0;

    Long expResult = (long) 0;
    Long result = zero.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void test179_90_0() {
    // Testing Coordinates: 179, 90, zoom 0
    double longitude = 179.0;
    double latitude = 90.0;

    Long expResult = (long) 0;
    Long result = zero.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void testneg180_neg90_2() {
    // Testing Coordinates: -180, -90, zoom 2
    double longitude = -180.0;
    double latitude = -90.0;

    Long expResult = (long) 0;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void test180_90_2() {
    // Testing Coordinates: 180, 90, zoom 2
    double longitude = 180.0;
    double latitude = 90.0;

    Long expResult = (long) 4;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void test179_90_2() {
    // Testing Coordinates: 179, 90, zoom 2
    double longitude = 180.0 - GEOM_PRECISION;
    double latitude = 90.0;

    Long expResult = (long) 7;
    Long result = two.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void testneg180_neg90_31() {
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
  void test180_90_neg1() {
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
  void testneg180_neg90_30() {
    // Testing Coordinates: -180, -90, zoom 30
    double longitude = -180.0;
    double latitude = -90.0;

    Long expResult = (long) 0;
    Long result = thirty.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void test180_90_30() {
    // Testing Coordinates: 180, 90, zoom 30
    double longitude = 180.0;
    double latitude = 90.0;

    Long expResult = 576460751229681664L;
    Long result = thirty.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void test179_90_30() {
    // Testing Coordinates: 179, 90, zoom 30
    double longitude = 180.0 - GEOM_PRECISION;
    double latitude = 90.0;

    Long expResult = 576460752303423487L;
    Long result = thirty.getId(longitude, latitude);
    assertEquals(expResult, result);
  }

  @Test
  void testGetId_BoundingBox() {
    OSHDBBoundingBox bbx = bboxWgs84Coordinates(-10.0, -10.0, 10.0, 10.0);
    XYGrid instance = new XYGrid(2);
    long expResult = 1L;
    long result = instance.getId(bbx);
    assertEquals(expResult, result);

    OSHDBBoundingBox bbx2 = bboxWgs84Coordinates(10.0, -10.0, -9.0, 10.0);
    instance = new XYGrid(2);
    expResult = 2L;
    result = instance.getId(bbx2);
    assertEquals(expResult, result);
  }

  @Test
  void testGetCellWidth() {
    double expResult = 90;

    double result = two.getCellWidth();
    assertEquals(expResult, result, 0.0);
  }

  @Test
  void testGetCellDimensions() {
    long cellId = 0L;
    OSHDBBoundingBox expResult = bboxWgs84Coordinates(-180.0, -90.0, -90.0 - GEOM_PRECISION,
        0.0 - GEOM_PRECISION);
    OSHDBBoundingBox result = two.getCellDimensions(cellId);
    assertEquals(expResult, result);

    cellId = 6L;
    expResult = bboxWgs84Coordinates(0.0, 0.0, 90.0 - GEOM_PRECISION, 90.0);
    result = two.getCellDimensions(cellId);
    assertEquals(expResult, result);

    cellId = 7L;
    expResult = bboxWgs84Coordinates(90.0, 0.0, 180.0 - GEOM_PRECISION, 90.0);
    result = two.getCellDimensions(cellId);
    assertEquals(expResult, result);

    cellId = 0L;
    expResult = bboxWgs84Coordinates(-180.0, -90.0, 180.0 - GEOM_PRECISION, 90.0);
    result = zero.getCellDimensions(cellId);
    assertEquals(expResult, result);

    cellId = 0L;
    expResult = bboxWgs84Coordinates(-180.0, -90.0, 0.0 - GEOM_PRECISION, 90.0);
    XYGrid instance = new XYGrid(1);
    result = instance.getCellDimensions(cellId);
    assertEquals(expResult, result);
  }

  @Test
  void testGetEstimatedIdCount() {
    OSHDBBoundingBox data = bboxWgs84Coordinates(0.0, 0.0, 89.0, 89.0);
    long expResult = 1L;
    long result = two.getEstimatedIdCount(data);
    assertEquals(expResult, result);

    data = bboxWgs84Coordinates(-89.0, -90.0, 89.0, 90.0);
    expResult = 2L;
    result = two.getEstimatedIdCount(data);
    assertEquals(expResult, result);

    data = bboxWgs84Coordinates(0.0, 0.0, 0.0000053, 0.0000053);
    expResult = 16L;
    result = thirty.getEstimatedIdCount(data);
    assertEquals(expResult, result);

    // "just" touching three cells, see https://github.com/GIScience/oshdb/pull/183
    data = bboxWgs84Coordinates(-0.1, 0, 90.1, 89);
    expResult = 3L;
    result = two.getEstimatedIdCount(data);
    assertEquals(expResult, result);
  }

  @Test
  void testGetLevel() {
    int expResult = 2;
    int result = two.getLevel();
    assertEquals(expResult, result);
  }

  @Test
  void testBbox2Ids() {
    OSHDBBoundingBox bbox = bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0);
    Set<IdRange> result = zero.bbox2CellIdRanges(bbox, false);

    assertEquals(1, result.size());
    IdRange interval = result.iterator().next();

    assertEquals(0, interval.getStart());
    assertEquals(0, interval.getEnd());

    bbox = bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0);
    result = two.bbox2CellIdRanges(bbox, false);

    assertEquals(2, result.size());
    interval = result.iterator().next();

    assertEquals(0, interval.getStart());
    assertEquals(3, interval.getEnd());

    bbox = bboxWgs84Coordinates(-10.0, -10.0, 10.0, 10.0);
    result = zero.bbox2CellIdRanges(bbox, false);

    assertEquals(1, result.size());
    interval = result.iterator().next();

    assertEquals(0, interval.getStart());
    assertEquals(0, interval.getEnd());

    bbox = bboxWgs84Coordinates(179.0, 0.0, 89.0, 5.0);
    result = zero.bbox2CellIdRanges(bbox, false);

    assertEquals(1, result.size());
    interval = result.iterator().next();

    assertEquals(0, interval.getStart());
    assertEquals(0, interval.getEnd());

    bbox = bboxWgs84Coordinates(-10.0, -10.0, 10.0, 10.0);
    TreeSet<Long> expectedCellIds = new TreeSet<>();
    expectedCellIds.add(1L);
    expectedCellIds.add(2L);
    expectedCellIds.add(5L);
    expectedCellIds.add(6L);
    result = two.bbox2CellIdRanges(bbox, false);
    for (IdRange interval2 : result) {
      for (long cellId = interval2.getStart(); cellId <= interval2.getEnd(); cellId++) {
        assertEquals(true, expectedCellIds.remove(cellId));
      }
    }
    assertEquals(0, expectedCellIds.size());

    bbox = bboxWgs84Coordinates(-180.0, 0.0, 89.0, 5.0);
    expectedCellIds = new TreeSet<>();
    expectedCellIds.add(4L);
    expectedCellIds.add(5L);
    expectedCellIds.add(6L);

    result = two.bbox2CellIdRanges(bbox, false);
    for (IdRange interval2 : result) {
      for (long cellId = interval2.getStart(); cellId <= interval2.getEnd(); cellId++) {
        assertEquals(true, expectedCellIds.remove(cellId));
      }
    }
    assertEquals(0, expectedCellIds.size());

    bbox = bboxWgs84Coordinates(90.0, -90.0, 89.0, -1.0);
    expectedCellIds = new TreeSet<>();
    expectedCellIds.add(0L);
    expectedCellIds.add(1L);
    expectedCellIds.add(2L);
    expectedCellIds.add(3L);

    result = two.bbox2CellIdRanges(bbox, false);
    for (IdRange interval2 : result) {
      for (long cellId = interval2.getStart(); cellId <= interval2.getEnd(); cellId++) {
        assertEquals(true, expectedCellIds.remove(cellId));
      }
    }
    assertEquals(0, expectedCellIds.size());

    result = two.bbox2CellIdRanges(two.getCellDimensions(0), false);
    assertEquals(1, result.size());
    interval = result.iterator().next();
    assertEquals(0, interval.getStart());
    assertEquals(0, interval.getEnd());

    // test performance for maximum sized BBOX
    bbox = bboxWgs84Coordinates(-180.0, -90.0, 180.0, 90.0);
    int expResult = (int) Math.pow(2, MAXZOOM) / 2;
    LOG.info("If this throws a warning because of the maximum zoomlevel, "
        + "we have to change XYGrid-Code:");
    result = new XYGrid(MAXZOOM).bbox2CellIdRanges(bbox, true);
    assertEquals(expResult, result.size());
    interval = result.iterator().next();
    assertEquals(0, interval.getStart());
    assertEquals((int) Math.pow(2, MAXZOOM) - 1, interval.getEnd());
  }

  @Test
  void testGetNeighbours() {
    CellId center = new CellId(2, 6L);
    Set<IdRange> expResult = new TreeSet<>();
    expResult.add(IdRange.of(1L, 3L));
    expResult.add(IdRange.of(5L, 7L));
    expResult.add(IdRange.of(-1L, -1L));
    Set<IdRange> result = two.getNeighbours(center);
    assertEquals(expResult, result);
  }

  @Test
  void testGetBoundingBox() {
    OSHDBBoundingBox result = XYGrid.getBoundingBox(new CellId(2, 2));
    OSHDBBoundingBox expResult =
        bboxWgs84Coordinates(0.0, -90.0, 90.0 - GEOM_PRECISION, 0.0 - GEOM_PRECISION);
    assertEquals(expResult, result);

    OSHDBBoundingBox enlarged = XYGrid.getBoundingBox(new CellId(2, 2), true);
    expResult = bboxWgs84Coordinates(0.0, -90.0, 180.0 - GEOM_PRECISION, 90.0);
    assertEquals(expResult, enlarged);
  }
}
