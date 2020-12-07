package org.heigit.ohsome.oshdb.util.geometry.fip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class FastPolygonOperationsTest {
  private final GeometryFactory gf = new GeometryFactory();

  @Test
  public void testEmptyGeometryPolygon() {
    Polygon p = FastPointInPolygonTest.createPolygon();
    FastPolygonOperations pop = new FastPolygonOperations(p);

    assertEquals(
        gf.createPoint((Coordinate) null),
        pop.intersection(gf.createPoint((Coordinate) null))
    );
  }

  @Test
  public void testNullGeometryPolygon() {
    Polygon p = FastPointInPolygonTest.createPolygon();
    FastPolygonOperations pop = new FastPolygonOperations(p);

    assertNull(pop.intersection(null));
  }

  @Test
  public void testBug206() throws ParseException {
    // see https://github.com/GIScience/oshdb/pull/204
    
    String polyWkt = "POLYGON ((-0.0473915 51.5539955,-0.0473872 51.5540543,-0.0473811 51.554121,"
        + "-0.0473792 51.5541494,-0.0473193 51.5541485,-0.047305 51.5540903,-0.0472924 51.5540904,"
        + "-0.0472852 51.5540674,-0.0472735 51.5540681,-0.0472692 51.5540517,-0.0472852 51.5540499,"
        + "-0.0472602 51.5539917,-0.0472317 51.553925,-0.0472157 51.5539254,-0.0472097 51.5539106,"
        + "-0.0473992 51.5538913,-0.0474735 51.5538845,-0.0475498 51.5538774,-0.0476179 51.5538712,"
        + "-0.0476923 51.5538643,-0.0476923 51.5538575,-0.0477741 51.5538544,-0.0478501 51.5538516,"
        + "-0.0479217 51.553849,-0.0479986 51.5538462,-0.0480796 51.5538432,-0.0481503 51.5538406,"
        + "-0.0482264 51.5538378,-0.0482708 51.5538377,-0.0483561 51.5538358,-0.0484275 51.5538343,"
        + "-0.0485046 51.5538325,-0.0485294 51.553832,-0.0485255 51.5539171,-0.0485813 51.5539157,"
        + "-0.0485787 51.5539268,-0.0485505 51.5540481,-0.048532 51.5540479,-0.0485309 51.5539901,"
        + "-0.0484762 51.5539897,-0.0484784 51.5540545,-0.0484507 51.5540541,-0.0484187 51.5540536,"
        + "-0.0484198 51.5539893,-0.0483923 51.5539892,-0.0483925 51.5539796,-0.0483729 51.5539795,"
        + "-0.048362 51.5539794,-0.0483618 51.553989,-0.0483251 51.5539888,-0.0483241 51.5540522,"
        + "-0.0482959 51.5540518,-0.0482676 51.5540514,-0.0482697 51.5539916,-0.0482366 51.5539911,"
        + "-0.0482369 51.5539847,-0.0482236 51.5539846,-0.0482067 51.5539843,-0.0482064 51.5539907,"
        + "-0.0481825 51.5539904,-0.0481803 51.5540561,-0.0481466 51.5540562,-0.0481136 51.5540562,"
        + "-0.048115 51.5539953,-0.0480721 51.5539945,-0.048072 51.5540025,-0.0480546 51.5540021,"
        + "-0.0480539 51.5540142,-0.0480194 51.5540138,-0.048018 51.5540561,-0.04799 51.5540558,"
        + "-0.0479623 51.5540556,-0.047964 51.5540149,-0.0479185 51.5540142,-0.0478768 51.5540135,"
        + "-0.047875 51.5540547,-0.0478614 51.5540546,-0.0478616 51.5540458,-0.0478423 51.5540457,"
        + "-0.0478439 51.554006,-0.0477667 51.5540059,-0.0476868 51.5540057,-0.0476872 51.5539952,"
        + "-0.047616 51.5539953,-0.0475449 51.5539954,-0.0474682 51.5539954,-0.0473915 51.5539955)"
        + ")";
    Polygon poly = (Polygon) (new WKTReader()).read(polyWkt);
    FastPolygonOperations pop = new FastPolygonOperations(poly);

    String testWkt = "POLYGON ((-0.0478421 51.5540544,-0.0478399 51.5541248,-0.0478499 51.5541249,"
        + "-0.0478549 51.5541307,-0.0478795 51.5541313,-0.0478858 51.5541252,-0.0479136 51.5541254,"
        + "-0.0479185 51.5540142,-0.0478768 51.5540135,-0.047875 51.5540547,-0.0478614 51.5540546,"
        + "-0.0478616 51.5540458,-0.0478423 51.5540457,-0.0478421 51.5540544))";
    Geometry test = (new WKTReader()).read(testWkt);

    assertNotNull(pop.intersection(test));
  }

  @Test
  public void testGeometries() throws ParseException {
    String polyWkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (1 1, 1 9, 9 9, 9 1, 1 1))";
    Polygon poly = (Polygon) (new WKTReader()).read(polyWkt).buffer(0.1, 200);
    FastPolygonOperations pop = new FastPolygonOperations(poly);

    // points
    for (int i = 0; i < 30; i++) {
      Geometry testGeom = gf.createPoint(new Coordinate(0.4 * i, 0.35 * i));
      assertEquals(
          poly.intersection(testGeom),
          pop.intersection(testGeom)
      );
    }

    // lines
    for (int i = 0; i < 30; i++) {
      Geometry testGeom = gf.createLineString(new Coordinate[] {
          new Coordinate(0.4 * i + 1.0, 0.35 * i),
          new Coordinate(0.4 * i, 0.35 * i + 1.0)
      });
      assertEquals(
          poly.intersection(testGeom),
          pop.intersection(testGeom)
      );
    }

    // polygons
    for (int i = 0; i < 30; i++) {
      Geometry testGeom = gf.createPoint(new Coordinate(0.4 * i, 0.35 * i)).buffer(0.01);
      assertEquals(
          poly.intersection(testGeom),
          pop.intersection(testGeom)
      );
    }
  }
}
