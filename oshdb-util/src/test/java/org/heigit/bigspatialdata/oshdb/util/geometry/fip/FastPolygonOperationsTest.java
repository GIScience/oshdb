package org.heigit.bigspatialdata.oshdb.util.geometry.fip;

import static org.junit.Assert.assertEquals;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Polygon;
import org.junit.Test;

public class FastPolygonOperationsTest {

  @Test
  public void testEmptyGeometryPolygon() {
    Polygon p = FastPointInPolygonTest.createPolygon();
    FastPolygonOperations pop = new FastPolygonOperations(p);

    GeometryFactory gf = new GeometryFactory();

    assertEquals(
        pop.intersection(gf.createPoint((Coordinate) null)),
        gf.createPoint((Coordinate) null)
    );
  }

  @Test
  public void testNullGeometryPolygon() {
    Polygon p = FastPointInPolygonTest.createPolygon();
    FastPolygonOperations pop = new FastPolygonOperations(p);

    GeometryFactory gf = new GeometryFactory();

    assertEquals(
        pop.intersection((Polygon) null),
        null
    );
  }
}
