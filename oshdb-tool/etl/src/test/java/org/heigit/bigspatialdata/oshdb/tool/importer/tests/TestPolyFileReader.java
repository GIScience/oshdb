package org.heigit.bigspatialdata.oshdb.tool.importer.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.PolyFileReader;
import org.junit.Test;
import org.wololo.geojson.GeoJSON;
import org.wololo.geojson.MultiPolygon;
import org.wololo.geojson.Polygon;

public class TestPolyFileReader {

  @Test
  public void testAustralia() throws URISyntaxException, IOException, ParseException {
    // simple polygon
    Path file = Paths.get(TestPolyFileReader.class.getResource("/poly/australia.poly").toURI());
    GeoJSON result = PolyFileReader.parse(file);
    assertEquals(result.getType(), "Polygon");
    assertTrue(result instanceof Polygon);
    Polygon poly = (Polygon)result;
    final double[][][] coordinates = poly.getCoordinates();
    assertEquals(coordinates.length, 1);
    assertEquals(coordinates[0].length, 23);
    // check if it actually is Australia:
    assertTrue(coordinates[0][0][0] > 100); // quite a bit to the West
    assertTrue(coordinates[0][0][1] < 0); // southern hemisphere
    // -> must be Australia ;)
  }

  @Test
  public void testAustraliaOpen() throws URISyntaxException, IOException, ParseException {
    // simple polygon, open ring
    Path file = Paths.get(TestPolyFileReader.class.getResource("/poly/australia-open.poly").toURI());
    GeoJSON result = PolyFileReader.parse(file);
    assertEquals(result.getType(), "Polygon");
    assertTrue(result instanceof Polygon);
    Polygon poly = (Polygon)result;
    final double[][][] coordinates = poly.getCoordinates();
    assertEquals(coordinates.length, 1);
    assertEquals(coordinates[0].length, 23);
  }

  @Test
  public void testSouthAfrica() throws URISyntaxException, IOException, ParseException {
    // polygon with hole
    Path file = Paths.get(TestPolyFileReader.class.getResource("/poly/south-africa.poly").toURI());
    GeoJSON result = PolyFileReader.parse(file);
    assertEquals(result.getType(), "Polygon");
    assertTrue(result instanceof Polygon);
    Polygon poly = (Polygon)result;
    final double[][][] coordinates = poly.getCoordinates();
    assertEquals(coordinates.length, 2);
    assertEquals(
        coordinates[0].length + coordinates[1].length,
        639
    );
  }

  @Test
  public void testRussia() throws URISyntaxException, IOException, ParseException {
    // multi polygon
    Path file = Paths.get(TestPolyFileReader.class.getResource("/poly/russia.poly").toURI());
    GeoJSON result = PolyFileReader.parse(file);
    assertEquals(result.getType(), "MultiPolygon");
    assertTrue(result instanceof MultiPolygon);
    MultiPolygon poly = (MultiPolygon)result;
    final double[][][][] coordinates = poly.getCoordinates();
    assertEquals(coordinates.length, 3);
    assertEquals(coordinates[0].length, 1);
    assertEquals(coordinates[1].length, 1);
    assertEquals(coordinates[2].length, 1);
    assertEquals(
        coordinates[0][0].length + coordinates[1][0].length + coordinates[2][0].length,
        1111
    );
  }

}
