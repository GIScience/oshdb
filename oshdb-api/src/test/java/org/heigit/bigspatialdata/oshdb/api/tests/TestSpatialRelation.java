package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.SpatialRelation;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.SpatialRelation.Relation;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.wololo.geojson.FeatureCollection;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

public class TestSpatialRelation {
  private static Geometry setProperties(Geometry obj, Map<String,Object> properties) {
    obj.setUserData(properties);
    return obj;
  }

  private static Map<String,Object> getProperties(Geometry obj) {
    return (Map<String,Object>) obj.getUserData();
  }
  
  private List<Geometry> getFeatures(String filePath) throws IOException {
    GeoJSONReader reader = new GeoJSONReader();
    FeatureCollection FeatureCollection = (FeatureCollection)
        GeoJSONFactory.create(new String(Files.readAllBytes(Paths.get(filePath))));

    // Adjust precision of coordinates
    PrecisionModel precisionModel = new PrecisionModel(1000000000000.);
    GeometryPrecisionReducer geometryPrecisionReducer = new GeometryPrecisionReducer(precisionModel);

    return Arrays.stream(FeatureCollection.getFeatures()).map(Geometry -> {
      Geometry geom = (Geometry) reader.read(Geometry.getGeometry());
      geom = (Geometry) geometryPrecisionReducer.reduce(geom);
      return setProperties(geom, Geometry.getProperties());
    }).collect(Collectors.toList());
  }
  
  // -----------------------------------------------------------------------------------------------
  // TESTS
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testPolygonVsPolygon() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/polygon_polygon.geojson";
    String filePathCentral = "./src/test/resources/geojson/polygon_central.geojson";

    List<Geometry> Geometrys = getFeatures(filePathOthers);
    Geometry geomCentral = getFeatures(filePathCentral).get(0);

    for (Geometry feat : Geometrys) {
      Relation relation = SpatialRelation.relate(geomCentral, feat);
      assertEquals(getProperties(feat).get("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPolygonVsLine() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/polygon_line.geojson";
    String filePathCentral = "./src/test/resources/geojson/polygon_central.geojson";

    List<Geometry> Geometrys = getFeatures(filePathOthers);
    Geometry geomCentral = getFeatures(filePathCentral).get(0);

    for (Geometry feat : Geometrys) {
      Relation relation = SpatialRelation.relate(geomCentral, feat);
      assertEquals(getProperties(feat).get("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPolygonVsPoint() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/polygon_point.geojson";
    String filePathCentral = "./src/test/resources/geojson/polygon_central.geojson";

    List<Geometry> Geometrys = getFeatures(filePathOthers);
    Geometry geomCentral = getFeatures(filePathCentral).get(0);

    for (Geometry feat : Geometrys) {
      Relation relation = SpatialRelation.relate(geomCentral, feat);
      assertEquals(getProperties(feat).get("relation"), relation.toString().toLowerCase());
    }
  }

  // todo: FIX TEST !!!! coveredby does not work
  @Test
  public void testLineVsPolygon() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/line_polygon.geojson";
    String filePathCentral = "./src/test/resources/geojson/line_central.geojson";

    List<Geometry> Geometrys = getFeatures(filePathOthers);
    Geometry geomCentral = getFeatures(filePathCentral).get(0);

    for (Geometry feat : Geometrys) {
      Relation relation = SpatialRelation.relate(geomCentral, feat);
      assertEquals(getProperties(feat).get("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testLineVsLine() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/line_line.geojson";
    String filePathCentral = "./src/test/resources/geojson/line_central.geojson";

    List<Geometry> Geometrys = getFeatures(filePathOthers);
    Geometry geomCentral = getFeatures(filePathCentral).get(0);

    for (Geometry feat : Geometrys) {
      Relation relation = SpatialRelation.relate(geomCentral, feat);
      assertEquals(getProperties(feat).get("relation"), relation.toString().toLowerCase());
    }
  }

  // todo: contains only works if the point is equal to a vertex on the line not if it is somewhere inbetween
  // vertices of a line
  @Test
  public void testLineVsPoint() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/line_point.geojson";
    String filePathCentral = "./src/test/resources/geojson/line_central.geojson";

    List<Geometry> Geometrys = getFeatures(filePathOthers);
    Geometry geomCentral = getFeatures(filePathCentral).get(0);

    for (Geometry feat : Geometrys) {
      Relation relation = SpatialRelation.relate(geomCentral, feat);
      assertEquals(getProperties(feat).get("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPointVsPolygon() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/point_polygon.geojson";
    String filePathCentral = "./src/test/resources/geojson/point_central.geojson";

    List<Geometry> Geometrys = getFeatures(filePathOthers);
    Geometry geomCentral = getFeatures(filePathCentral).get(0);

    for (Geometry feat : Geometrys) {
      Relation relation = SpatialRelation.relate(geomCentral, feat);
      assertEquals(getProperties(feat).get("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPointVsline() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/point_line.geojson";
    String filePathCentral = "./src/test/resources/geojson/point_central.geojson";

    List<Geometry> Geometrys = getFeatures(filePathOthers);
    Geometry geomCentral = getFeatures(filePathCentral).get(0);

    for (Geometry feat : Geometrys) {
      Relation relation = SpatialRelation.relate(geomCentral, feat);
      assertEquals(getProperties(feat).get("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPointVsPoint() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/point_point.geojson";
    String filePathCentral = "./src/test/resources/geojson/point_central.geojson";

    List<Geometry> Geometrys = getFeatures(filePathOthers);
    Geometry geomCentral = getFeatures(filePathCentral).get(0);

    for (Geometry feat : Geometrys) {
      Relation relation = SpatialRelation.relate(geomCentral, feat);
      assertEquals(getProperties(feat).get("relation"), relation.toString().toLowerCase());
    }
  }
}