package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.precision.GeometryPrecisionReducer;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.SpatialRelations;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.SpatialRelations.relationType;
import org.junit.Test;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;

public class TestSpatialRelations {

  public TestSpatialRelations() {
  }

  public LinkedList<SimpleFeature> getFeatures(String filePath) throws IOException {
    LinkedList<SimpleFeature> features = new LinkedList<>();

    File file = new File(filePath);
    FileInputStream in = new FileInputStream(file);

    FeatureIterator<SimpleFeature> fc = new FeatureJSON().streamFeatureCollection(in);

    // Adjust precision of coordinates
    PrecisionModel precisionModel = new PrecisionModel(1000000000000.);
    GeometryPrecisionReducer geometryPrecisionReducer = new GeometryPrecisionReducer(precisionModel);

    while(fc.hasNext()) {
      SimpleFeature feature = fc.next();
      Geometry geom = (Geometry) feature.getDefaultGeometryProperty().getValue();
      geom = geometryPrecisionReducer.reduce(geom);
      feature.setDefaultGeometry(geom);
      features.add(feature);
    }
    return features;
  }


  // -----------------------------------------------------------------------------------------------
  // TESTS
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testPolygonVsPolygon() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/polygon_polygon.geojson";
    String filePathCentral = "./src/test/resources/geojson/polygon_central.geojson";

    LinkedList<SimpleFeature> features = getFeatures(filePathOthers);
    LinkedList<SimpleFeature> centralFeature = getFeatures(filePathCentral);
    Geometry geomCentral = (Geometry) centralFeature.get(0).getDefaultGeometryProperty().getValue();

    for (Feature feat : features) {
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();
      relationType relation = SpatialRelations.relate(geomCentral, geom2);
      //System.out.println(((SimpleFeature) feat).getAttribute("relation") + " : " + relation.toString());
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPolygonVsLine() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/polygon_line.geojson";
    String filePathCentral = "./src/test/resources/geojson/polygon_central.geojson";

    LinkedList<SimpleFeature> features = getFeatures(filePathOthers);
    LinkedList<SimpleFeature> centralFeature = getFeatures(filePathCentral);
    Geometry geomCentral = (Geometry) centralFeature.get(0).getDefaultGeometryProperty().getValue();

    for (Feature feat : features) {
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();
      relationType relation = SpatialRelations.relate(geomCentral, geom2);
      // System.out.println(((SimpleFeature) feat).getAttribute("relation") + " : " + relation.toString());
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPolygonVsPoint() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/polygon_point.geojson";
    String filePathCentral = "./src/test/resources/geojson/polygon_central.geojson";

    LinkedList<SimpleFeature> features = getFeatures(filePathOthers);
    LinkedList<SimpleFeature> centralFeature = getFeatures(filePathCentral);
    Geometry geomCentral = (Geometry) centralFeature.get(0).getDefaultGeometryProperty().getValue();

    for (Feature feat : features) {
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();
      relationType relation = SpatialRelations.relate(geomCentral, geom2);
      // System.out.println(((SimpleFeature) feat).getAttribute("relation") + " : " + relation.toString());
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }

  // todo: FIX TEST !!!! coveredby does not work
  @Test
  public void testLineVsPolygon() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/line_polygon.geojson";
    String filePathCentral = "./src/test/resources/geojson/line_central.geojson";

    LinkedList<SimpleFeature> features = getFeatures(filePathOthers);
    LinkedList<SimpleFeature> centralFeature = getFeatures(filePathCentral);

    Geometry geomCentral = (Geometry) centralFeature.get(0).getDefaultGeometryProperty().getValue();

    for (Feature feat : features) {
      System.out.println(((SimpleFeature) feat).getAttribute("relation"));
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();

      relationType relation = SpatialRelations.relate(geomCentral, geom2);
      // System.out.println(((SimpleFeature) feat).getAttribute("relation") + " : " + relation.toString().toLowerCase());
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testLineVsLine() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/line_line.geojson";
    String filePathCentral = "./src/test/resources/geojson/line_central.geojson";

    LinkedList<SimpleFeature> features = getFeatures(filePathOthers);
    LinkedList<SimpleFeature> centralFeature = getFeatures(filePathCentral);
    Geometry geomCentral = (Geometry) centralFeature.get(0).getDefaultGeometryProperty().getValue();

    for (Feature feat : features) {
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();
      relationType relation = SpatialRelations.relate(geomCentral, geom2);
      // System.out.println(((SimpleFeature) feat).getAttribute("relation") + " : " + relation.toString());
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }

  // todo: contains only works if the point is equal to a vertex on the line not if it is somewhere inbetween
  // vertices of a line
  @Test
  public void testLineVsPoint() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/line_point.geojson";
    String filePathCentral = "./src/test/resources/geojson/line_central.geojson";

    LinkedList<SimpleFeature> features = getFeatures(filePathOthers);
    LinkedList<SimpleFeature> centralFeature = getFeatures(filePathCentral);
    Geometry geomCentral = (Geometry) centralFeature.get(0).getDefaultGeometryProperty().getValue();

    for (Feature feat : features) {
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();
      relationType relation = SpatialRelations.relate(geomCentral, geom2);
      // System.out.println(((SimpleFeature) feat).getAttribute("relation") + " : " + relation.toString());
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPointVsPolygon() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/point_polygon.geojson";
    String filePathCentral = "./src/test/resources/geojson/point_central.geojson";

    LinkedList<SimpleFeature> features = getFeatures(filePathOthers);
    LinkedList<SimpleFeature> centralFeature = getFeatures(filePathCentral);
    Geometry geomCentral = (Geometry) centralFeature.get(0).getDefaultGeometryProperty().getValue();

    for (Feature feat : features) {
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();
      relationType relation = SpatialRelations.relate(geomCentral, geom2);
      System.out.println(((SimpleFeature) feat).getAttribute("relation") + " : " + relation.toString());
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPointVsline() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/point_line.geojson";
    String filePathCentral = "./src/test/resources/geojson/point_central.geojson";

    LinkedList<SimpleFeature> features = getFeatures(filePathOthers);
    LinkedList<SimpleFeature> centralFeature = getFeatures(filePathCentral);
    Geometry geomCentral = (Geometry) centralFeature.get(0).getDefaultGeometryProperty().getValue();

    for (Feature feat : features) {
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();
      relationType relation = SpatialRelations.relate(geomCentral, geom2);
      // System.out.println(((SimpleFeature) feat).getAttribute("relation") + " : " + relation.toString());
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPointVsPoint() throws Exception {
    String filePathOthers = "./src/test/resources/geojson/point_point.geojson";
    String filePathCentral = "./src/test/resources/geojson/point_central.geojson";

    LinkedList<SimpleFeature> features = getFeatures(filePathOthers);
    LinkedList<SimpleFeature> centralFeature = getFeatures(filePathCentral);
    Geometry geomCentral = (Geometry) centralFeature.get(0).getDefaultGeometryProperty().getValue();

    for (Feature feat : features) {
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();
      relationType relation = SpatialRelations.relate(geomCentral, geom2);
      // System.out.println(((SimpleFeature) feat).getAttribute("relation") + " : " + relation.toString());
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }

}