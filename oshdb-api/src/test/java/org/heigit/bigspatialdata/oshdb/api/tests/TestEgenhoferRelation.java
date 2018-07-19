package org.heigit.bigspatialdata.oshdb.api.tests;

import static org.junit.Assert.assertEquals;

import com.vividsolutions.jts.geom.Geometry;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;
import org.geotools.feature.FeatureIterator;
import org.geotools.geojson.feature.FeatureJSON;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.EgenhoferRelation;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.EgenhoferRelation.relationType;
import org.junit.Test;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;

public class TestEgenhoferRelation {

  private Geometry geomCentral = null;;
  private LinkedList<SimpleFeature> others = new LinkedList<>();

  public TestEgenhoferRelation() {
  }

  public void readData(String filePath) throws IOException {
    File file = new File(filePath);
    FileInputStream in = new FileInputStream(file);

    FeatureIterator<SimpleFeature> fc = new FeatureJSON().streamFeatureCollection(in);

    while(fc.hasNext()) {
      SimpleFeature feature = fc.next();
      Geometry geom = (Geometry) feature.getDefaultGeometryProperty().getValue();
      if (feature.getAttribute("relation").equals("central")) {
        geomCentral = geom;
      } else {
        others.add(feature);
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // TESTS
  // -----------------------------------------------------------------------------------------------

  @Test
  public void testLineVsLine() throws Exception {
    String filePath = "/Users/chludwig/Data/oshdb/testdata/line_line.geojson";
    readData(filePath);

    for (Feature feat : others) {
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();
      relationType relation = EgenhoferRelation.getRelation(geomCentral, geom2);
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }

  @Test
  public void testPolygonVsPolygon() throws Exception {
    String filePath = "/Users/chludwig/Data/oshdb/testdata/polygon_polygon.geojson";
    readData(filePath);

    for (Feature feat : others) {
      //System.out.println(((SimpleFeature) feat).getAttribute("relation"));
      Geometry geom2 = (Geometry) feat.getDefaultGeometryProperty().getValue();
      relationType relation = EgenhoferRelation.getRelation(geomCentral, geom2);
      assertEquals(((SimpleFeature) feat).getAttribute("relation"), relation.toString().toLowerCase());
    }
  }
}