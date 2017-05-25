package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.geometry.text.WKTParser;
import org.heigit.bigspatialdata.oshdb.examples.activity.ActivityIndicatorFromPolygon;
import org.heigit.bigspatialdata.oshdb.examples.activity.ActivityIndicatorFromPolygonBuildings;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

public class ISEA3HGrid {
  public static void main(String[] args) throws IOException, SQLException, IndexOutOfBoundsException, ParseException,
      ClassNotFoundException, org.json.simple.parser.ParseException {
    System.out.println("Start: " + new Date().toString());
    // create filewriter to output results
    FileWriter fw = new FileWriter("hd_activity.csv");
    
    // load shapefile and get features
    File file = new File("/tmp/grid19_hd_ISEA3H_poly.shp");
    //File file = new File("/tmp/heidelberg_boundary.shp");

    // File file = new File("data/heidelberg_boundary.shp");
    Map<String, Object> map = new HashMap<>();
    map.put("url", file.toURI().toURL());

    DataStore dataStore = DataStoreFinder.getDataStore(map);
    String typeName = dataStore.getTypeNames()[0];

    FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(typeName);
    Filter filter = Filter.INCLUDE;

    // connect to Database

    // tcp://localhost
    Connection conn = DriverManager
        .getConnection("jdbc:h2:./heidelberg", "sa", "");
    
    //create lookupmap of tags and TagInterpreter for getGeometry() methods
    final Statement stmt = conn.createStatement();
    ResultSet rstTags = stmt.executeQuery(
        "select k.ID as KEYID, kv.VALUEID as VALUEID, k.txt as KEY, kv.txt as VALUE from KEYVALUE kv inner join KEY k on k.ID = kv.KEYID;");
    Map<String, Map<String, Pair<Integer, Integer>>> allKeyValues = new HashMap<>();
    while (rstTags.next()) {
      int keyId = rstTags.getInt(1);
      int valueId = rstTags.getInt(2);
      String keyStr = rstTags.getString(3);
      String valueStr = rstTags.getString(4);
      if (!allKeyValues.containsKey(keyStr))
        allKeyValues.put(keyStr, new HashMap<>());
      allKeyValues.get(keyStr).put(valueStr, new ImmutablePair<>(keyId, valueId));
    }
    rstTags.close();
    ResultSet rstRoles = stmt.executeQuery("select ID as ROLEID, txt as ROLE from ROLE;");
    Map<String, Integer> allRoles = new HashMap<>();
    while (rstRoles.next()) {
      int roleId = rstRoles.getInt(1);
      String roleStr = rstRoles.getString(2);
      allRoles.put(roleStr, roleId);
    }
    rstRoles.close();
    final TagInterpreter tagInterpreter = new DefaultTagInterpreter(allKeyValues, allRoles);

    
    // fill features into parallelizable List
    List<MultiPolygon> polygonList = new ArrayList<>();
    FeatureCollection<SimpleFeatureType, SimpleFeature> fcollection = source.getFeatures(filter);

    try (FeatureIterator<SimpleFeature> features = fcollection.features()) {
      while (features.hasNext()) {

        SimpleFeature feature = features.next();
        MultiPolygon multipolygon = (MultiPolygon) feature.getDefaultGeometry();
        polygonList.add(multipolygon);

      }
    }

    
    //execute ActivityIndicator in parallel for all features of shapefile
    /*
    Optional<String> superresult = polygonList.parallelStream().map(multipoly -> {
      ActivityIndicatorFromPolygonBuildings aifp = new ActivityIndicatorFromPolygonBuildings();
      Map<Long, Long> cellresult;
      StringBuilder sb = new StringBuilder();
      try {
        //get activities for feature
        cellresult = aifp.execute(conn, multipoly, tagInterpreter);
        
        //create output: 1st column: WKT geometry
        sb.append(multipoly.toString());
        
        //create output: rest of columns activity values
        for (Map.Entry<Long, Long> timePeriod : cellresult.entrySet()) {
          sb.append(";" + timePeriod.getValue());
        }
        //create output: prepare next csv line by adding a linebreak after the values
        sb.append("\n");
      } catch (ClassNotFoundException | ParseException | IOException e) {

        e.printStackTrace();
      }

      return sb.toString();
    }).reduce((a, b) -> {

      a = a + b;

      return a.toString();
    });*/

    ActivityIndicatorFromPolygonBuildings aifp = new ActivityIndicatorFromPolygonBuildings();
    Map<Pair<Integer, Long>, Long> superresult = aifp.execute(conn, polygonList, tagInterpreter);
    for (Map.Entry<Pair<Integer, Long>, Long> result : superresult.entrySet()) {
      System.out.printf("%d\t%d\t%d\n", result.getKey().getLeft(), result.getKey().getRight(), result.getValue());
    }
    
    //write out the whole csv file
    //fw.append(superresult);
    
    fw.close();
    System.out.println("Done: " + new Date().toString());
  }

}
