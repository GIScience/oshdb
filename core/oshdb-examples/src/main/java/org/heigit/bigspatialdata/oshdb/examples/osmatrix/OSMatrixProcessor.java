package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import javax.measure.unit.SystemOfUnits;
import javax.sql.DataSource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.geometry.jts.JTS;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.utils.OSMTimeStamps;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;


public class OSMatrixProcessor {
  
  private static final Logger logger = Logger.getLogger(OSMatrixProcessor.class); //apache log4J
  
  public enum TABLE { // could be one of those three
    /** The polygon. */
    RELATION,
    /** The line. */
    WAY,
    /** The point. */
    NODE
  }


  private static String pathToConfigFile;
  
  private Map<TABLE, List<String>> mapTableTypeDep = new HashMap<TABLE, List<String>>();
  
  private Map<String, Attribute> mapTypeAttribute = new HashMap<String, Attribute>();
  
  private Map<String, Integer> mapTypId = new HashMap<String, Integer>();
  
  List<Long> timestampsList;
  
  
  
  
  public void start() { 
 
    if (!init())
      return;
    
    try {
      
      doTheWork();
      
    } finally {
      cleanup();
    }
  }
  
  
  private void cleanup() {
    logger.info("All finished!");
    
  }


  private void doTheWork() {
    if (mapTypeAttribute.size() == 0) {
      logger.info("No registered types!");
      return;
    }
    //TODO get List of timestamps
    //TODO Collections.sort(timestamps, Collections.reverseOrder());
   
    
    
    
  }
  public Map<Pair<Integer, Long>, Long> executeNodes(Connection conn, List<MultiPolygon> polygons, TagInterpreter ti, List<Long> timestamps)
      throws ClassNotFoundException, ParseException, IOException {
    
    Class.forName("org.h2.Driver");


    // create BBOX to query gridcells as CellId-Objects, which contain
    // (zoomlevel,id)

    Double minLon = Double.MAX_VALUE;
    Double maxLon = Double.MIN_VALUE;
    Double minLat = Double.MAX_VALUE;
    Double maxLat = Double.MIN_VALUE;
    for (MultiPolygon inputPolygon: polygons) {
      minLon = Double.min(JTS.toEnvelope(inputPolygon).getMinX(), minLon);
      maxLon = Double.max(JTS.toEnvelope(inputPolygon).getMaxX(), maxLon);
      minLat = Double.min(JTS.toEnvelope(inputPolygon).getMinY(), minLat);
      maxLat = Double.max(JTS.toEnvelope(inputPolygon).getMaxY(), maxLat);
    }

    BoundingBox inputBbox = new BoundingBox(minLon, maxLon, minLat, maxLat);
    
    XYGrid grid = new XYGrid(12);
   

    final List<CellId> cellIds = new ArrayList<>();

    //grid.bbox2CellIds(inputBbox, true).forEach(cellIds::add);

    // start processing in parallel all grid cells that relate to the input

    Map<Pair<Integer, Long>, Long> superresult = cellIds.parallelStream().flatMap(cellId -> {

      try (final PreparedStatement pstmt = conn.prepareStatement(

          "(select data from grid_way where level = ?1 and id = ?2) union (select data from grid_relation where level = ?1 and id = ?2)")) {
        pstmt.setInt(1, cellId.getZoomLevel());
        pstmt.setLong(2, cellId.getId());

        try (final ResultSet rst2 = pstmt.executeQuery()) {
          List<GridOSHEntity> cells = new LinkedList<>();
          while (rst2.next()) {
            final ObjectInputStream ois = new ObjectInputStream(rst2.getBinaryStream(1));
            cells.add((GridOSHEntity) ois.readObject());

          }

          return cells.stream();

        }
      } catch (IOException | SQLException | ClassNotFoundException e) {
        e.printStackTrace();
        return null;
      }
    }).map(gridCell -> {

      GridOSHEntity cell = (GridOSHEntity) gridCell;
      //Map<Integer, Map<Long, Long>> timestampActivity = new TreeMap<>();
      Map<Pair<Integer,Long>, Long> timestampActivity = new TreeMap<>();

      for (OSHEntity<OSMEntity> osh : (Iterable<OSHEntity<OSMEntity>>) cell) {
        
        if (!osh.hasTagKey(0)) continue;


        List<OSMEntity> versions = new ArrayList<>();
        List<Integer> polygonIds = new ArrayList<>();

        List<Long> modTs = osh.getModificationTimestamps(true);
        modTs.sort(Collections.reverseOrder());

        Iterator<OSMEntity> allVersions = osh.getVersions().iterator();
        allVersions.hasNext();

        OSMEntity osm = allVersions.next();
        for (Long t : modTs) {

          if (t < osm.getTimestamp()) {
            if (!allVersions.hasNext())
              break;
            osm = allVersions.next();
          }

          if (!osm.isVisible() || !osm.hasTagKey(0)) continue;

            try {


              Geometry osmGeom = osm.getGeometry(t, ti);

              Point centr = osmGeom.getCentroid();
              //every map has to go through all osmatrix cells
              int foundIndex = -1;
              for (MultiPolygon p : polygons) {
                if (p.contains(centr)) {
                  foundIndex = polygons.indexOf(p);
                  break;
                }
              }

              if ( 
                  foundIndex != -1
                  )

              {

                versions.add(osm);
                polygonIds.add(foundIndex);
              }
            } catch (Exception e) {
              // TODO: handle exception

            }

        }

        int v = 0;
        for (int i = 0; i < timestamps.size(); i++) {
          long ts = timestamps.get(i);
          while (v < versions.size() && versions.get(v).getTimestamp() > ts) {
            if (i != 0) { // ??????
              int polygonId = polygonIds.get(v);
              Pair<Integer, Long> idx = new ImmutablePair<>(polygonId, ts);
              if (timestampActivity.containsKey(idx)) {
                timestampActivity.put(idx, timestampActivity.get(idx) + 1l);
              } else {
                timestampActivity.put(idx, 1l);
              }
            }

            v++;
          }


          if (v >= versions.size())
            break;

        }

      }

      return timestampActivity;

    }).reduce(Collections.emptyMap(), (partial, b) -> {

      Map<Pair<Integer, Long>, Long> sum = new TreeMap<>();
      sum.putAll(partial);
      for (Map.Entry<Pair<Integer, Long>, Long> entry : b.entrySet()) {

        Long activity = partial.get(entry.getKey());
        if (activity == null) {

          activity = entry.getValue();

          if (activity == null) {
            activity = 0l;
          }

        } else {
          Long newActivity = entry.getValue();
          // if (newActivity == null){ newActivity = Long.valueOf(0); }
          activity = activity + newActivity;

        }
        sum.put(entry.getKey(), activity);
      }

      // System.out.println(sum);

      return sum;
    }

    );

    // fill missing values with 0
    for (int i=0; i<polygons.size(); i++) {
      for (Long ts : timestamps.subList(1, timestamps.size())) {
        ;//superresult.putIfAbsent(new ImmutablePair<>(i, ts), 0l);
      }
    }
    //System.out.println("1 Polygon done.");
    return superresult;

  }



  @SuppressWarnings("static-access")
  public static void main(String[] args) throws SQLException { 
    org.apache.log4j.BasicConfigurator.configure();

    //options start---------------------------------------
    Option help = OptionBuilder.withLongOpt("help").withDescription("Print this help.").create('?');    
    Option configFile = OptionBuilder.hasArg().withArgName("config-file").withDescription("Configuration File").withLongOpt("config").isRequired().create("cf");
   
    Options options = new Options();    
    options.addOption(configFile);    
    options.addOption(help);

    HelpFormatter formatter = new HelpFormatter();
   
    CommandLineParser parser = new PosixParser();

    CommandLine line;
    OSMatrixProcessor op = new OSMatrixProcessor();
    
    try {
      line = parser.parse(options, args);
      pathToConfigFile = line.getOptionValue(configFile.getOpt());
      
      
      logger.info("using: <" + pathToConfigFile + "> as config File");
      // get all configs from json
   
      
      op.start();
      
    } catch (org.apache.commons.cli.ParseException e) {
      System.out.println("Unexpected exception:" + e.getMessage());
      formatter.printHelp("osmatrix [options]", options);
    } 
    
  }
  
  private boolean init() {

    try {
      logger.info("Starting initialization....");

      logger.info("parsing conifg file ...");

      JSONParser jsonParser = new JSONParser();
      Object configFileObject = jsonParser.parse(new FileReader(pathToConfigFile + ""));

      JSONObject config = (JSONObject) configFileObject;

      JSONObject osmatrixDbConfig = (JSONObject) config.get("osmatrix-db");
      JSONObject oshDbConfig = (JSONObject) config.get("osh-db");
      JSONObject tempDbConfig = (JSONObject) config.get("temp-db");
      JSONObject timestampsConfig = (JSONObject) config.get("timestamps");

      JSONArray attributesConfig = (JSONArray) config.get("attributes");

      // TODO get Timestamps from config.json

      logger.info("generating timestamps");
      OSMTimeStamps timestamps = new OSMTimeStamps(2012, 2013, 1, 9);
      timestampsList = timestamps.getTimeStamps(); //TODO net gut
      Collections.sort(timestampsList, Collections.reverseOrder());

      // get connection to oshdb
      OshDBManager oshmgr = new OshDBManager(oshDbConfig.get("connection").toString(), oshDbConfig.get("user").toString(), oshDbConfig.get("password").toString());
      
      //TODO das muss man noch ander machen
      Connection h2Conn = oshmgr.createOshDBConnection();
      // create lookup tables
      TagLookup tlookup = new TagLookup(h2Conn);

      // get connection to osmatrix db
      OSMatrixDBManager osmatrixmgr = new OSMatrixDBManager(osmatrixDbConfig.get("connection").toString(), osmatrixDbConfig.get("user").toString(), osmatrixDbConfig.get("password").toString());
      
      mapTypId = osmatrixmgr.getAttrAndId();
      
      ResultSet osmatrixCells = osmatrixmgr.getOSMatrixDBConnection().createStatement().executeQuery("SELECT id, geom FROM cells");
      //FeatureCollection<Polygon, Feature>
      SpatialIndexFeatureCollection cellsIndex  = new SpatialIndexFeatureCollection();
      
      SimpleFeatureSource sfsource; 
      
      while (osmatrixCells.next()) {
       
//        SimpleFeature feature = new 
//        cellsIndex.add(feature);
        
        //new SpatialIndexFeatureCollection(grid.getFeatures());
      }
      
      

      DataSource dataSource = TempDBManager.getDataSource(tempDbConfig.get("connection").toString(),
          tempDbConfig.get("user").toString(), tempDbConfig.get("password").toString());
     
      logger.info("TempDB Connection Pool established.");
     
      Connection connection = dataSource.getConnection();
      System.out.println("The Connection Object is of Class: " + connection.getClass());

//      PreparedStatement  pstmt = connection
//          .prepareStatement("INSERT INTO attributes_temp (cell_id, attribute_type_id, value, valid) VALUES (?,?,?,?)");
//
//      pstmt.setInt(1, 100);
//      pstmt.setInt(2, 2);
//      pstmt.setDouble(3, 123.1234);
//      pstmt.setInt(4, 1000);
//
//      pstmt.execute();
        
   
      
      // starting init phase

      // load all attributes listed in config.json
      for (int i = 0; i < attributesConfig.size(); i++) {
        Class<Attribute> clazz;

        try {
          clazz = (Class<Attribute>) Class
              .forName("org.heigit.bigspatialdata.oshdb.examples.osmatrix." + attributesConfig.get(i).toString());

          // clazz = (Class<Attribute>)
          // Class.forName("org.heigit.bigspatialdata.oshdb.examples.osmatrix.TotalAreaVineyards");
          addAttribute(clazz);

        } catch (ClassNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (InstantiationException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (IllegalAccessException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

      }
      
      
      Set<String> attrTypes = mapTypeAttribute.keySet();
      
      for (String type : attrTypes) {
        Attribute attr = mapTypeAttribute.get(type);
               
        if (attr == null) {
          logger.error("attribute[" + type + "] is null!");
          continue;
        }
        Timestamp ts = new Timestamp(timestampsList.get(0)/1000);
        osmatrixmgr.insertOSMatrixAttributeTypes(type, attr.getDescription(), type, ts);
       
      } 
      
    StringBuilder sb;
    {
      Set<String> types = mapTypeAttribute.keySet();
      sb = new StringBuilder("\n Registered Types:");
      for (String type : types) {
        sb.append("\n  ");
        sb.append(type).append(":\t\t\t");
        sb.append(mapTypId.get(type)).append(" - ");
        sb.append(mapTypeAttribute.get(type).getClass().getName());
      }
      logger.debug(sb.toString());
      // System.out.println(sb.toString());
    }

    {
      sb = new StringBuilder("\n Table Dependencies:\n");
      Set<TABLE> tables = mapTableTypeDep.keySet();
      for (TABLE table : tables) {
        sb.append("  ");
        sb.append(table).append(": ");
        List<String> types = mapTableTypeDep.get(table);
        for (String type : types) {
          sb.append(type).append(", ");
        }
        sb.delete(sb.lastIndexOf(","), sb.length());
        sb.append("\n");
      }
      sb.delete(sb.lastIndexOf("\n"), sb.length());
      logger.debug(sb.toString());
    }
    
    // TODO init temp table
    
    
    
    return true;
      
    } catch (FileNotFoundException e) {
      logger.error("config file not found!");
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (org.json.simple.parser.ParseException e) {
      logger.error("parsing config file went wrong!");
      e.printStackTrace();
    } catch (SQLException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    return false;

    // System.out.println("!!!!!!!!!!!!!!!!!" +
    // tlookup.getAllKeyValues().get("building").get("yes"));

  }
  

  
  private void addAttribute(Class<? extends Attribute> clazz) //erwarted klassendefiniion die von attribute abgeleitet ist
      throws InstantiationException, IllegalAccessException {

    Attribute attr = clazz.newInstance();
    if (attr != null) {
      String attrName = attr.getName(); // Attribute class name
      attrName = attrName.trim().replace(" ", "_");
      attr.setName(attrName);

      List<TABLE> tableDependencies = attr.getDependencies();
      if (attrName.length() > 0 && !mapTypeAttribute.containsKey(attrName)) {
        mapTypeAttribute.put(attrName, attr);
        for (TABLE table : tableDependencies) {
          List<String> types = mapTableTypeDep.get(table);
          if (types == null) {
            types = new ArrayList<String>();
            mapTableTypeDep.put(table, types);
          }
          types.add(attrName);
        }
      }
    }
  }

  }
