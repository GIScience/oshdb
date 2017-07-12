package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.geotools.data.postgis.PostgisNGDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureSource;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;

import org.heigit.bigspatialdata.oshdb.utils.OSMTimeStamps;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
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
  
  private static Map<String, Attribute> mapTypeAttribute = new HashMap<String, Attribute>();
  
  private static Map<String, Integer> mapTypId = new HashMap<String, Integer>();
  
  private static List<Long> timestampsList;
  
  private static TagLookup tagLookup;
  
  private OshDBManager oshmgr;
  
  DataSource dataSource;
  
  BoundingBox inputBbox;
  
  static SimpleFeatureSource cellsIndex;// = new SpatialIndexFeatureCollection();
  
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
    try {
      execute();
      //create view 
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void execute()
      throws ClassNotFoundException, ParseException, IOException {
//    System.out.println("execute");
    Connection oshdbCon = oshmgr.createOshDBConnection();
    
   

    XYGridTree grid = new XYGridTree(12);

    final List<CellId> cellIds = new ArrayList<>();

    
    grid.bbox2CellIds(inputBbox, true).forEach(cellIds::add);
    
    
    
    

    // start processing in parallel all grid cells that relate to the input

    //TODO implement different workflows for count, unique, etc...? 
    

    
    
    
//    System.out.println("before flatmap");
    System.out.println("# of grid cells " + cellIds.size());
    //AttributeCells superresult =
      cellIds.parallelStream().flatMap(cellId -> {
    
//    System.out.println("flatmap");
      
    try (final PreparedStatement pstmt = oshdbCon.prepareStatement(

        "(select data from grid_node where level = ?1 and id = ?2) "
        + "union (select data from grid_way where level = ?1 and id = ?2) "
        + "union (select data from grid_relation where level = ?1 and id = ?2)")) {
       // "(select data from grid_node where level = ?1 and id = ?2)")) {
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
    }).map( this::mapper )
      .forEach(r -> {


        
        try (
          Connection connection = dataSource.getConnection();      

      PreparedStatement  pstmt = connection
       //   .prepareStatement("INSERT INTO attributes_temp (cell_id, attribute_type_id, value, valid) VALUES (?,?,?,?)");
          //.prepareStatement("INSERT INTO attributes_temp (cell_id, attribute_type_id, value, valid ) VALUES (?,?,?,(SELECT id FROM times WHERE times.time = ?),(SELECT (id + 1) as id FROM times WHERE times.time = ?)");
            .prepareStatement("INSERT INTO attributes_temp (cell_id, attribute_type_id, value, valid ) VALUES (?,?,?,(SELECT id FROM times WHERE times.time = ?))");

      ){
      
        
        for (Map.Entry<Integer, CellTimeStamps>  attributeCell : ((AttributeCells)r).map.entrySet()){
          
          final CellTimeStamps cellTimestamps = attributeCell.getValue();
          
          for ( Map.Entry<Long, TimeStampValuesWeights> cellTimestamp : cellTimestamps.map.entrySet()){
            
            final TimeStampValuesWeights timestampValueWeights = cellTimestamp.getValue();
            
            for ( Map.Entry<Long, ValueWeight> timestampValueWeight : timestampValueWeights.map.entrySet() ){
              
              final int attributeId = attributeCell.getKey();
              final long cellId = cellTimestamp.getKey();
              final long ts = timestampValueWeight.getKey();
              double value = timestampValueWeight.getValue().getValue();
              double weight = timestampValueWeight.getValue().getWeight();
              Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
              Timestamp sqlTime = new Timestamp(ts*1000);
              
              pstmt.setLong(1, cellId);
              pstmt.setInt(2,attributeId );
              pstmt.setDouble(3, value);
              pstmt.setTimestamp(4, sqlTime,cal);
            //  pstmt.setTimestamp(5, sqlTime, cal);
              
              
              pstmt.addBatch();
              
            // System.out.println(attributeId + ";" + cellId + ";" + ts*1000 + ";" + value);
            }
            
          }
          
        }
        pstmt.executeBatch();
      //  logger.info("commit");
        connection.commit();
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      });  


  }

  public  AttributeCells mapper(GridOSHEntity<OSHEntity> gridCell){
    

      
      GridOSHEntity cell = (GridOSHEntity) gridCell;
      
      //TODO hier sollte special object hin
      AttributeCells gridcellOutput = new AttributeCells();
      
      //Map<Pair<Integer,Long>, Long> timestampActivity = new TreeMap<>(); //TODO remove
      
      for (OSHEntity<OSMEntity> osh : (Iterable<OSHEntity<OSMEntity>>) cell) {        

        for (Map.Entry<String, Attribute> entry : mapTypeAttribute.entrySet()){
          Attribute attribute = entry.getValue();
          int attributeId = mapTypId.get(entry.getKey());
          //System.out.println(attributeId + "...");
          AttributeCells oshresult =  attribute.compute(cellsIndex,osh,tagLookup, timestampsList, attributeId);
          
          //System.out.println("oshresult " + oshresult.get(2));
          attribute.aggregate(gridcellOutput,oshresult,timestampsList);
          
          
        }
          
        
      }   
     
      
      return gridcellOutput; 

  }

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws SQLException { 
    
    Class psql = org.postgresql.Driver.class;
    Class h2 = org.h2.Driver.class;
    System.out.println(psql);
    System.out.println(h2);
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
  
  @SuppressWarnings("unchecked")
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
      JSONArray processingbbox = (JSONArray) config.get("bbox");
      
      inputBbox = new BoundingBox(
          Double.parseDouble(processingbbox.get(0).toString()),
          Double.parseDouble(processingbbox.get(1).toString()), 
          Double.parseDouble(processingbbox.get(2).toString()),
          Double.parseDouble(processingbbox.get(3).toString())
          );

      // TODO get Timestamps from config.json

      logger.info("generating timestamps");
      OSMTimeStamps timestamps = new OSMTimeStamps(2008, 2017, 1, 1);
      timestampsList = timestamps.getTimeStampIds(); //TODO net gut
      //Collections.sort(timestampsList, Collections.reverseOrder());

      // get connection to oshdb
      oshmgr = new OshDBManager(oshDbConfig.get("connection").toString(), oshDbConfig.get("user").toString(), oshDbConfig.get("password").toString());
      
      //TODO das muss man noch ander machen
      Connection h2Conn = oshmgr.createOshDBConnection();
      
      Connection h2conTags = null;
      try {
        h2conTags = DriverManager.getConnection("jdbc:h2:/home/lloos/Data/africaKeytables", "sa", "");
        logger.info("connection to h2 key table connection established");
        
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        System.err.println("h2 Tag Connection failed.");
        e.printStackTrace();
      }
      // create lookup tables
      tagLookup = new TagLookup(h2conTags);

      // get connection to osmatrix db
      OSMatrixDBManager osmatrixmgr = new OSMatrixDBManager(osmatrixDbConfig.get("connection").toString(), osmatrixDbConfig.get("user").toString(), osmatrixDbConfig.get("password").toString());
      
      osmatrixmgr.truncateAttributeTempTable();
      logger.info("attributes_temp Table truncated!");
      mapTypId = osmatrixmgr.getAttrAndId();
      
     // ResultSet osmatrixCells = osmatrixmgr.getOSMatrixDBConnection().createStatement().executeQuery("SELECT id, ST_AsText(geom) FROM cells");
      PostgisNGDataStoreFactory bla = new PostgisNGDataStoreFactory();
      
      Map<String,Object> params = new HashMap<>();
      params.put( "dbtype", "postgis");
      params.put( "host", "lemberg.geog.uni-heidelberg.de");
      params.put( "port", 5432);
      params.put( "schema", "public");
      params.put( "database", "osmatrixhd");
      params.put( "user", "osmatrix");
      params.put( "passwd", "osmatrix2016");
      
      DataStore dataStore = DataStoreFinder.getDataStore(params);
     System.out.println(dataStore.getSchema("cells").getGeometryDescriptor().getLocalName());
      
     // SimpleFeatureSource featureSource = dataStore.getFeatureSource("cells4326");
      SimpleFeatureSource featureSource = dataStore.getFeatureSource("cells");
      
//      final FilterFactory ff = CommonFactoryFinder.getFilterFactory();
//      Filter filter = ff.propertyLessThan( ff.property( "AGE"), ff.literal( 12 ) );
//      //FeatureCollection<Polygon, Feature>
      System.out.println(featureSource.getFeatures().getSchema().getGeometryDescriptor().getLocalName());
      cellsIndex  = DataUtilities.source( new SpatialIndexFeatureCollection(featureSource.getFeatures()) );
      
      dataStore.dispose();
      
     // System.out.println(cellsIndex.getBounds().getArea());
      
      //WKTReader wktreader = new WKTReader();
       

      
      

      dataSource = TempDBManager.getDataSource(tempDbConfig.get("connection").toString(),
          tempDbConfig.get("user").toString(), tempDbConfig.get("password").toString());
     
      logger.info("TempDB Connection Pool established.");
     
      //Connection connection = dataSource.getConnection();      

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
        osmatrixmgr.truncateTimesTable();
//        List<Long> TtimestampsListTMP = new List<Long>()
//            timestampsList;
        osmatrixmgr.fillTimesTable(timestampsList);
       
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
      System.out.println("addAttribute " + attrName);
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
