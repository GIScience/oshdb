package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.measure.unit.SystemOfUnits;
import javax.sql.DataSource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.heigit.bigspatialdata.oshdb.utils.OSMTimeStamps;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


public class OSMatrixProcessor {
  
  private static final Logger logger = Logger.getRootLogger(); //apache log4J
  
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
      List<Long> timestampsList = timestamps.getTimeStamps();


      // get connection to oshdb
      OshDBManager oshmgr = new OshDBManager(oshDbConfig.get("connection").toString(), oshDbConfig.get("user").toString(), oshDbConfig.get("password").toString());
      
      //TODO das muss man noch ander machen
      Connection h2Conn = oshmgr.createOshDBConnection();
      // create lookup tables
      TagLookup tlookup = new TagLookup(h2Conn);

      // get connection to osmatrix db
      OSMatrixDBManager osmatrixmgr = new OSMatrixDBManager(osmatrixDbConfig.get("connection").toString(), osmatrixDbConfig.get("user").toString(), osmatrixDbConfig.get("password").toString());
      mapTypId = osmatrixmgr.getAttrAndId();
      
      
      
      TempDBManager tmpdb = new TempDBManager();

      DataSource dataSource = TempDBManager.getDataSource(tempDbConfig.get("connection").toString(),
          tempDbConfig.get("user").toString(), tempDbConfig.get("password").toString());
     
     
      Connection connection = dataSource.getConnection();
      System.out.println("The Connection Object is of Class: " + connection.getClass());

      PreparedStatement  pstmt = connection
          .prepareStatement("INSERT INTO attributes_temp (cell_id, attribute_type_id, value, valid) VALUES (?,?,?,?)");

      pstmt.setInt(1, 100);
      pstmt.setInt(2, 2);
      pstmt.setDouble(3, 123.1234);
      pstmt.setInt(4, 1000);

      pstmt.executeBatch();
        

    

      
      
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
        System.out.println("adsfadsf" + type);
        
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
      sb = new StringBuilder("Registered Types:");
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
      sb = new StringBuilder("Table Dependencies:\n");
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

    //
    
    
    
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
