package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class OSMatrixProcessor {
  
  public enum TABLE { // could be one of those three
    /** The polygon. */
    RELATION,
    /** The line. */
    WAY,
    /** The point. */
    NODE
  }
  

  private Map<TABLE, List<String>> mapTableTypeDep = new HashMap<TABLE, List<String>>();
  
  private Map<String, Attribute> mapTypeAttribute = new HashMap<String, Attribute>();
  
  public static void main(String[] args) throws SQLException { //options start---------------------------------------
    
    Option help = OptionBuilder.withLongOpt("help").withDescription("Print this help.").create('?');
    
    Option configFile = OptionBuilder.hasArg().withArgName("config-file").withDescription("Configuration File")
        .withLongOpt("config").isRequired().create("cf");
   
    Options options = new Options();
    
    options.addOption(configFile);    
    options.addOption(help);

    HelpFormatter formatter = new HelpFormatter();
   
    CommandLineParser parser = new PosixParser();

    CommandLine line;
    try {
      line = parser.parse(options, args);
      String pathToConfigFile = line.getOptionValue(configFile.getOpt());
      System.out.println("using config file: "+ pathToConfigFile);
      
      JSONParser jsonParser = new JSONParser();
      
      Object configFileObject = jsonParser.parse(new FileReader(pathToConfigFile));
      
      JSONObject config = (JSONObject)configFileObject;     
      
      JSONObject osmatrixdb = (JSONObject) config.get("osmatrix-db");
      JSONObject oshdb = (JSONObject) config.get("osh-db");
      JSONObject tempdb = (JSONObject) config.get("temp-db");
      JSONObject timestamps = (JSONObject) config.get("timestamps");
      
      JSONArray attributes = (JSONArray) config.get("attributes");
      
      OSMatrixProcessor op = new OSMatrixProcessor();
      //starting init phase
      for (int i = 0; i < attributes.size(); i++) {
        Class<Attribute> clazz;
        
        try {
          clazz = (Class<Attribute>) Class.forName("org.heigit.bigspatialdata.oshdb.examples.osmatrix."+ attributes.get(i).toString());
  
          //clazz = (Class<Attribute>) Class.forName("org.heigit.bigspatialdata.oshdb.examples.osmatrix.TotalAreaVineyards");
          op.addAttribute(clazz);
         // TotalAreaVineyards tota = new TotalAreaVineyards();
          
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
      
      System.out.println("mapTypeAttribute: " + op.mapTypeAttribute + " mapTableTypeDep: " + op.mapTableTypeDep);
      
      System.out.println("attributes size: " + attributes.size());
      
      System.out.println(osmatrixdb.get("connection")+ " " + osmatrixdb.get("user") );
      
      
      
      OshDBManager oshmgr = new OshDBManager(oshdb.get("connection").toString(), oshdb.get("user").toString(), oshdb.get("password").toString());
      
      Connection h2Conn = oshmgr.createOshDBConnection();
      TagLookup tlookup = new TagLookup(h2Conn);
      
      //
      //TODO init temp table
      //TODO query attribute_types from osmatrix
      //TODO 
      
      
      System.out.println("!!!!!!!!!!!!!!!!!" + tlookup.getAllKeyValues().get("building").get("yes"));
      
      
      processNodeTiles();
      
      

      
    } catch (org.apache.commons.cli.ParseException e) {
      System.out.println("Unexpected exception:" + e.getMessage());
      formatter.printHelp("osmatrix [options]", options);
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (org.json.simple.parser.ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }
  

  
  private static void processNodeTiles() {
    
    
    
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
