package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
    POLYGON,
    /** The line. */
    LINE,
    /** The point. */
    POINT
  }
  

  private Map<TABLE, List<String>> mapTableTypeDep = new HashMap<TABLE, List<String>>();
  
  private Map<String, Attribute> mapTypeAttribute = new HashMap<String, Attribute>();
  
  public static void main(String[] args) { //options start---------------------------------------
    
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
      
      for (int i = 0; i < attributes.size(); i++) {
        //addAttribute(attributes.get(i));
      }
      
      
      System.out.println(attributes.size());
      
      System.out.println(osmatrixdb.get("connection")+ " " + osmatrixdb.get("user") );
     
      
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


  
  private void addAttribute(Class<? extends Attribute> clazz) //erwarted klassendefiniion die von attribute abgeleitet ist
      throws InstantiationException, IllegalAccessException {

    Attribute attr = clazz.newInstance();
    if (attr != null) {
      String type = attr.getName(); // Attribute class name
      type = type.trim().replace(" ", "_");
      attr.setType(type);

      List<TABLE> dependencies = attr.getDependencies();
      if (type.length() > 0 && !mapTypeAttribute.containsKey(type)) {
        mapTypeAttribute.put(type, attr);
        for (TABLE table : dependencies) {
          List<String> types = mapTableTypeDep.get(table);
          if (types == null) {
            types = new ArrayList<String>();
            mapTableTypeDep.put(table, types);
          }
          types.add(type);
        }
      }
    }
  }

  }
