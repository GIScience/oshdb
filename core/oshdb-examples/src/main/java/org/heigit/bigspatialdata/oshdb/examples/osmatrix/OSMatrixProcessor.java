package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.FileNotFoundException;
import java.io.FileReader;
//getOSMatrixCell
//getOSMatrixCell
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.geometry.jts.JTS;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.*;
import org.heigit.bigspatialdata.oshdb.osm.*;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;

public class OSMatrixProcessor {
  
 
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
      
      
      
      //System.out.println(config.get());
     
      
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
  
  

  }
