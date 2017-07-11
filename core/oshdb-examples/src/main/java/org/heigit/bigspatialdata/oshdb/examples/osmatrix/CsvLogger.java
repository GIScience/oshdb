package org.heigit.bigspatialdata.oshdb.examples.osmatrix;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;

public class CsvLogger {

  public static void logCsv(OSMEntity osm) {
    // TODO Auto-generated method stub
    
  }

  public static void logCsv(Geometry osmGeometry) {
    // TODO Auto-generated method stub
    
  }
  
  public static void logCsv(Geometry osmGeometry, String[] array) {
    // TODO Auto-generated method stub
    
  }

  public static void logCsv(Geometry osmGeometry, long id, long ts,String fileName) {
    WKTWriter wkt = new WKTWriter();
    String  mywkt = wkt.write(osmGeometry);
    System.out.println(mywkt + ";" + id + ";" + ts);
    
//    File file = new File(fileName);
//    
//    try {
//      if(file.exists()) {
//       
//        
//      }
//        
//      FileWriter fw = new FileWriter(file);
//      fw.append(mywkt+";"+id+";"+ts + "\n");
//      fw.close();
//      
//    } catch (IOException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
    
  }
  
  

}
