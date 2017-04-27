package org.heigit.bigspatialdata.oshdb.etl;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Paths;

import org.heigit.bigspatialdata.oshdb.etl.transform.data.WayRelation;

public class TestReadRelation {
  //test to read data form .ser file (for controller :-)

  public static void main(String[] args) {

    File nodeRelationFile = Paths.get("./", "temp_waysForRelation.ser").toFile();

    try (//get ObjectInputStream form file
            final FileInputStream fileStream = new FileInputStream(nodeRelationFile);
        final BufferedInputStream bufferedStream = new BufferedInputStream(fileStream);
        final ObjectInputStream relationStream = new ObjectInputStream(bufferedStream)) {

    	
    	try{
    	while(true){
    		WayRelation nr = (WayRelation) relationStream.readObject();
    		System.out.printf("(%d) -> %d\n",nr.getMaxRelationId(),nr.way().getId());
    	}
    	}catch(EOFException e){
    		System.out.println(e);
    	}
    	
    	
      

    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }

}
