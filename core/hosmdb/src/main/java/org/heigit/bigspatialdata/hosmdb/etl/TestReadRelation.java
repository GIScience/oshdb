package org.heigit.bigspatialdata.hosmdb.etl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Paths;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.WayRelation;

public class TestReadRelation {
  //test to read data form .ser file (for controller :-)

  public static void main(String[] args) {

    File nodeRelationFile = Paths.get("./", "temp_waysForRelation.ser").toFile();

    try (final FileInputStream fileStream = new FileInputStream(nodeRelationFile);
        final BufferedInputStream bufferedStream = new BufferedInputStream(fileStream);
        final ObjectInputStream relationStream = new ObjectInputStream(bufferedStream)) {

    	WayRelation nr = (WayRelation) relationStream.readObject();
      //while (nr.way().getId() < 883702275) {
      //  nr = (WayRelation) relationStream.readObject();
      // }
      System.out.printf("(%d) -> %d\n",nr.getMaxRelationId(),nr.way().getId());
      for(int i=0; i< 10; i++){
        nr = (WayRelation) relationStream.readObject();
        System.out.printf("(%d) -> %d\n",nr.getMaxRelationId(),nr.way().getId());
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
