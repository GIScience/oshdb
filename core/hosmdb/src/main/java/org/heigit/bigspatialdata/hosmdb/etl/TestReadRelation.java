package org.heigit.bigspatialdata.hosmdb.etl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Paths;

import org.heigit.bigspatialdata.hosmdb.etl.transform.data.NodeRelation;

public class TestReadRelation {

  public static void main(String[] args) {

    File nodeRelationFile = Paths.get("./", "nodesForRelation.ser").toFile();

    try (final FileInputStream fileStream = new FileInputStream(nodeRelationFile);
        final BufferedInputStream bufferedStream = new BufferedInputStream(fileStream);
        final ObjectInputStream relationStream = new ObjectInputStream(bufferedStream)) {

      NodeRelation nr = (NodeRelation) relationStream.readObject();
      while (nr.node().getId() < 883702275) {
        nr = (NodeRelation) relationStream.readObject();
      }
      System.out.printf("(%d) -> %d\n",nr.getMaxRelationId(),nr.node().getId());
      for(int i=0; i< 10; i++){
        nr = (NodeRelation) relationStream.readObject();
        System.out.printf("(%d) -> %d\n",nr.getMaxRelationId(),nr.node().getId());
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
