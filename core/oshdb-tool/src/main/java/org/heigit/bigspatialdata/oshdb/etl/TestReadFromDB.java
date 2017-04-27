package org.heigit.bigspatialdata.oshdb.etl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;

public class TestReadFromDB {
  //or how to get data

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
    Class.forName("org.h2.Driver");

    try (Connection conn = DriverManager.getConnection("jdbc:h2:./hosmdb_relation", "sa", "");
            Statement stmt = conn.createStatement()) {

      //collect all relations into a variable (serialised)
      ResultSet rst = stmt.executeQuery("select data from grid");

      while (rst.next()) {

        //convert to Object-Stream (Cells) [has only one object, which is the Cell]
        ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(1));

        //get one object (Cell) from the stream
        GridOSHRelations hosmCell = (GridOSHRelations) ois.readObject();

        //three possibilities to get Data from this Cell:
        //1. use a Java8 Stream:
        Stream<OSHEntity> stream = StreamSupport.stream(hosmCell.spliterator(), false);
        //count relations in cell
        System.out.println("Stream: " + stream.count());
        //System.out.printf("\nLevel:%d, Id:%d, CountRelations:%d\n", hosmCell.getLevel(),hosmCell.getId(), stream.count());

        //2. use foreach loop
        //HOSMRealtion in cell
        for (Object hosmr : hosmCell) {
          //OSMRelation in HOSMRelation
          OSHRelation hosmrel = (OSHRelation)hosmr;
          for (OSMRelation osmr : hosmrel) {
            System.out.println("For: " + osmr.getVersion());
          }
        }

        //3. iterator
        Iterator<OSHRelation> hosmrI = hosmCell.iterator();
        while (hosmrI.hasNext()) {
          OSHRelation temp = hosmrI.next();
          Iterator<OSMRelation> osmrI = temp.iterator();
          while (osmrI.hasNext()) {
            OSMRelation temp2 = osmrI.next();
            System.out.println("Iterator: " + temp2.getVersion());
          }
        }
        /*
        stream = StreamSupport.stream(hosmCell.spliterator(), false);
        stream.forEach(hway -> {
          System.out.printf("%d (%s),", hway.getId(), hway.getBoundingBox());
          try {
            System.out.println(hway.getNodes().size());
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }

        });
         */


 /*
         * stream = StreamSupport.stream(hosmCell.ways().spliterator(), false); stream.forEach(hway
         * -> {
         * 
         * 
         * if (hway.getId() == 154680944) { try { List<HOSMNode> hnodes = hway.getNodes(); for
         * (HOSMNode hnode : hnodes) { Iterator<OSMNode> node = hnode.iterator(); while
         * (node.hasNext()) { OSMNode n = node.next(); System.out.printf("%d %d (%f %f)\n",
         * n.getId(), n.getVersion(), n.getLongitude(), n.getLatitude()); } } } catch (IOException
         * e) { // TODO Auto-generated catch block e.printStackTrace(); } }
         * 
         * 
         * });
         */
      }
    }

  }

}
