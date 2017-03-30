package org.heigit.bigspatialdata.hosmdb.etl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongBinaryOperator;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.heigit.bigspatialdata.hosmdb.grid.HOSMCellNodes;
import org.heigit.bigspatialdata.hosmdb.grid.HOSMCellRelations;
import org.heigit.bigspatialdata.hosmdb.grid.HOSMCellWays;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMNode;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMRelation;
import org.heigit.bigspatialdata.hosmdb.osh.HOSMWay;
import org.heigit.bigspatialdata.hosmdb.osm.OSMNode;



public class TestReadFromDB {

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
    Class.forName("org.h2.Driver");

    try (Connection conn = DriverManager.getConnection("jdbc:h2:./hosmdb_relation", "sa", "");
        Statement stmt = conn.createStatement()) {

      ResultSet rst = stmt.executeQuery("select data from grid");

      while (rst.next()) {

        ObjectInputStream ois = new ObjectInputStream(rst.getBinaryStream(1));

        // System.out.println(ois.readObject());


        HOSMCellRelations hosmCell = (HOSMCellRelations) ois.readObject();


        Stream<HOSMRelation> stream = StreamSupport.stream(hosmCell.spliterator(), false);
        System.out.printf("\nLevel:%d, Id:%d, CountRelations:%d\n", hosmCell.getLevel(),hosmCell.getId(), stream.count());


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
