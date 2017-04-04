package org.heigit.bigspatialdata.hosmdb.etl;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.CellInfo;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.CellNode;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.CellRelation;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.CellWay;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.NodeRelation;
import org.heigit.bigspatialdata.hosmdb.etl.transform.data.WayRelation;
import org.heigit.bigspatialdata.hosmdb.etl.transform.node.TransformNodeMapper;
import org.heigit.bigspatialdata.hosmdb.etl.transform.node.TransformNodeMapper.Result;
import org.heigit.bigspatialdata.hosmdb.etl.transform.relation.TransformRelationMapper;
import org.heigit.bigspatialdata.hosmdb.etl.transform.way.TransformWayMapper;
import org.heigit.bigspatialdata.hosmdb.grid.HOSMCellNodes;
import org.heigit.bigspatialdata.hosmdb.grid.HOSMCellRelations;
import org.heigit.bigspatialdata.hosmdb.grid.HOSMCellWays;
import org.heigit.bigspatialdata.hosmdb.osm.OSMNode;
import org.heigit.bigspatialdata.hosmdb.util.XYGrid;

/**
 * HOSMDbTransform
 * 
 * inputs from HOSMDbExtract - KeyTables (KeyValues, Roles, User) - Relation Mappings node2Way,
 * node2Relation, way2Relation, relation2Relation
 * 
 * output - transformed pbffile nodes with Strings replaced by references - transformed pbffile ways
 * with Strings replaced by references - transformed pbffile relations with Strings replaced by
 * references
 * 
 * 
 * @author Rafael Troilo <rafael.troilo@uni-heidelberg.de>
 */

public class HOSMDbTransform {



  public static void extract(String[] args)
      throws FileNotFoundException, IOException, SQLException, ClassNotFoundException {
    final CommandLineParser parser = new DefaultParser();
    final Options opts = buildCLIOptions();
    try {
      //parse input
      CommandLine cli = parser.parse(opts, args);

      final String pbfFile = cli.getOptionValue("pbf");
      final String tmpDir = cli.getOptionValue("tmpDir", "./");

      //set some basic variables
      final int maxZoom = 12;
      final String n2wRelationFile = tmpDir + "temp_nodesForWays.ser";
      final String n2rRelationFile = tmpDir + "temp_nodesForRelation.ser";
      final String w2rRelationFile = tmpDir + "temp_waysForRelation.ser";
      final XYGrid grid = new XYGrid(12);

      Class.forName("org.h2.Driver");

      // serialise nodes and write to .ser-file
      if (true) {
        try (//
                final FileInputStream in = new FileInputStream(pbfFile) //
                ) {
          //define parts of file so it can be devided between threads
          TransformNodeMapper nodeMapper = new TransformNodeMapper(maxZoom);
          TransformNodeMapper.Result nodeResult = nodeMapper.map(in);

          if (true) {
            System.out.println("Saving Node Grid");
            //create grid cells containing all nodes in that cell:
            // header; node1v1, node1v2,...,noder2v1,...nodeNvN (deltaencaoded and serialized as you know it)
            //THIS is the MAGIC!!!
            saveGrid(nodeResult, grid);

            //delete previous files
            Path pN2W = Paths.get(tmpDir, n2wRelationFile);
            if (pN2W.toFile().exists())
              Files.delete(pN2W);
            Path pN2R = Paths.get(tmpDir, n2rRelationFile);
            if (pN2R.toFile().exists())
              Files.delete(pN2R);
            System.out.println("Saving NodesForRelation");
            //temp_store serialized nodes so they can be read when creating a way or relation
            saveNodesForRelations(nodeResult, pN2W.toFile(), pN2R.toFile());
          }
        }
      }

      // ways
      // do same for ways and relations (see nodes)
      if (true) {
        try (//
            final FileInputStream in = new FileInputStream(pbfFile) //
        ) {

          System.out.println("Start Way Mapper");
          TransformWayMapper wayMapper = new TransformWayMapper(maxZoom, n2wRelationFile);
          TransformWayMapper.Result wayResults = wayMapper.map(in);
          System.out.println("Saving Way Grid");
          saveGrid(wayResults);
          
          Path pW2R = Paths.get(tmpDir, w2rRelationFile);
          if (pW2R.toFile().exists())
            Files.delete(pW2R);
          
          saveWaysForRelations(wayResults, pW2R.toFile());
        }
      }

      //relations
      if (true) {
        try (//
                final FileInputStream in = new FileInputStream(pbfFile) //
                ) {
          System.out.println("Start Relation Mapper");
          TransformRelationMapper mapper = new TransformRelationMapper(maxZoom, n2rRelationFile, w2rRelationFile);
          TransformRelationMapper.Result result = mapper.map(in);
          System.out.println("Saving Relation Grid");
          saveGrid(result);
          
        }
            
      }
      
      
    } catch (ParseException exp) {
      System.err.println("Parsing failed.  Reason: " + exp.getMessage());
    }
  }


  private static void saveGrid(Result result, XYGrid grid) {
	    try (
	        Connection conn =
	            DriverManager.getConnection("jdbc:h2:./hosmdb_node;COMPRESS=TRUE", "sa", "");
	        Statement stmt = conn.createStatement()) {

	      stmt.executeUpdate(
	          "drop table if exists grid; create table if not exists grid(level int, id bigint, data blob,  primary key(level,id))");

	      PreparedStatement insert =
	          conn.prepareStatement("insert into grid (level,id,data) values(?,?,?)");

	      for (CellNode cell : result.getNodeCells()) {
	        insert.setInt(1, cell.info().getZoomLevel());
	        insert.setLong(2, cell.info().getId());



	        CellInfo cellInfo = cell.info();
	        MultiDimensionalNumericData cellDimensions = grid.getCellDimensions(cellInfo.getId());
	        double[] minValues = cellDimensions.getMinValuesPerDimension();
	        HOSMCellNodes hosmCell = HOSMCellNodes.rebase(cellInfo.getId(), cellInfo.getZoomLevel(),
	            cell.minId(), cell.minTimestamp(), (long) (minValues[0] / OSMNode.GEOM_PRECISION),
	            (long) (minValues[1] / OSMNode.GEOM_PRECISION), cell.getNodes());

	        ByteArrayOutputStream out = new ByteArrayOutputStream();
	        ObjectOutputStream oos = new ObjectOutputStream(out);
	        oos.writeObject(hosmCell);
	        oos.flush();
	        oos.close();
	        byte[] buf = out.toByteArray();
	        ByteArrayInputStream in = new ByteArrayInputStream(buf);
	        insert.setBinaryStream(3, in);
	        insert.executeUpdate();
	      }
	    } catch (SQLException | IOException e) {
	      e.printStackTrace();
	    }
	  }

  private static void saveGrid(TransformWayMapper.Result wayResults) {
    try (
        Connection conn = DriverManager.getConnection("jdbc:h2:./hosmdb_way;COMPRESS=TRUE", "sa", "");
        Statement stmt = conn.createStatement()) {

      stmt.executeUpdate(
          "drop table if exists grid; create table if not exists grid(level int, id bigint, data blob,  primary key(level,id))");

      PreparedStatement insert =
          conn.prepareStatement("insert into grid (level,id,data) values(?,?,?)");

      for (CellWay cell : wayResults.getCells()) {
        insert.setInt(1, cell.info().getZoomLevel());
        insert.setLong(2, cell.info().getId());



        CellInfo cellInfo = cell.info();

        XYGrid grid = new XYGrid(cellInfo.getZoomLevel());
        MultiDimensionalNumericData cellDimensions = grid.getCellDimensions(cellInfo.getId());
        double[] minValues = cellDimensions.getMinValuesPerDimension();
        HOSMCellWays hosmCell = HOSMCellWays.compact(cellInfo.getId(), cellInfo.getZoomLevel(),
            cell.minId(), cell.minTimestamp(), (long) (minValues[0] / OSMNode.GEOM_PRECISION),
            (long) (minValues[1] / OSMNode.GEOM_PRECISION), cell.getWays());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(hosmCell);
        oos.flush();
        oos.close();
        byte[] buf = out.toByteArray();
        ByteArrayInputStream in = new ByteArrayInputStream(buf);
        insert.setBinaryStream(3, in);
        insert.executeUpdate();
      }


    } catch (SQLException | IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }


  
  private static void saveGrid(TransformRelationMapper.Result result){
    try (
        Connection conn = DriverManager.getConnection("jdbc:h2:./hosmdb_relation;COMPRESS=TRUE", "sa", "");
        Statement stmt = conn.createStatement()) {

      stmt.executeUpdate(
          "drop table if exists grid; create table if not exists grid(level int, id bigint, data blob,  primary key(level,id))");

      PreparedStatement insert =
          conn.prepareStatement("insert into grid (level,id,data) values(?,?,?)");

      for (CellRelation cell : result.getCells()) {
        insert.setInt(1, cell.info().getZoomLevel());
        insert.setLong(2, cell.info().getId());



        CellInfo cellInfo = cell.info();

        XYGrid grid = new XYGrid(cellInfo.getZoomLevel());
        MultiDimensionalNumericData cellDimensions = grid.getCellDimensions(cellInfo.getId());
        double[] minValues = cellDimensions.getMinValuesPerDimension();
        HOSMCellRelations hosmCell = HOSMCellRelations.compact(cellInfo.getId(), cellInfo.getZoomLevel(),
            cell.minId(), cell.minTimestamp(), (long) (minValues[0] / OSMNode.GEOM_PRECISION),
            (long) (minValues[1] / OSMNode.GEOM_PRECISION), cell.getRelations());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(hosmCell);
        oos.flush();
        oos.close();
        byte[] buf = out.toByteArray();
        ByteArrayInputStream in = new ByteArrayInputStream(buf);
        insert.setBinaryStream(3, in);
        insert.executeUpdate();
      }


    } catch (SQLException | IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  

  private static void saveNodesForRelations(TransformNodeMapper.Result nodeResult, File n2wRelationFile, File n2rRelationFile)
      throws FileNotFoundException, IOException {
    
    try (//
        FileOutputStream fileOutput = new FileOutputStream(n2wRelationFile);
        BufferedOutputStream bufferedOutput = new BufferedOutputStream(fileOutput);
        ObjectOutputStream objectOutput = new ObjectOutputStream(bufferedOutput)) {
      for (NodeRelation nr : nodeResult.getNodesForWays()) {
        objectOutput.writeObject(nr);
      }
      objectOutput.flush();
    }
    
    try (//
        FileOutputStream fileOutput = new FileOutputStream(n2rRelationFile);
        BufferedOutputStream bufferedOutput = new BufferedOutputStream(fileOutput);
        ObjectOutputStream objectOutput = new ObjectOutputStream(bufferedOutput)) {
      for (NodeRelation nr : nodeResult.getNodesForRelations()) {
        objectOutput.writeObject(nr);
      }
      objectOutput.flush();
    }
  }

  private static void saveWaysForRelations(TransformWayMapper.Result result, File w2rRelationFile) throws FileNotFoundException, IOException {
    try (//
        FileOutputStream fileOutput = new FileOutputStream(w2rRelationFile);
        BufferedOutputStream bufferedOutput = new BufferedOutputStream(fileOutput);
        ObjectOutputStream objectOutput = new ObjectOutputStream(bufferedOutput)) {
      for (WayRelation nr : result.getWaysForRelations()) {
        objectOutput.writeObject(nr);
      }
      objectOutput.flush();
    }
  }


  
  
  
  

  private static Options buildCLIOptions() {
    final Options opts = new Options();

    opts.addOption(Option.builder("p") //
        .longOpt("pbf") //
        .argName("pbf") //
        .desc("pbf file to import") //
        // .numberOfArgs(Option.UNLIMITED_VALUES).hasArgs() //
        .hasArg().required() //
        .build());

    opts.addOption(Option.builder().longOpt("tmpDir") //
        .desc("Directory to store temporary files. DEFAULT ./") //
        .hasArg() //
        .required(false) //
        .build());

    return opts;
  }

  public static void main(String[] args)
      throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
    HOSMDbTransform.extract(args);
  }
}
