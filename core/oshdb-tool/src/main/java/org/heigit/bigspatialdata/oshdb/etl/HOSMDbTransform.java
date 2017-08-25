package org.heigit.bigspatialdata.oshdb.etl;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.etl.cmdarg.TransformArgs;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.CellInfo;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.CellNode;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.CellRelation;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.CellWay;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.NodeRelation;
import org.heigit.bigspatialdata.oshdb.etl.transform.data.WayRelation;
import org.heigit.bigspatialdata.oshdb.etl.transform.node.TransformNodeMapper;
import org.heigit.bigspatialdata.oshdb.etl.transform.node.TransformNodeMapper.Result;
import org.heigit.bigspatialdata.oshdb.etl.transform.relation.TransformRelationMapper;
import org.heigit.bigspatialdata.oshdb.etl.transform.way.TransformWayMapper;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;

/**
 * HOSMDbTransform
 *
 * inputs from HOSMDbExtract - KeyTables (KeyValues, Roles, User) - Relation
 * Mappings node2Way, node2Relation, way2Relation, relation2Relation
 *
 * output - transformed pbffile nodes with Strings replaced by references -
 * transformed pbffile ways with Strings replaced by references - transformed
 * pbffile relations with Strings replaced by references
 *
 *
 * @author Rafael Troilo <rafael.troilo@uni-heidelberg.de>
 */
public class HOSMDbTransform {

  private static final Logger LOG = Logger.getLogger(HOSMDbTransform.class.getName());

  /**
   * Transform the extracted Data to an OSH-DB.
   *
   * @param pbfFile pbf-File Data was extracted from
   * @param conn connection to the final BigDB that already holds the keytables
   * from the extract step
   * @param tempDir make sure the temp_relations.mv.db is present there!
   * @throws IOException
   * @throws FileNotFoundException
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  public static void transform(File pbfFile, Connection conn, Path tempDir) throws IOException, FileNotFoundException, SQLException, ClassNotFoundException {
    HOSMDbTransform.transform(pbfFile, conn, tempDir, conn);
  }

  /**
   * Transform the extracted Data to an OSH-DB.
   *
   * @param pbfFile pbf-File Data was extracted from
   * @param conn connection to the final OSHDb
   * @param tempDir make sure the temp_Relations.mv.db is present there!
   * @param keytables make sure this points to the DB the data was extracted to!
   * @throws FileNotFoundException
   * @throws IOException
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  public static void transform(File pbfFile, Connection conn, Path tempDir, Connection keytables)
          throws FileNotFoundException, IOException, SQLException, ClassNotFoundException {
    Class.forName("org.h2.Driver");

    HOSMDbTransform hosmDbTransform = new HOSMDbTransform(tempDir, conn);

    final int maxZoom = OSHDB.MAXZOOM;
    final String n2wRelationFile = hosmDbTransform.getTmp() + "/temp_nodesForWays.ser";
    final String n2rRelationFile = hosmDbTransform.getTmp() + "/temp_nodesForRelation.ser";
    final String w2rRelationFile = hosmDbTransform.getTmp() + "/temp_waysForRelation.ser";
    final XYGrid grid = new XYGrid(maxZoom);
    try (Connection tempRelations = DriverManager.getConnection("jdbc:h2:" + tempDir + "/temp_relations", "sa", "")) {
      if (true) {
        try (//
                final FileInputStream in = new FileInputStream(pbfFile); //
                ) {
          //define parts of file so it can be devided between threads
          TransformNodeMapper nodeMapper = new TransformNodeMapper(maxZoom, tempRelations, keytables);
          TransformNodeMapper.Result nodeResult = nodeMapper.map(in);

          if (true) {
            System.out.println("Saving Node Grid");
            //create grid cells containing all nodes in that cell:
            // header; node1v1, node1v2,...,noder2v1,...nodeNvN (deltaencaoded and serialized as you know it)
            //THIS is the MAGIC!!!
            hosmDbTransform.saveGrid(nodeResult, grid);

            //delete previous files
            Path pN2W = Paths.get(n2wRelationFile);
            if (pN2W.toFile().exists()) {
              Files.delete(pN2W);
            }
            Path pN2R = Paths.get(n2rRelationFile);
            if (pN2R.toFile().exists()) {
              Files.delete(pN2R);
            }
            System.out.println("Saving NodesForRelation");
            //temp_store serialized nodes so they can be read when creating a way or relation
            saveNodesForRelations(nodeResult, pN2W.toFile(), pN2R.toFile());
          }
        }
      }
      if (true) {
        try (//
                final FileInputStream in = new FileInputStream(pbfFile) //
                ) {

          System.out.println("Start Way Mapper");
          TransformWayMapper wayMapper = new TransformWayMapper(maxZoom, n2wRelationFile, tempRelations, keytables);
          TransformWayMapper.Result wayResults = wayMapper.map(in);
          System.out.println("Saving Way Grid");
          hosmDbTransform.saveGrid(wayResults);

          Path pW2R = Paths.get(w2rRelationFile);
          if (pW2R.toFile().exists()) {
            Files.delete(pW2R);
          }

          saveWaysForRelations(wayResults, pW2R.toFile());
        }
      }
      if (true) {
        try (//
                final FileInputStream in = new FileInputStream(pbfFile) //
                ) {
          System.out.println("Start Relation Mapper");
          TransformRelationMapper mapper = new TransformRelationMapper(maxZoom, n2rRelationFile, w2rRelationFile, tempRelations, keytables);
          TransformRelationMapper.Result result = mapper.map(in);
          System.out.println("Saving Relation Grid");
          hosmDbTransform.saveGrid(result);

        }

      }
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

  public static void main(String[] args)
          throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
    Class.forName("org.h2.Driver");
    TransformArgs targs = new TransformArgs();
    JCommander jcom = JCommander.newBuilder().addObject(targs).build();
    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      System.out.println("");
      LOG.log(Level.SEVERE, e.getLocalizedMessage());
      System.out.println("");
      jcom.usage();

      return;
    }

    if (targs.baseArgs.help.help) {
      jcom.usage();
      return;
    }

    final File pbfFile = targs.baseArgs.pbfFile;

    final Path tmpDir = targs.baseArgs.tempDir;
    try (Connection conn = DriverManager.getConnection("jdbc:h2:" + targs.oshdbarg.oshdb, "sa", "")) {
      if ((new File(targs.baseArgs.keytables.toString() + ".mv.db")).exists()) {
        try (Connection keytables = DriverManager.getConnection("jdbc:h2:" + targs.baseArgs.keytables, "sa", "")) {
          HOSMDbTransform.transform(pbfFile, conn, tmpDir, keytables);
        }
      } else {
        HOSMDbTransform.transform(pbfFile, conn, tmpDir);
      }
    }
  }
  private final Path tmpDir;
  private final Connection conn;

  private HOSMDbTransform(final Path tmpDir, Connection conn) {
    this.tmpDir = tmpDir;
    this.conn = conn;
  }

  protected Path getTmp() {
    return tmpDir;
  }

  private void saveGrid(Result result, XYGrid grid) {

    //get connection and set up h2-DB
    Connection dbconn = this.conn;
    try (Statement stmt = dbconn.createStatement()) {

      stmt.executeUpdate(
              "drop table if exists grid_node; create table if not exists grid_node(level int, id bigint, data blob,  primary key(level,id))");

      PreparedStatement insert
              = dbconn.prepareStatement("insert into grid_node (level,id,data) values(?,?,?)");

      //iterate over cells parsed from PBF-File
      for (CellNode cell : result.getNodeCells()) {
        //write zoomlevel and id to h2
        insert.setInt(1, cell.info().getZoomLevel());
        insert.setLong(2, cell.info().getId());

        CellInfo cellInfo = cell.info();
        MultiDimensionalNumericData cellDimensions = grid.getCellDimensions(cellInfo.getId());
        double[] minValues = cellDimensions.getMinValuesPerDimension();
        //create hosmCell of nodes from a "cellNode", a collection of nodes. First part is header, followed by cell.getNodes(), the actual nodes. At this point, delta encoding is already done within the HOSMNode
        GridOSHNodes hosmCell = GridOSHNodes.rebase(cellInfo.getId(), cellInfo.getZoomLevel(),
                cell.minId(), cell.minTimestamp(), (long) (minValues[0] / OSMNode.GEOM_PRECISION),
                (long) (minValues[1] / OSMNode.GEOM_PRECISION), cell.getNodes());

        //convert hosmCell of nodes to a byteArray of objects to the database
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

  private void saveGrid(TransformWayMapper.Result wayResults) {

    Connection dbconn = this.conn;
    try (Statement stmt = dbconn.createStatement()) {

      stmt.executeUpdate(
              "drop table if exists grid_way; create table if not exists grid_way(level int, id bigint, data blob,  primary key(level,id))");

      PreparedStatement insert
              = dbconn.prepareStatement("insert into grid_way (level,id,data) values(?,?,?)");

      for (CellWay cell : wayResults.getCells()) {
        insert.setInt(1, cell.info().getZoomLevel());
        insert.setLong(2, cell.info().getId());

        CellInfo cellInfo = cell.info();

        XYGrid grid = new XYGrid(cellInfo.getZoomLevel());
        MultiDimensionalNumericData cellDimensions = grid.getCellDimensions(cellInfo.getId());
        double[] minValues = cellDimensions.getMinValuesPerDimension();
        GridOSHWays hosmCell = GridOSHWays.compact(cellInfo.getId(), cellInfo.getZoomLevel(),
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

  private void saveGrid(TransformRelationMapper.Result result) {

    Connection dbconn = this.conn;
    try (Statement stmt = dbconn.createStatement()) {

      stmt.executeUpdate(
              "drop table if exists grid_relation; create table if not exists grid_relation(level int, id bigint, data blob,  primary key(level,id))");

      PreparedStatement insert
              = dbconn.prepareStatement("insert into grid_relation (level,id,data) values(?,?,?)");

      for (CellRelation cell : result.getCells()) {
        insert.setInt(1, cell.info().getZoomLevel());
        insert.setLong(2, cell.info().getId());

        CellInfo cellInfo = cell.info();

        XYGrid grid = new XYGrid(cellInfo.getZoomLevel());
        MultiDimensionalNumericData cellDimensions = grid.getCellDimensions(cellInfo.getId());
        double[] minValues = cellDimensions.getMinValuesPerDimension();
        GridOSHRelations hosmCell = GridOSHRelations.compact(cellInfo.getId(), cellInfo.getZoomLevel(),
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
}
