package org.heigit.bigspatialdata.oshdb.tool.importer.load.handle;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.tool.importer.load.Loader;
import org.heigit.bigspatialdata.oshdb.tool.importer.load.LoaderKeyTables;
import org.heigit.bigspatialdata.oshdb.tool.importer.load.LoaderNode;
import org.heigit.bigspatialdata.oshdb.tool.importer.load.LoaderRelation;
import org.heigit.bigspatialdata.oshdb.tool.importer.load.LoaderWay;
import org.heigit.bigspatialdata.oshdb.tool.importer.load.cli.DBH2Arg;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class OSHDB2H2Handler extends OSHDbHandler {

  private PreparedStatement insertKey;
  private PreparedStatement insertValue;
  private PreparedStatement insertRole;
  private PreparedStatement insertNode;
  private PreparedStatement insertWay;
  private PreparedStatement insertRelation;

  public OSHDB2H2Handler(Roaring64NavigableMap bitmapNodes, Roaring64NavigableMap bitmapWays,
      PreparedStatement insertKey, PreparedStatement insertValue, PreparedStatement insertRole,
      PreparedStatement insertNode, PreparedStatement insertWay, PreparedStatement insertRelation) {
    super(bitmapNodes, bitmapWays);
    this.insertKey = insertKey;
    this.insertValue = insertValue;
    this.insertRole = insertRole;
    this.insertNode = insertNode;
    this.insertWay = insertWay;
    this.insertRelation = insertRelation;

  }

  @Override
  public void loadKeyValues(int keyId, String key, List<String> values) {
    try {
      insertKey.setInt(1, keyId);
      insertKey.setString(2, key);
      insertKey.executeUpdate();

      int valueId = 0;
      for (String value : values) {
        try {
          insertValue.setInt(1, keyId);
          insertValue.setInt(2, valueId);
          insertValue.setString(3, value);
          insertValue.addBatch();
          // insertValue.executeUpdate();
          valueId++;
        } catch (SQLException e) {
          System.err.printf("error %d:%s %d:%s%n", keyId, key, valueId, value);
          throw e;
        }
      }
      insertValue.executeBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void loadRole(int id, String role) {
    try {
      insertRole.setInt(1, id);
      insertRole.setString(2, role);
      insertRole.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  FastByteArrayOutputStream out = new FastByteArrayOutputStream(1024);

  @Override
  public void handleNodeGrid(GridOSHNodes grid) {
    // System.out.println("nod "+grid.getLevel()+":"+grid.getId());
    try {
      out.reset();
      try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
        oos.writeObject(grid);
        oos.flush();
      }
      FastByteArrayInputStream in = new FastByteArrayInputStream(out.array, 0, out.length);

      insertNode.setInt(1, grid.getLevel());
      insertNode.setLong(2, grid.getId());
      insertNode.setBinaryStream(3, in);
      insertNode.executeUpdate();

    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void handleWayGrid(GridOSHWays grid) {
    // System.out.println("way "+grid.getLevel()+":"+grid.getId());
    try {
      out.reset();
      try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
        oos.writeObject(grid);
        oos.flush();
      }
      FastByteArrayInputStream in = new FastByteArrayInputStream(out.array, 0, out.length);

      insertWay.setInt(1, grid.getLevel());
      insertWay.setLong(2, grid.getId());
      insertWay.setBinaryStream(3, in);
      insertWay.executeUpdate();

    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void handleRelationsGrid(GridOSHRelations grid) {
    // System.out.println("rel "+ grid.getLevel()+":"+grid.getId());
    try {
      out.reset();
      try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
        oos.writeObject(grid);
        oos.flush();
      }
      FastByteArrayInputStream in = new FastByteArrayInputStream(out.array, 0, out.length);

      insertRelation.setInt(1, grid.getLevel());
      insertRelation.setLong(2, grid.getId());
      insertRelation.setBinaryStream(3, in);
      insertRelation.executeUpdate();

    } catch (IOException | SQLException e) {
      throw new RuntimeException(e);
    }

  }
  
  public static void load(DBH2Arg config) throws ClassNotFoundException {
    final Path workDirectory = config.common.workDir;
    Path oshdb = config.h2db;
    int maxZoomLevel = config.maxZoom;

    int minNodesPerGrid = config.minNodesPerGrid;
    int minWaysPerGrid = config.minWaysPerGrid;
    int minRelationPerGrid = config.minRelationPerGrid;

    boolean onlyNodesWithTags = config.onlyNodesWithTags;

    boolean withKeyTables = config.withKeyTables;

    
    Class.forName("org.h2.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:h2:" + oshdb.toString(), "sa", "")) {
      try (Statement stmt = conn.createStatement()) {

        if (withKeyTables) {
          stmt.executeUpdate("drop table if exists " + TableNames.E_KEY.toString() + "; create table if not exists "
              + TableNames.E_KEY.toString() + "(id int primary key, txt varchar)");
          stmt.executeUpdate("drop table if exists " + TableNames.E_KEYVALUE.toString()
              + "; create table if not exists " + TableNames.E_KEYVALUE.toString()
              + "(keyId int, valueId int, txt varchar, primary key (keyId,valueId))");
          stmt.executeUpdate("drop table if exists " + TableNames.E_ROLE.toString() + "; create table if not exists "
              + TableNames.E_ROLE.toString() + "(id int primary key, txt varchar)");
        }

        PreparedStatement insertKey = conn
            .prepareStatement("insert into " + TableNames.E_KEY.toString() + " (id,txt) values (?,?)");
        PreparedStatement insertValue = conn.prepareStatement(
            "insert into " + TableNames.E_KEYVALUE.toString() + " ( keyId, valueId, txt ) values(?,?,?)");
        PreparedStatement insertRole = conn
            .prepareStatement("insert into " + TableNames.E_ROLE.toString() + " (id,txt) values(?,?)");

        stmt.executeUpdate("drop table if exists " + TableNames.T_NODES.toString() + "; create table if not exists "
            + TableNames.T_NODES.toString() + "(level int, id bigint, data blob,  primary key(level,id))");
        PreparedStatement insertNode = conn
            .prepareStatement("insert into " + TableNames.T_NODES.toString() + " (level,id,data) values(?,?,?)");

        stmt.executeUpdate("drop table if exists " + TableNames.T_WAYS.toString() + "; create table if not exists "
            + TableNames.T_WAYS.toString() + "(level int, id bigint, data blob,  primary key(level,id))");
        PreparedStatement insertWay = conn
            .prepareStatement("insert into " + TableNames.T_WAYS.toString() + " (level,id,data) values(?,?,?)");

        stmt.executeUpdate("drop table if exists " + TableNames.T_RELATIONS.toString() + "; create table if not exists "
            + TableNames.T_RELATIONS.toString() + "(level int, id bigint, data blob,  primary key(level,id))");
        PreparedStatement insertRelation = conn
            .prepareStatement("insert into " + TableNames.T_RELATIONS.toString() + " (level,id,data) values(?,?,?)");

        Roaring64NavigableMap bitmapWays = new Roaring64NavigableMap();
        try (FileInputStream fileIn = new FileInputStream(workDirectory.resolve("transform_wayWithRelation.bitmap").toFile());
            ObjectInputStream in = new ObjectInputStream(fileIn)) {
          bitmapWays.readExternal(in);
        }

        LoaderHandler handler = new OSHDB2H2Handler(Roaring64NavigableMap.bitmapOf(), bitmapWays, insertKey,
            insertValue, insertRole, insertNode, insertWay, insertRelation);
        Stopwatch loadingWatch = Stopwatch.createUnstarted();
        if (withKeyTables) {
          LoaderKeyTables keyTables = new LoaderKeyTables(workDirectory, handler);
          System.out.print("loading tags ... ");
          loadingWatch.reset().start();
          keyTables.loadTags();
          System.out.println(" done! "+loadingWatch);
          System.out.print("loading roles ...");
          loadingWatch.reset().start();
          keyTables.loadRoles();
          System.out.println(" done! "+loadingWatch);
        }

        Loader loader;
        LoaderNode node;
        loader = node = new LoaderNode(workDirectory, handler, minNodesPerGrid, onlyNodesWithTags, maxZoomLevel);
        LoaderWay way;
        loader = way = new LoaderWay(workDirectory, handler, minWaysPerGrid, node, maxZoomLevel);
        LoaderRelation rel;
        loader = rel = new LoaderRelation(workDirectory, handler, minRelationPerGrid, node, way, maxZoomLevel);

        System.out.print("loading to grid ...");
        loadingWatch.reset().start();
        loader.load();
        System.out.println(" done! "+loadingWatch);
      } 
    }catch (IOException | SQLException e) {
      e.printStackTrace();
    }

    
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

    DBH2Arg config = new DBH2Arg();
    JCommander jcom = JCommander.newBuilder().addObject(config).build();

    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      System.out.println("");
      System.out.println(e.getLocalizedMessage());
      System.out.println("");
      jcom.usage();
      return;
    }
    if (config.common.help) {
      jcom.usage();
      return;
    }

    final Stopwatch stopWatch = Stopwatch.createStarted();
    load(config);
    System.out.println("loading done in " + stopWatch);
 

  }

}
