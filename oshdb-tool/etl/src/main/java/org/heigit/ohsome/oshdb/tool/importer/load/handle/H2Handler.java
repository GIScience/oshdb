package org.heigit.ohsome.oshdb.tool.importer.load.handle;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
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
import org.heigit.ohsome.oshdb.grid.GridOSHNodes;
import org.heigit.ohsome.oshdb.grid.GridOSHRelations;
import org.heigit.ohsome.oshdb.grid.GridOSHWays;
import org.heigit.ohsome.oshdb.tool.importer.load.LoaderKeyTables;
import org.heigit.ohsome.oshdb.tool.importer.load.LoaderNode;
import org.heigit.ohsome.oshdb.tool.importer.load.LoaderRelation;
import org.heigit.ohsome.oshdb.tool.importer.load.LoaderWay;
import org.heigit.ohsome.oshdb.tool.importer.load.cli.DbH2Arg;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class H2Handler extends OSHDBHandler {

  private PreparedStatement insertKey;
  private PreparedStatement insertValue;
  private PreparedStatement insertRole;
  private PreparedStatement insertNode;
  private PreparedStatement insertWay;
  private PreparedStatement insertRelation;

  public H2Handler(Roaring64NavigableMap bitmapNodes, Roaring64NavigableMap bitmapWays,
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
      System.out.print("insert " + grid.getLevel() + ":" + grid.getId());
      insertNode.setInt(1, grid.getLevel());
      insertNode.setLong(2, grid.getId());
      insertNode.setBinaryStream(3, new FastByteArrayInputStream(out.array, 0, out.length));
      insertNode.executeUpdate();
      System.out.println(" done!");
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

  public static void load(DbH2Arg config) throws ClassNotFoundException {
    final Path workDirectory = config.common.workDir;
    Path oshdb = config.h2db;
    int maxZoomLevel = config.maxZoom;

    int minNodesPerGrid = config.minNodesPerGrid;
    int minWaysPerGrid = config.minWaysPerGrid;
    int minRelationPerGrid = config.minRelationPerGrid;

    boolean onlyNodesWithTags = config.onlyNodesWithTags;

    boolean withOutKeyTables = config.withOutKeyTables;


    Class.forName("org.h2.Driver");
    try (Connection conn =
        DriverManager.getConnection("jdbc:h2:" + oshdb.toString() + "", "sa", null)) {
      try (Statement stmt = conn.createStatement()) {


        try (
            BufferedReader br =
                new BufferedReader(new FileReader(workDirectory.resolve("extract_meta").toFile()));
            PreparedStatement insert = conn.prepareStatement(
                "insert into metadata (key,value) values (?,?)");) {
          stmt.executeUpdate("drop table if exists metadata"
              + "; create table if not exists metadata"
              + "(key varchar primary key, value varchar)");

          String line = null;
          while ((line = br.readLine()) != null) {
            if (line.trim().isEmpty()) {
              continue;
            }

            String[] split = line.split("=", 2);
            if (split.length != 2) {
              throw new RuntimeException("metadata file is corrupt");
            }

            insert.setString(1, split[0]);
            insert.setString(2, split[1]);
            insert.addBatch();
          }

          insert.setString(1, "attribution.short");
          insert.setString(2, config.attribution);
          insert.addBatch();
          insert.setString(1, "attribution.url");
          insert.setString(2, config.attributionUrl);
          insert.addBatch();

          insert.setString(1, "oshdb.maxzoom");
          insert.setString(2, "" + maxZoomLevel);
          insert.addBatch();

          insert.executeBatch();
        }

        stmt.executeUpdate("drop table if exists grid_node"
            + "; create table if not exists grid_node"
            + "(level int, id bigint, data blob,  primary key(level,id))");

        stmt.executeUpdate("drop table if exists grid_way"
            + "; create table if not exists grid_way"
            + "(level int, id bigint, data blob,  primary key(level,id))");

        stmt.executeUpdate("drop table if exists grid_relation"
            + "; create table if not exists grid_relation"
            + "(level int, id bigint, data blob,  primary key(level,id))");

        Roaring64NavigableMap bitmapWays = new Roaring64NavigableMap();
        try (
            FileInputStream fileIn = new FileInputStream(
                workDirectory.resolve("transform_wayWithRelation.bitmap").toFile());
            ObjectInputStream in = new ObjectInputStream(fileIn)) {
          bitmapWays.readExternal(in);
        }
        try (
            PreparedStatement insertNode = conn.prepareStatement(
                "insert into grid_node (level,id,data) values(?,?,?)");
            PreparedStatement insertWay = conn.prepareStatement(
                "insert into grid_way (level,id,data) values(?,?,?)");
            PreparedStatement insertRelation = conn.prepareStatement(
                "insert into grid_relation (level,id,data) values(?,?,?)")) {
          LoaderHandler handler;

          Stopwatch loadingWatch = Stopwatch.createUnstarted();
          if (!withOutKeyTables) {
            stmt.executeUpdate("drop table if exists key"
                + "; create table if not exists key "
                + "(id int primary key, txt varchar)");
            stmt.executeUpdate("drop table if exists keyvalue "
                + "; create table if not exists keyvalue"
                + "(keyId int, valueId int, txt varchar, primary key (keyId,valueId))");
            stmt.executeUpdate("drop table if exists role"
                + "; create table if not exists role"
                + "(id int primary key, txt varchar)");

            try (
                PreparedStatement insertKey = conn.prepareStatement(
                    "insert into key (id,txt) values (?,?)");
                PreparedStatement insertValue = conn.prepareStatement(
                    "insert into keyvalue ( keyId, valueId, txt ) values(?,?,?)");
                PreparedStatement insertRole = conn.prepareStatement(
                    "insert into role (id,txt) values(?,?)");) {

              handler = new H2Handler(Roaring64NavigableMap.bitmapOf(), bitmapWays, insertKey,
                  insertValue, insertRole, insertNode, insertWay, insertRelation);

              LoaderKeyTables keyTables = new LoaderKeyTables(workDirectory, handler);
              System.out.print("loading tags ... ");
              loadingWatch.reset().start();
              keyTables.loadTags();
              System.out.println(" done! " + loadingWatch);
              System.out.print("loading roles ...");
              loadingWatch.reset().start();
              keyTables.loadRoles();
              System.out.println(" done! " + loadingWatch);
            }
          } else {
            handler = new H2Handler(Roaring64NavigableMap.bitmapOf(), bitmapWays, null, null, null,
                insertNode, insertWay, insertRelation);
          }

          try (
              LoaderNode node = new LoaderNode(workDirectory, handler, minNodesPerGrid,
                  onlyNodesWithTags, maxZoomLevel);
              LoaderWay way =
                  new LoaderWay(workDirectory, handler, minWaysPerGrid, node, maxZoomLevel);
              LoaderRelation rel = new LoaderRelation(workDirectory, handler, minRelationPerGrid,
                  node, way, maxZoomLevel);) {
            System.out.print("loading to grid ...");
            loadingWatch.reset().start();
            rel.load();
          }
          System.out.println(" done! " + loadingWatch);
        }
      }
    } catch (IOException | SQLException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

    DbH2Arg config = new DbH2Arg();
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
