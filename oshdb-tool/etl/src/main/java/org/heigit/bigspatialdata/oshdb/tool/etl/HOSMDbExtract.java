package org.heigit.bigspatialdata.oshdb.tool.etl;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import org.heigit.bigspatialdata.oshdb.tool.etl.cmdarg.ExtractArgs;
import org.heigit.bigspatialdata.oshdb.tool.etl.extract.ExtractMapper;
import org.heigit.bigspatialdata.oshdb.tool.etl.extract.ExtractMapperResult;
import org.heigit.bigspatialdata.oshdb.tool.etl.extract.data.KeyValuesFrequency;
import org.heigit.bigspatialdata.oshdb.api.utils.dbaccess.TableNames;
import org.heigit.bigspatialdata.oshpbf.HeaderInfo;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfEntity.Type;
import org.heigit.bigspatialdata.oshpbf.osm.OSMPbfUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HOSMDbExtract {

  private static final Logger LOG = LoggerFactory.getLogger(HOSMDbExtract.class);

  /**
   * Extract HOSM-Data from pbf to H2.
   *
   * @param pbfFile to extract data from
   * @param keytables database to store key mapping
   * @param tmpDir temporary directory
   * @throws FileNotFoundException
   * @throws IOException
   * @throws SQLException
   * @throws ClassNotFoundException
   */
  public static void extract(File pbfFile, Connection keytables, Path tmpDir)
      throws FileNotFoundException, IOException, SQLException, ClassNotFoundException {

    Class.forName("org.h2.Driver");

    HOSMDbExtract hosmDbExtract = new HOSMDbExtract(tmpDir, keytables);

    try (//
        final FileInputStream in = new FileInputStream(pbfFile) //
    ) {
      // define parts of file so it can be devided between threads
      ExtractMapper mapper = hosmDbExtract.createMapper();
      ExtractMapperResult mapResult = mapper.map(in);

      // create a h2 containing a collection of all keys and they corresponding values in this
      // dataset
      hosmDbExtract.storeKeyTables(mapResult);
      // create a temp_h2 containing storing all nodes, ways and relations that will are part of
      // other ways or relations
      hosmDbExtract.storeRelationMapping(mapResult);
      // collect and temp_write some baisc statistics about the imported data
      hosmDbExtract.storePBFMetaData(mapResult);

      // friendly output
      System.out.printf("Found N/W/R(%d,%d,%d)\n", mapResult.getCountNodes(),
          mapResult.getCountWays(), mapResult.getCountRelations());

    }

  }

  public static void main(String[] args)
      throws SQLException, IOException, FileNotFoundException, ClassNotFoundException {
    Class.forName("org.h2.Driver");
    ExtractArgs eargs = new ExtractArgs();
    JCommander jcom = JCommander.newBuilder().addObject(eargs).build();
    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      System.out.println("");
      LOG.error(e.getLocalizedMessage());
      System.out.println("");
      jcom.usage();

      return;
    }

    if (eargs.help.help) {
      jcom.usage();
      return;
    }

    final File pbfFile = eargs.pbfFile;
    final Path tmpDir = eargs.tempDir;

    try (Connection conn = DriverManager.getConnection("jdbc:h2:" + eargs.keytables, "sa", "")) {

      HOSMDbExtract.extract(pbfFile, conn, tmpDir);
    }
  }

  private final Path tmpDir;
  private final Connection conn;

  private HOSMDbExtract(final Path tmpDir, Connection conn) {
    this.tmpDir = tmpDir;
    this.conn = conn;
  }

  public ExtractMapper createMapper() {
    return new ExtractMapper();
  }

  private void storePBFMetaData(ExtractMapperResult mapResult) throws SQLException, IOException {
    Properties props = new Properties();
    props.put("countNodes", "" + mapResult.getCountNodes());
    props.put("countWay", "" + mapResult.getCountWays());
    props.put("countRelations", "" + mapResult.getCountRelations());
    props.put("startPosNode",
        "" + mapResult.getPbfTypeBlockFirstPosition().get(Type.NODE).longValue());
    props.put("startPosWay",
        "" + mapResult.getPbfTypeBlockFirstPosition().get(Type.WAY).longValue());
    props.put("startPosRelation",
        "" + mapResult.getPbfTypeBlockFirstPosition().get(Type.RELATION).longValue());

    HeaderInfo info = mapResult.getHeaderInfo();

    try (FileOutputStream out = new FileOutputStream(this.tmpDir + "/temp_meta.properties")) {
      props.store(out, String.format("created at %1$tF %1$tT", new Date()));
      out.flush();
    }

  }

  private void storeRelationMapping(ExtractMapperResult mapResult) throws SQLException {
    try (Connection tmprelconn = DriverManager.getConnection(
        "jdbc:h2:"
            + (new File(tmpDir.toFile(), EtlFiles.E_TEMPRELATIONS.getName())).getAbsolutePath(),
        "sa", "")) {
      Statement stmt = tmprelconn.createStatement();
      stmt.executeUpdate("drop table if exists " + TableNames.E_NODE2WAY.toString()
          + "; create table if not exists " + TableNames.E_NODE2WAY.toString()
          + "(node bigint primary key, ways array)");
      try (PreparedStatement insert = tmprelconn.prepareStatement(
          "insert into " + TableNames.E_NODE2WAY.toString() + " (node,ways) values(?,?)")) {
        for (Map.Entry<Long, SortedSet<Long>> relation : mapResult.getMapping().nodeToWay()
            .entrySet()) {
          insert.setLong(1, relation.getKey());
          insert.setObject(2, relation.getValue().toArray(new Long[0]));
          insert.addBatch();
        }
        insert.executeBatch();
      }

      stmt.executeUpdate("drop table if exists " + TableNames.E_NODE2RELATION.toString()
          + "; create table if not exists " + TableNames.E_NODE2RELATION.toString()
          + "(node bigint primary key, relations array)");
      try (PreparedStatement insert = tmprelconn.prepareStatement("insert into "
          + TableNames.E_NODE2RELATION.toString() + " (node,relations) values(?,?)")) {
        for (Map.Entry<Long, SortedSet<Long>> relation : mapResult.getMapping().nodeToRelation()
            .entrySet()) {
          insert.setLong(1, relation.getKey());
          insert.setObject(2, relation.getValue().toArray(new Long[0]));
          insert.addBatch();
        }
        insert.executeBatch();
      }

      stmt.executeUpdate("drop table if exists " + TableNames.E_WAY2RELATION.toString()
          + "; create table if not exists " + TableNames.E_WAY2RELATION.toString()
          + "(way bigint primary key, relations array)");
      try (PreparedStatement insert = tmprelconn.prepareStatement(
          "insert into " + TableNames.E_WAY2RELATION.toString() + " (way,relations) values(?,?)")) {
        for (Map.Entry<Long, SortedSet<Long>> relation : mapResult.getMapping().wayToRelation()
            .entrySet()) {
          insert.setLong(1, relation.getKey());
          insert.setObject(2, relation.getValue().toArray(new Long[0]));
          insert.addBatch();
        }
        insert.executeBatch();
      }

      stmt.executeUpdate("drop table if exists " + TableNames.E_RELATION2RELATION.toString()
          + "; create table if not exists " + TableNames.E_RELATION2RELATION.toString()
          + "(relation bigint primary key, relations array)");
      try (PreparedStatement insert = tmprelconn.prepareStatement("insert into "
          + TableNames.E_RELATION2RELATION.toString() + " (relation,relations) values(?,?)")) {
        for (Map.Entry<Long, SortedSet<Long>> relation : mapResult.getMapping().relationToRelation()
            .entrySet()) {
          insert.setLong(1, relation.getKey());
          insert.setObject(2, relation.getValue().toArray(new Long[0]));
          insert.addBatch();
        }
        insert.executeBatch();
      }
    }

  }

  private void storeKeyTables(ExtractMapperResult mapResult) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.executeUpdate(
        "drop table if exists " + TableNames.E_KEY.toString() + "; create table if not exists "
            + TableNames.E_KEY.toString() + "(id int primary key, txt varchar)");
    stmt.executeUpdate("drop table if exists " + TableNames.E_KEYVALUE.toString()
        + "; create table if not exists " + TableNames.E_KEYVALUE.toString()
        + "(keyId int, valueId int, txt varchar, primary key (keyId,valueId))");
    try (//
        PreparedStatement insertKey = conn.prepareStatement(
            "insert into " + TableNames.E_KEY.toString() + " (id,txt) values (?,?)");
        PreparedStatement insertValue = conn.prepareStatement("insert into "
            + TableNames.E_KEYVALUE.toString() + " ( keyId, valueId, txt ) values(?,?,?)")) {

      Map<String, KeyValuesFrequency> keyValuesFrequency = mapResult.getTagToKeyValuesFrequency();

      List<Map.Entry<String, KeyValuesFrequency>> sortedKeys =
          new ArrayList<>(keyValuesFrequency.entrySet());
      Comparator<Map.Entry<String, KeyValuesFrequency>> keyFrequencComparator =
          (e1, e2) -> Integer.compare(e1.getValue().freq(), e2.getValue().freq());
      sortedKeys.sort(keyFrequencComparator.reversed());

      for (int i = 0; i < sortedKeys.size(); i++) {
        Map.Entry<String, KeyValuesFrequency> entry = sortedKeys.get(i);
        insertKey.setInt(1, i);
        insertKey.setString(2, entry.getKey());
        insertKey.addBatch();

        List<Map.Entry<String, Integer>> sortedValues =
            new ArrayList<>(entry.getValue().values().entrySet());
        Comparator<Map.Entry<String, Integer>> valueFrequencComparator =
            (e1, e2) -> Integer.compare(e1.getValue(), e2.getValue());
        sortedValues.sort(valueFrequencComparator.reversed());
        for (int j = 0; j < sortedValues.size(); j++) {
          insertValue.setInt(1, i);
          insertValue.setInt(2, j);
          insertValue.setString(3, sortedValues.get(j).getKey());
          insertValue.addBatch();
        }
        insertValue.executeBatch();
      }
      insertKey.executeBatch();
    }

    stmt.executeUpdate(
        "drop table if exists " + TableNames.E_USER.toString() + "; create table if not exists "
            + TableNames.E_USER.toString() + "(id int, name varchar, primary key (id,name))");
    try (PreparedStatement insertUser = conn.prepareStatement(
        "insert into " + TableNames.E_USER.toString() + " (id, name) values (?,?)")) {
      for (OSMPbfUser user : mapResult.getUniqueUser()) {
        insertUser.setInt(1, user.getId());
        insertUser.setString(2, user.getName());
        insertUser.addBatch();
      }
      insertUser.executeBatch();
    }
    stmt.executeUpdate(
        "drop table if exists " + TableNames.E_ROLE.toString() + "; create table if not exists "
            + TableNames.E_ROLE.toString() + "(id int primary key, txt varchar)");
    try (PreparedStatement insertRole = conn.prepareStatement(
        "insert into " + TableNames.E_ROLE.toString() + " (id,txt) values(?,?)")) {
      List<Map.Entry<String, Integer>> sortedRoles =
          new ArrayList<>(mapResult.getRoleToFrequency().entrySet());
      Comparator<Map.Entry<String, Integer>> roleFrequencComparator =
          (e1, e2) -> Integer.compare(e1.getValue(), e2.getValue());
      sortedRoles.sort(roleFrequencComparator.reversed());

      for (int i = 0; i < sortedRoles.size(); i++) {
        insertRole.setInt(1, i);
        insertRole.setString(2, sortedRoles.get(i).getKey());
        insertRole.addBatch();
      }
      insertRole.executeBatch();
    }

  }

}
