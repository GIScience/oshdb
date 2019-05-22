package org.heigit.bigspatialdata.updater;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBH2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHRelationImpl;
import org.heigit.bigspatialdata.oshdb.impl.osh.OSHWayImpl;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHRelation;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.etl.EtlFileStore;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.dbhandler.update.UpdateDatabaseHandler;
import org.heigit.bigspatialdata.updater.util.cmd.FlushArgs;
import org.openstreetmap.osmosis.core.util.FileBasedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Flusher {

  private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

  /**
   * Flush updates form JDBC to real Ignite (best done once in a while, when database-usage is
   * low).Aka. merge, aka. commit.
   *
   * @param oshdb
   * @param updatedb
   * @param dbBit
   * @param etlPath
   * @throws java.sql.SQLException
   * @throws java.io.IOException
   * @throws java.lang.ClassNotFoundException
   */
  public static void flush(OSHDBDatabase oshdb, Connection updatedb, Connection dbBit, Path etlPath)
      throws SQLException, IOException, ClassNotFoundException {
    EtlFileStore etlf = new EtlFileStore(etlPath);
    XYGridTree xyt = new XYGridTree(OSHDB.MAXZOOM);
    //do i need to block the cluster somehow, to prevent false operations?
    //ignite.cluster().active(false);
    //wait for all queries to finish
    //do flush here: can I use something from the etl?
    //ignite.cluster().active(true);
    Statement updateDBStatement = updatedb.createStatement();
    for (OSMType t : OSMType.values()) {
      if (t == OSMType.UNKNOWN) {
        continue;
      }

      //get alls updated entities
      updateDBStatement.execute(
          "SELECT id as id, data as data FROM "
          + TableNames.forOSMType(t).get()
          + ";");
      ResultSet resultSetUpdate = updateDBStatement.getResultSet();

      while (resultSetUpdate.next()) {
        byte[] bytes = resultSetUpdate.getBytes("data");
        OSHEntity updateEntity = Flusher.createUpdateEntity(bytes, t);

        CellId currentCellId = etlf.getCurrentCellId(updateEntity.getType(), updateEntity.getId());
        CellId newCellId = xyt.getInsertId(updateEntity.getBoundingBox());

        if (currentCellId != null) {
          GridOSHEntity outdatedGridCell = Flusher.getSpecificGridCell(oshdb, t, currentCellId);

          //new Version is in lower zoomlevel, must be promoted
          if (newCellId.getZoomLevel() < currentCellId.getZoomLevel()) {
            GridOSHEntity croppedGridCell = Flusher
                .updateGridCell(outdatedGridCell, updateEntity, true, false);
            Flusher.writeUpdatedGridCell(updateEntity.getType(), oshdb, croppedGridCell);

            outdatedGridCell = Flusher.getSpecificGridCell(oshdb, t, newCellId);
            GridOSHEntity insertedGrid = Flusher
                .updateGridCell(outdatedGridCell, updateEntity, false, true);
            Flusher.writeUpdatedGridCell(updateEntity.getType(), oshdb, insertedGrid);

          } else {

            GridOSHEntity updatedGrid = Flusher
                .updateGridCell(outdatedGridCell, updateEntity, true, true);
            Flusher.writeUpdatedGridCell(updateEntity.getType(), oshdb, updatedGrid);
          }
        } else {
          //entity not yet in oshdb
          //this method might also be one day provided by ETL to make a load balancing but for now not possible
          //this may cause some fragmentation on the cluster (creating small cells with only one object)
          GridOSHEntity createdGrid = Flusher.createGridCell(newCellId, updateEntity);
          Flusher.writeUpdatedGridCell(updateEntity.getType(), oshdb, createdGrid);
        }
      }
    }
    UpdateDatabaseHandler.ereaseDb(updatedb, dbBit);
  }

  public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException,
      Exception {
    FlushArgs config = new FlushArgs();
    JCommander jcom = JCommander.newBuilder().addObject(config).build();
    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      LOG.error("There were errors with the given arguments! See below for more information!", e);
      jcom.usage();
      return;
    }
    if (config.baseArgs.help) {
      jcom.usage();
      return;
    }
    if (config.baseArgs.dbbit == null) {
      config.baseArgs.dbbit = config.baseArgs.jdbc;
    }

    Path wd = Paths.get("target/updaterWD/");
    wd.toFile().mkdirs();

    try (FileBasedLock fileLock = new FileBasedLock(
        wd.resolve(Updater.LOCK_FILE).toFile())) {
      try (Connection updateDb = DriverManager.getConnection(config.baseArgs.jdbc);
          Connection dbBit = DriverManager.getConnection(config.baseArgs.dbbit);) {
        if (config.dbconfig.contains("h2")) {
          try (Connection conn = DriverManager.getConnection(config.dbconfig, "sa", "");
              OSHDBH2 oshdb = new OSHDBH2(conn);) {
            Flusher.flush(oshdb, updateDb, dbBit, config.baseArgs.etl);
          }
        } else {
          try (OSHDBIgnite oshdb = new OSHDBIgnite(config.dbconfig);) {
            Flusher.flush(oshdb, updateDb, dbBit, config.baseArgs.etl);
          }
        }
      }
    }
  }

  private static GridOSHEntity createGridCell(CellId insertId, OSHEntity updateEntity) throws
      IOException {
//this object is the first one in its cell, so create a new one
    GridOSHEntity updatedGridCell;
    switch (updateEntity.getType()) {
      case NODE:
        List<OSHNode> nodes = new ArrayList<>(1);
        nodes.add((OSHNode) updateEntity);
        updatedGridCell = GridOSHNodes.rebase(
            insertId.getId(),
            insertId.getZoomLevel(),
            updateEntity.getId(),
            updateEntity.getVersions().iterator().next().getTimestamp().getRawUnixTimestamp(),
            Flusher.getBaseLon(insertId.getZoomLevel(), insertId.getId()),
            Flusher.getBaseLat(insertId.getZoomLevel(), insertId.getId()),
            nodes);
        break;
      case WAY:
        List<OSHWay> ways = new ArrayList<>(1);
        ways.add((OSHWay) updateEntity);
        ways.sort((way1, way2) -> Long.compare(way1.getId(), way2.getId()));
        updatedGridCell = GridOSHWays.compact(
            insertId.getId(),
            insertId.getZoomLevel(),
            updateEntity.getId(),
            updateEntity.getVersions().iterator().next().getTimestamp().getRawUnixTimestamp(),
            Flusher.getBaseLon(insertId.getZoomLevel(), insertId.getId()),
            Flusher.getBaseLat(insertId.getZoomLevel(), insertId.getId()),
            ways);
        break;
      case RELATION:
        List<OSHRelation> relations = new ArrayList<>(1);
        relations.add((OSHRelation) updateEntity);
        relations.sort((relation1, relation2) -> Long
            .compare(relation1.getId(), relation2.getId()));
        updatedGridCell = GridOSHRelations.compact(
            insertId.getId(),
            insertId.getZoomLevel(),
            updateEntity.getId(),
            updateEntity.getVersions().iterator().next().getTimestamp().getRawUnixTimestamp(),
            Flusher.getBaseLon(insertId.getZoomLevel(), insertId.getId()),
            Flusher.getBaseLat(insertId.getZoomLevel(), insertId.getId()),
            relations);
        break;
      default:
        throw new AssertionError(updateEntity.getType().name());
    }
    return updatedGridCell;
  }

  private static OSHEntity createUpdateEntity(byte[] bytes, OSMType t) throws IOException {
    switch (t) {
      case NODE:
        return OSHNodeImpl.instance(bytes, 0, bytes.length);
      case WAY:
        return OSHWayImpl.instance(bytes, 0, bytes.length);
      case RELATION:
        return OSHRelationImpl.instance(bytes, 0, bytes.length);
      default:
        throw new AssertionError(t.name());
    }
  }

  private static long getBaseLat(int level, long id) {
    CellId insertId = new CellId(level, id);
    return XYGrid.getBoundingBox(insertId).getMinLatLong()
        + (XYGrid.getBoundingBox(insertId).getMaxLatLong()
        - XYGrid.getBoundingBox(insertId).getMinLatLong())
        / 2;
  }

  private static long getBaseLon(int level, long id) {
    CellId insertId = new CellId(level, id);
    return XYGrid.getBoundingBox(insertId).getMinLonLong()
        + (XYGrid.getBoundingBox(insertId).getMaxLonLong()
        - XYGrid.getBoundingBox(insertId).getMinLonLong())
        / 2;
  }

  private static GridOSHEntity getSpecificGridCell(OSHDBDatabase oshdb, OSMType t, CellId insertId)
      throws SQLException, IOException, ClassNotFoundException {
    if (oshdb instanceof OSHDBH2) {
      Statement oshdbStatement = ((OSHDBH2) oshdb).getConnection().createStatement();
      oshdbStatement.execute(
          "SELECT data FROM "
          + oshdb.prefix()
          + TableNames.forOSMType(t).get()
          + " WHERE id="
          + insertId.getId()
          + " and level="
          + insertId.getZoomLevel()
          + ";"
      );
      ResultSet resultSetOSHDB = oshdbStatement.getResultSet();
      if (resultSetOSHDB.next()) {
        return (GridOSHEntity) (new ObjectInputStream(resultSetOSHDB.getBinaryStream(1)))
            .readObject();
      }
      return null;
    } else {
      IgniteCache<Long, GridOSHEntity> cache = ((OSHDBIgnite) oshdb)
          .getIgnite()
          .cache(oshdb.prefix() + TableNames.forOSMType(t));
      return cache.get(insertId.getLevelId());
    }

  }

  private static GridOSHEntity updateGridCell(
      GridOSHEntity outdatedGridCell,
      OSHEntity updateEntity,
      boolean remove,
      boolean insert)
      throws SQLException, IOException, ClassNotFoundException {

    //if(remove && insert) -> update
    if (!(remove && insert)) {
      throw new AssertionError("Why did you come here?");
    }

    GridOSHEntity updatedGridCell;
    boolean contains = false;
    switch (updateEntity.getType()) {
      case NODE:
        List<OSHNode> nodes = new ArrayList<>();
        if (remove) {
          for (OSHEntity gridEntity : outdatedGridCell.getEntities()) {
            OSHNode theNode = (OSHNode) gridEntity;
            if (theNode.getId() == updateEntity.getId()) {
              contains = true;
            } else {
              nodes.add(theNode);
            }
          }
        }
        if (insert) {
          nodes.add((OSHNode) updateEntity);
          nodes.sort((node1, node2) -> Long.compare(node1.getId(), node2.getId()));
        }
        updatedGridCell = GridOSHNodes.rebase(
            outdatedGridCell.getId(),
            outdatedGridCell.getLevel(),
            nodes.get(0).getId(),
            nodes.get(0).getVersions().iterator().next().getTimestamp().getRawUnixTimestamp(),
            Flusher.getBaseLon(outdatedGridCell.getLevel(), outdatedGridCell.getId()),
            Flusher.getBaseLat(outdatedGridCell.getLevel(), outdatedGridCell.getId()),
            nodes);
        break;
      case WAY:
        List<OSHWay> ways = new ArrayList<>();
        for (OSHEntity gridEntity : outdatedGridCell.getEntities()) {
          OSHWay theWay = (OSHWay) gridEntity;
          if (remove) {
            if (theWay.getId() == updateEntity.getId()) {
              contains = true;
            } else {
              ways.add(theWay);
            }
          }
        }
        if (insert) {
          ways.add((OSHWay) updateEntity);
          ways.sort((way1, way2) -> Long.compare(way1.getId(), way2.getId()));
        }
        updatedGridCell = GridOSHWays.compact(
            outdatedGridCell.getId(),
            outdatedGridCell.getLevel(),
            ways.get(0).getId(),
            ways.get(0).getVersions().iterator().next().getTimestamp().getRawUnixTimestamp(),
            Flusher.getBaseLon(outdatedGridCell.getLevel(), outdatedGridCell.getId()),
            Flusher.getBaseLat(outdatedGridCell.getLevel(), outdatedGridCell.getId()),
            ways);
        break;
      case RELATION:
        List<OSHRelation> relations = new ArrayList<>();
        if (remove) {
          for (OSHEntity gridEntity : outdatedGridCell.getEntities()) {
            OSHRelation theRelation = (OSHRelation) gridEntity;
            if (theRelation.getId() == updateEntity.getId()) {
              contains = true;
            } else {
              relations.add(theRelation);
            }
          }
        }
        if (insert) {
          relations.add((OSHRelation) updateEntity);
          relations.sort((relation1, relation2) -> Long
              .compare(relation1.getId(), relation2.getId()));
        }
        updatedGridCell = GridOSHRelations.compact(
            outdatedGridCell.getId(),
            outdatedGridCell.getLevel(),
            relations.get(0).getId(),
            relations.get(0).getVersions().iterator().next().getTimestamp().getRawUnixTimestamp(),
            Flusher.getBaseLon(outdatedGridCell.getLevel(), outdatedGridCell.getId()),
            Flusher.getBaseLat(outdatedGridCell.getLevel(), outdatedGridCell.getId()),
            relations);
        break;
      default:
        throw new AssertionError(updateEntity.getType().name());
    }

    if (remove && !contains) {
      LOG.debug(
          "The Entity "
          + updateEntity
          + " was previously not in the GridCell "
          + updatedGridCell);
    }

    return updatedGridCell;
  }

  private static void writeUpdatedGridCell(
      OSMType t,
      OSHDBDatabase oshdb,
      GridOSHEntity updatedGridCell)
      throws SQLException, IOException {

    if (oshdb instanceof OSHDBH2) {
      PreparedStatement oshdbPreparedStatement = ((OSHDBH2) oshdb).getConnection().prepareStatement(
          "MERGE INTO "
          + oshdb.prefix()
          + TableNames.forOSMType(t).get()
          + " KEY(level,id) "
          + "VALUES (?,?,?);"
      );
      try (
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream(baos);) {
        oos.writeObject(updatedGridCell);
        oshdbPreparedStatement.setInt(1, updatedGridCell.getLevel());
        oshdbPreparedStatement.setLong(2, updatedGridCell.getId());
        oshdbPreparedStatement.setBytes(3, baos.toByteArray());
        oshdbPreparedStatement.execute();
      }
    } else {
      IgniteCache<Long, GridOSHEntity> cache = ((OSHDBIgnite) oshdb)
          .getIgnite()
          .cache(oshdb.prefix() + TableNames.forOSMType(t));
      cache.put(updatedGridCell.getId(), updatedGridCell);
    }

  }

}
