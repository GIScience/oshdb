package org.heigit.bigspatialdata.updater;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Iterables;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
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
   * low).Aka.merge, aka. commit.
   *
   * @param oshdb
   * @param updatedb
   * @param dbBit
   * @param etlPath
   * @param batchSize
   * @throws java.sql.SQLException
   * @throws java.io.IOException
   * @throws java.lang.ClassNotFoundException
   */
  public static void flush(
      OSHDBDatabase oshdb,
      Connection updatedb,
      Connection dbBit,
      Path etlPath,
      int batchSize)
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

      Map<CellId, Map<CellId, List<OSHEntity>>> entities = new HashMap<>(3);
      int i = 0;
      while (resultSetUpdate.next()) {
        byte[] bytes = resultSetUpdate.getBytes("data");
        OSHEntity updateEntity = Flusher.createUpdateEntity(bytes, t);

        CellId currentCellId = etlf.getCurrentCellId(updateEntity.getType(), updateEntity.getId());
        CellId newCellId = xyt.getInsertId(updateEntity.getBoundingBox());

        if ((currentCellId != null) && !(newCellId.getZoomLevel() < currentCellId.getZoomLevel())) {
          newCellId = currentCellId;
        }

        Map<CellId, List<OSHEntity>> insertCellEntities = entities
            .getOrDefault(newCellId, new HashMap<>());
        List<OSHEntity> entitiesList = insertCellEntities
            .getOrDefault(currentCellId, new ArrayList<>());

        entitiesList.add(updateEntity);
        insertCellEntities.put(currentCellId, entitiesList);
        entities.put(newCellId, insertCellEntities);

        i++;

        if (i >= batchSize) {
          Flusher.runBatch(entities, oshdb, t);
          entities.clear();
          i = 0;
        }
      }
      Flusher.runBatch(entities, oshdb, t);
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
            Flusher.flush(oshdb, updateDb, dbBit, config.baseArgs.etl, config.baseArgs.batchSize);
          }
        } else if (config.dbconfig.contains("ignite")) {
          try (OSHDBIgnite oshdb = new OSHDBIgnite(config.dbconfig);) {
            Flusher.flush(oshdb, updateDb, dbBit, config.baseArgs.etl, config.baseArgs.batchSize);
          }
        } else {
          throw new AssertionError(
              "Backend of type " + config.dbconfig + " not supported yet.");
        }
      }
    }
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
    } else if (oshdb instanceof OSHDBIgnite) {
      IgniteCache<Long, GridOSHEntity> cache = ((OSHDBIgnite) oshdb)
          .getIgnite()
          .cache(oshdb.prefix() + TableNames.forOSMType(t));
      return cache.get(insertId.getLevelId());
    } else {
      throw new AssertionError(
          "Backend of type " + oshdb.getClass().getName() + " not supported yet.");
    }

  }

  private static void runBatch(
      Map<CellId, Map<CellId, List<OSHEntity>>> entities,
      OSHDBDatabase oshdb,
      OSMType t
  )
      throws SQLException, IOException, ClassNotFoundException {
    for (Entry<CellId, Map<CellId, List<OSHEntity>>> insertCellEntities : entities.entrySet()) {
      for (Entry<CellId, List<OSHEntity>> updateCellEntities
          : insertCellEntities.getValue().entrySet()) {

        List<OSHEntity> updateEntitys = updateCellEntities.getValue();
        CellId currentCellId = updateCellEntities.getKey();
        CellId newCellId = insertCellEntities.getKey();

        if (currentCellId != null) {
          GridOSHEntity outdatedGridCell = Flusher.getSpecificGridCell(oshdb, t, currentCellId);

          if (newCellId != currentCellId) {
            //new Version is in lower zoomlevel, must be promoted
            GridOSHEntity croppedGridCell = Flusher
                .updateGridCell(currentCellId, outdatedGridCell, updateEntitys, t, true, false);
            Flusher.writeUpdatedGridCell(t, oshdb, croppedGridCell);

            outdatedGridCell = Flusher.getSpecificGridCell(oshdb, t, newCellId);
            GridOSHEntity insertedGrid = Flusher
                .updateGridCell(newCellId, outdatedGridCell, updateEntitys, t, false, true);
            Flusher.writeUpdatedGridCell(t, oshdb, insertedGrid);

          } else {

            GridOSHEntity updatedGrid = Flusher
                .updateGridCell(newCellId, outdatedGridCell, updateEntitys, t, true, true);
            Flusher.writeUpdatedGridCell(t, oshdb, updatedGrid);
          }
        } else {
          //entity not yet in oshdb
          GridOSHEntity outdatedGridCell = Flusher.getSpecificGridCell(oshdb, t, newCellId);

          //this method might also be one day provided by ETL to make a load balancing but for now not possible
          //this may cause some fragmentation on the cluster (creating small cells with only one object)
          //insert entity
          GridOSHEntity insertedGrid = Flusher
              .updateGridCell(newCellId, outdatedGridCell, updateEntitys, t, false, true);
          Flusher.writeUpdatedGridCell(t, oshdb, insertedGrid);

        }
      }
    }
  }

  private static GridOSHEntity updateGridCell(
      CellId outdatedCellId,
      GridOSHEntity outdatedGridCell,
      List<OSHEntity> updateEntities,
      OSMType t,
      boolean remove,
      boolean insert)
      throws SQLException, IOException, ClassNotFoundException {

    //if(remove && insert) -> update
    if (!(remove || insert)) {
      throw new AssertionError("Why did you come here?");
    }

    GridOSHEntity updatedGridCell;
    List<OSHEntity> filteredEntities = null;
    if (remove) {
      Set<Long> ids = updateEntities
          .stream()
          .map(entity -> entity.getId())
          .collect(Collectors.toSet());
      filteredEntities = Lists.newArrayList(
          Iterables.filter(outdatedGridCell.getEntities(), theEnt -> ids.contains(theEnt.getId()))
              .iterator());
    }
    switch (t) {
      case NODE:
        List<OSHNode> nodes = new ArrayList<>();
        if (remove) {
          nodes.addAll((List<OSHNode>) (List<?>) filteredEntities);
        }

        if (insert) {
          nodes.addAll((List<OSHNode>) (List<?>) updateEntities);
          nodes.sort((node1, node2) -> Long.compare(node1.getId(), node2.getId()));
        }

        updatedGridCell = GridOSHNodes.rebase(
            outdatedCellId.getLevelId(),
            outdatedCellId.getZoomLevel(),
            nodes.get(0).getId(),
            nodes.get(0).getVersions().iterator().next().getTimestamp().getRawUnixTimestamp(),
            Flusher.getBaseLon(outdatedCellId.getZoomLevel(), outdatedCellId.getLevelId()),
            Flusher.getBaseLat(outdatedCellId.getZoomLevel(), outdatedCellId.getLevelId()),
            nodes);
        break;
      case WAY:
        List<OSHWay> ways = new ArrayList<>();
        if (remove) {
          ways.addAll((List<OSHWay>) (List<?>) filteredEntities);
        }

        if (insert) {
          ways.addAll((List<OSHWay>) (List<?>) updateEntities);
          ways.sort((node1, node2) -> Long.compare(node1.getId(), node2.getId()));
        }
        updatedGridCell = GridOSHWays.compact(
            outdatedCellId.getLevelId(),
            outdatedCellId.getZoomLevel(),
            ways.get(0).getId(),
            ways.get(0).getVersions().iterator().next().getTimestamp().getRawUnixTimestamp(),
            Flusher.getBaseLon(outdatedCellId.getZoomLevel(), outdatedCellId.getLevelId()),
            Flusher.getBaseLat(outdatedCellId.getZoomLevel(), outdatedCellId.getLevelId()),
            ways);
        break;
      case RELATION:
        List<OSHRelation> relations = new ArrayList<>();
        if (remove) {
          relations.addAll((List<OSHRelation>) (List<?>) filteredEntities);
        }

        if (insert) {
          relations.addAll((List<OSHRelation>) (List<?>) updateEntities);
          relations.sort((node1, node2) -> Long.compare(node1.getId(), node2.getId()));
        }
        updatedGridCell = GridOSHRelations.compact(
            outdatedCellId.getLevelId(),
            outdatedCellId.getZoomLevel(),
            relations.get(0).getId(),
            relations.get(0).getVersions().iterator().next().getTimestamp().getRawUnixTimestamp(),
            Flusher.getBaseLon(outdatedCellId.getZoomLevel(), outdatedCellId.getLevelId()),
            Flusher.getBaseLat(outdatedCellId.getZoomLevel(), outdatedCellId.getLevelId()),
            relations);
        break;
      default:
        throw new AssertionError(t.name());
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
    } else if (oshdb instanceof OSHDBIgnite) {
      IgniteCache<Long, GridOSHEntity> cache = ((OSHDBIgnite) oshdb)
          .getIgnite()
          .cache(oshdb.prefix() + TableNames.forOSMType(t));
      cache.put(updatedGridCell.getId(), updatedGridCell);
    } else {
      throw new AssertionError(
          "Backend of type " + oshdb.getClass().getName() + " not supported yet.");
    }

  }

}
