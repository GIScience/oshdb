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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
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
import org.heigit.bigspatialdata.updater.OSCHandling.OSCDownloader;
import org.heigit.bigspatialdata.updater.util.cmd.FlushArgs;
import org.heigit.bigspatialdata.updater.util.dbhandler.DatabaseHandler;
import org.openstreetmap.osmosis.core.util.FileBasedLock;
import org.openstreetmap.osmosis.core.util.PropertiesPersister;
import org.openstreetmap.osmosis.replication.common.ReplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Flusher {
  
  private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

  /**
   * Flush updates form JDBC to real Ignite. Aka.merge, aka.commit. This Class does not ensure
   * concurrency and data sefety. Entities might be doubled ore missing for a short period of time.
   * Database has to be locked by user.
   *
   * @param oshdb
   * @param updatedb
   * @param dbBit
   * @param etlPath
   * @param batchSize
   * @param updateMeta
   * @throws java.sql.SQLException
   * @throws java.io.IOException
   * @throws java.lang.ClassNotFoundException
   */
  public static void flush(
      OSHDBDatabase oshdb,
      Connection updatedb,
      Connection dbBit,
      Path etlPath,
      int batchSize,
      boolean updateMeta)
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

      //this could also be a bitmap
      Map<CellId, Set<Long>> filterEntities = new HashMap<>();
      Map<CellId, List<OSHEntity>> insertOrUpdateCells = new HashMap<>();
      int i = 0;
      while (resultSetUpdate.next()) {
        byte[] bytes = resultSetUpdate.getBytes("data");
        OSHEntity updateEntity = Flusher.createUpdateEntity(bytes, t);
        
        CellId currentCellId = etlf.getCurrentCellId(updateEntity.getType(), updateEntity.getId());
        CellId newCellId = xyt.getInsertId(updateEntity.getBoundingBox());
        
        if (currentCellId == null) {
          currentCellId = newCellId;
        } else if (newCellId.getZoomLevel() > currentCellId.getZoomLevel()) {
          newCellId = currentCellId;
        }
        
        insertOrUpdateCells
            .computeIfAbsent(newCellId, k -> new ArrayList<>())
            .add(updateEntity);

        //could be tested if actually needs to be removed
        filterEntities
            .computeIfAbsent(newCellId, k -> new HashSet<>())
            .add(updateEntity.getId());

        //could be tested if actually needs to be removed
        filterEntities
            .computeIfAbsent(currentCellId, k -> new HashSet<>())
            .add(updateEntity.getId());
        
        i++;
        if (i >= batchSize) {
          Flusher.runBatch(insertOrUpdateCells, filterEntities, oshdb, t);
          insertOrUpdateCells.clear();
          filterEntities.clear();
          i = 0;
        }
      }
      Flusher.runBatch(insertOrUpdateCells, filterEntities, oshdb, t);
    }
    DatabaseHandler.ereaseDb(updatedb, dbBit);
    if (updateMeta) {
      PropertiesPersister propertiesPersister = new PropertiesPersister(Updater.wd
          .resolve(
              OSCDownloader.LOCAL_STATE_FILE).toFile());
      DatabaseHandler.updateOSHDBMetadata(oshdb, new ReplicationState(propertiesPersister
          .loadMap()));
    }
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
            Flusher
                .flush(oshdb, updateDb, dbBit, config.baseArgs.etl, config.baseArgs.batchSize,
                    config.updateMeta);
          }
        } else if (config.dbconfig.contains("ignite")) {
          try (OSHDBIgnite oshdb = new OSHDBIgnite(config.dbconfig);) {
            Flusher
                .flush(oshdb, updateDb, dbBit, config.baseArgs.etl, config.baseArgs.batchSize,
                    config.updateMeta);
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
      Map<CellId, List<OSHEntity>> insertOrUpdateCells,
      Map<CellId, Set<Long>> removeCells,
      OSHDBDatabase oshdb,
      OSMType t
  ) throws SQLException, IOException, ClassNotFoundException {
    //at any stage duplicates and missing data are possible
    for (Entry<CellId, List<OSHEntity>> insertCellEntities : insertOrUpdateCells.entrySet()) {
      
      CellId currentCellId = insertCellEntities.getKey();
      List<OSHEntity> updateEntities = insertCellEntities.getValue();
      Set<Long> removeEntities = removeCells.remove(currentCellId);
      GridOSHEntity outdatedGridCell = Flusher.getSpecificGridCell(oshdb, t, currentCellId);
      
      GridOSHEntity updatedGridCell = Flusher.updateGridCell(currentCellId, outdatedGridCell,
          updateEntities, removeEntities, t);
      //this method might also be one day provided by ETL to make a load balancing but for now not possible
      //this may cause some fragmentation on the cluster (creating small cells with only one object)
      //insert entity
      Flusher.writeUpdatedGridCell(t, oshdb, updatedGridCell);
    }
    
    for (Entry<CellId, Set<Long>> removeCellEntities : removeCells.entrySet()) {
      CellId currentCellId = removeCellEntities.getKey();
      List<OSHEntity> updateEntities = new ArrayList<>();
      Set<Long> removeEntities = removeCellEntities.getValue();
      GridOSHEntity outdatedGridCell = Flusher.getSpecificGridCell(oshdb, t, currentCellId);
      GridOSHEntity updatedGridCell = Flusher.updateGridCell(currentCellId, outdatedGridCell,
          updateEntities, removeEntities, t);
      Flusher.writeUpdatedGridCell(t, oshdb, updatedGridCell);
    }
    
  }
  
  private static GridOSHEntity updateGridCell(
      CellId currentCellId,
      GridOSHEntity outdatedGridCell,
      List<? extends OSHEntity> updateEntities,
      Set<Long> removeEntities,
      OSMType t)
      throws SQLException, IOException, ClassNotFoundException {
    
    List<? extends OSHEntity> filteredEntities = new ArrayList<>();
    //check if grid cell does not yet exist (e.g. insertion)
    if (outdatedGridCell != null) {
      LOG.info("Creating new GridCell " + currentCellId.toString());
      filteredEntities = StreamSupport.stream(
          outdatedGridCell.getEntities().spliterator(), true)
          .filter(theEnt -> removeEntities.contains(theEnt.getId()))
          .collect(Collectors.toList());
    }
    
    GridOSHEntity updatedGridCell;
    switch (t) {
      case NODE:
        List<OSHNode> nodes = (List<OSHNode>) filteredEntities;
        
        nodes.addAll((List<OSHNode>) updateEntities);
        nodes.sort((enta, entb) -> Long.compare(enta.getId(), entb.getId()));
        
        updatedGridCell = GridOSHNodes.rebase(
            currentCellId.getLevelId(),
            currentCellId.getZoomLevel(),
            0,
            0,
            Flusher.getBaseLon(currentCellId.getZoomLevel(), currentCellId.getLevelId()),
            Flusher.getBaseLat(currentCellId.getZoomLevel(), currentCellId.getLevelId()),
            nodes);
        break;
      case WAY:
        List<OSHWay> ways = (List<OSHWay>) filteredEntities;
        
        ways.addAll((List<OSHWay>) updateEntities);
        ways.sort((enta, entb) -> Long.compare(enta.getId(), entb.getId()));
        
        updatedGridCell = GridOSHWays.compact(
            currentCellId.getLevelId(),
            currentCellId.getZoomLevel(),
            0,
            0,
            Flusher.getBaseLon(currentCellId.getZoomLevel(), currentCellId.getLevelId()),
            Flusher.getBaseLat(currentCellId.getZoomLevel(), currentCellId.getLevelId()),
            ways);
        break;
      case RELATION:
        List<OSHRelation> relations = (List<OSHRelation>) filteredEntities;
        
        relations.addAll((List<OSHRelation>) updateEntities);
        relations.sort((enta, entb) -> Long.compare(enta.getId(), entb.getId()));
        
        updatedGridCell = GridOSHRelations.compact(
            currentCellId.getLevelId(),
            currentCellId.getZoomLevel(),
            0,
            0,
            Flusher.getBaseLon(currentCellId.getZoomLevel(), currentCellId.getLevelId()),
            Flusher.getBaseLat(currentCellId.getZoomLevel(), currentCellId.getLevelId()),
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
