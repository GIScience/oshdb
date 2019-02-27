package org.heigit.bigspatialdata.updater;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.updater.util.cmd.FlushArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Flusher {

  private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

  /**
   * Flush updates form JDBC to real Ignite (best done once in a while, when database-usage is low).
   *
   * @param oshdb
   * @param updatedb
   * @param dbBit
   * @throws java.sql.SQLException
   * @throws java.io.IOException
   * @throws java.lang.ClassNotFoundException
   */
  public static void flush(Connection oshdb, Connection updatedb, Connection dbBit) throws
      SQLException, IOException, ClassNotFoundException {
    //do i need to block the cluster somehow, to prevent false operations?
    //ignite.cluster().active(false);
    //do flush here: can I use something from the etl?
    //ignite.cluster().active(true);
    Statement updateDBStatement = updatedb.createStatement();
    Statement oshdbStatement = oshdb.createStatement();
    for (OSMType t : OSMType.values()) {
      //nodes for testing only. later only exclude unkown
      if (t != OSMType.NODE) {
        continue;
      }
      updateDBStatement.execute("SELECT data FROM " + TableNames.forOSMType(t).get() + ";");
      ResultSet resultSetUpdate = updateDBStatement.getResultSet();
      while (resultSetUpdate.next()) {
        byte[] bytes = resultSetUpdate.getBytes("data");
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        OSHEntity updateEntity = (OSHEntity) ois.readObject();

        XYGridTree xyt = new XYGridTree(OSHDB.MAXZOOM);
        CellId insertId = xyt.getInsertId(updateEntity.getBoundingBox());
        System.out.println(insertId);

        oshdbStatement.execute(
            "SELECT data FROM " + TableNames.forOSMType(t).get() + " WHERE id=" + insertId.getId() + " and level=" + insertId
            .getZoomLevel() + ";"
        );
        ResultSet resultSetOSHDB = oshdbStatement.getResultSet();
        final boolean[] contains = {false};
        GridOSHEntity gridCell = null;
        if (resultSetOSHDB.next()) {
          GridOSHEntity insertGrid = (GridOSHEntity) (new ObjectInputStream(resultSetOSHDB
              .getBinaryStream(1))).readObject();

          List<OSHDBBoundingBox> bboxs = new ArrayList<>(1);

          switch (t) {
            case NODE:
              List<OSHNode> nodes = new ArrayList<>();
              insertGrid.forEach((Object gridEntity) -> {
                if (((OSHNode) gridEntity).getId() == updateEntity.getId()) {
                  contains[0] = true;
                } else {
                  nodes.add((OSHNode) gridEntity);
                  if (bboxs.isEmpty()) {
                    bboxs.add(((OSHNode) gridEntity).getBoundingBox());
                  } else {
                    OSHDBBoundingBox bbox1 = bboxs.get(bboxs.size() - 1);
                    bbox1.add(((OSHNode) gridEntity).getBoundingBox());
                    bboxs.add(bbox1);
                  }
                }
              });
              nodes.add((OSHNode) updateEntity);
              OSHDBBoundingBox bbox1 = bboxs.get(bboxs.size() - 1);
              bbox1.add(updateEntity.getBoundingBox());
              bboxs.add(bbox1);
              gridCell = GridOSHNodes.rebase(
                  insertId.getId(),
                  insertId.getZoomLevel(),
                  nodes.get(0).getId(),
                  nodes.get(0).getVersions().get(0).getTimestamp().getRawUnixTimestamp(),
                  XYGrid.getBoundingBox(insertId).getMinLonLong() + (XYGrid.getBoundingBox(insertId)
                  .getMaxLonLong() - XYGrid.getBoundingBox(insertId).getMinLonLong()) / 2,
                  XYGrid.getBoundingBox(insertId).getMinLatLong() + (XYGrid.getBoundingBox(insertId)
                  .getMaxLatLong() - XYGrid.getBoundingBox(insertId).getMinLatLong()) / 2,
                  nodes);
              break;
            case WAY:
              break;
            case RELATION:
              break;
            default:
              throw new AssertionError(t.name());
          }
        } else {
          //this object is the first one in its cell, so create a new one
          gridCell = GridOSHNodes.rebase(
              insertId.getId(),
              insertId.getZoomLevel(),
              updateEntity.getId(),
              ((OSMEntity) updateEntity.getVersions().get(0)).getTimestamp().getRawUnixTimestamp(),
              XYGrid.getBoundingBox(insertId).getMinLonLong() + (XYGrid.getBoundingBox(insertId)
              .getMaxLonLong() - XYGrid.getBoundingBox(insertId).getMinLonLong()) / 2,
              XYGrid.getBoundingBox(insertId).getMinLatLong() + (XYGrid.getBoundingBox(insertId)
              .getMaxLatLong() - XYGrid.getBoundingBox(insertId).getMinLatLong()) / 2,
              Arrays.asList((OSHNode) updateEntity));
          if (updateEntity.getVersions().size() < 2) {
            contains[0] = true;
          } else {
            contains[0] = false;
          }
        }
        //insert new grid here, support other dbs later (now only h2)
        PreparedStatement oshdbPreparedStatement = oshdb.prepareStatement(
            "MERGE INTO " + TableNames.forOSMType(t).get() + " "
            + "KEY(level,id) "
            + "VALUES ("
            + insertId.getZoomLevel() + ","
            + insertId.getId() + ","
            + "?);"
        );
        //TODO: a lot of resource handling should be done here and above
        try (
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);) {
          oos.writeObject(gridCell);
          oshdbPreparedStatement.setBytes(1, baos.toByteArray());
          oshdbPreparedStatement.execute();
        }
        if (!contains[0]) {
          if (updateEntity.getVersions().size() > 1) {
            //update old cell
            //get insertID of earlier stages
          }
        }
        //update bitmap
        //remove from updatedb
      }
    }
  }

  public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
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

    try (Connection updateDb = DriverManager.getConnection(config.baseArgs.jdbc);
        Connection dbBit = DriverManager.getConnection(config.baseArgs.dbbit);
        Connection oshdb = DriverManager.getConnection(config.dbconfig, "sa", "");) {
      Flusher.flush(oshdb, updateDb, dbBit);
    }
  }

}
