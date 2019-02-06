package org.heigit.bigspatialdata.updater.OSHUpdating;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.Producer;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.slf4j.LoggerFactory;

public class OSHLoader {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(OSHLoader.class);

  /**
   * represents the Load-Step in an ETL-Pipeline of updates.
   */
  public static void load(Connection conn, ArrayList<OSHEntity> oshEntityList) {
    OSHLoader.load(conn, oshEntityList, null);
  }

  /**
   * represents the Load-Step in an ETL-Pipeline of updates.
   *
   * @param updateDb
   * @param oshEntities
   * @param producer
   */
  public static void load(Connection updateDb, Iterable<OSHEntity> oshEntities, Producer<String, Stream<Byte[]>> producer) {
    LOG.info("loading");
    oshEntities.forEach((OSHEntity oshEntity) -> {
      //TODO: remove this:!
      if (oshEntity == null) {
        return;
      }
      try {
        try (PreparedStatement st = updateDb.prepareStatement("INSERT INTO update (id) VALUES (?)")) {
          System.out.println(oshEntity.getId());
          st.setLong(1, oshEntity.getId());
          st.executeUpdate();
        }
      } catch (SQLException ex) {
        LOG.error("error in SQL", ex);
      }
    });
    if (producer != null) {
      OSHLoader.promote(producer, oshEntities);
    }

  }

  public static void promote(Producer<String, Stream<Byte[]>> producer, Iterable<OSHEntity> oshEntities) {

  }

}
