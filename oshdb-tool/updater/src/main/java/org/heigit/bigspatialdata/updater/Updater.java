package org.heigit.bigspatialdata.updater;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.updater.OSCHandling.OSCDownloader;
import org.heigit.bigspatialdata.updater.OSCHandling.OSCParser;
import org.heigit.bigspatialdata.updater.OSHUpdating.OSHLoader;
import org.heigit.bigspatialdata.updater.util.OSCOSHTransformer;
import org.heigit.bigspatialdata.updater.util.ReplicationFile;
import org.heigit.bigspatialdata.updater.util.UpdateArgs;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.core.util.FileBasedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update a running OSHDB with replication files from OSMPlanet server.
 */
public class Updater {
  private static final String LOCK_FILE = "update.lock";
  private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

  public static void main(String[] args) throws MalformedURLException, ClassNotFoundException, IgniteCheckedException, FileNotFoundException, IOException, SQLException {
    UpdateArgs config = new UpdateArgs();
    JCommander jcom = JCommander.newBuilder().addObject(config).build();
    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      LOG.error("There were errors with the given arguments! See below for more information!", e);
      jcom.usage();
      return;
    }
    if (config.help) {
      jcom.usage();
      return;
    }
    config.workDir.toFile().mkdirs();

    try (Connection updateDb = DriverManager.getConnection(config.jdbc);
        Connection keytables = DriverManager.getConnection(config.keytables, "sa", "");) {
      if (config.flush) {
        Connection oshdb = DriverManager.getConnection(config.dbconfig);
        Updater.flush(oshdb, updateDb);
      } else {
        if (config.kafka != null) {
          Properties props = new Properties();
          FileInputStream input = new FileInputStream(config.kafka);
          props.load(input);
          Producer<String, Stream<Byte[]>> producer = new KafkaProducer<>(props);
          Updater.update(config.etl, keytables, config.workDir, updateDb, config.baseURL, producer);
          producer.close();
        } else {
          Updater.update(config.etl, keytables, config.workDir, updateDb, config.baseURL);
        }

      }
    }
  }

  /**
   * Downloads replication files, transforms them to OSHDB-Objects and stores them in a
   * JDBC-Database.At the same time it provides an index of updated entites.
   *
   * @param updateDb Connection to JDBC-Database where Updates should be stored.
   * @param workingDirectory The working-directory to download replication files to and save Update
   * states.
   * @param replicationUrl The URL to get replication files from. Determines if monthly, dayly etc.
   * updates are preferred.
   * @param producer a producer to promote updated entites to a kafka-cluster
   */
  public static void update(Path etlFiles, Connection keytables, Path workingDirectory, Connection updateDb, URL replicationUrl, Producer<String, Stream<Byte[]>> producer) throws SQLException {
    try (FileBasedLock fileLock = new FileBasedLock(workingDirectory.resolve(Updater.LOCK_FILE).toFile())) {
      fileLock.lock();
      Iterable<ReplicationFile> replicationFiles = OSCDownloader.download(replicationUrl, workingDirectory);
      Iterable<ChangeContainer> changes = OSCParser.parse(replicationFiles);
      Iterable<OSHEntity> oshEntities = OSCOSHTransformer.transform(etlFiles, keytables, changes);
      OSHLoader.load(updateDb, oshEntities, producer);
      fileLock.unlock();
    }
  }

  public static void update(Path etlFiles, Connection keytables, Path workingDirectory, Connection conn, URL replicationUrl) throws SQLException {
    Updater.update(etlFiles, keytables, workingDirectory, conn, replicationUrl, null);
  }

  /**
   * Flush updates form JDBC to real Ignite (best done once in a while, when database-usage is low).
   *
   * @param oshdb
   * @param updatedb
   */
  public static void flush(Connection oshdb, Connection updatedb) {
    //do i need to block the cluster somehow, to prevent false operations?
    //ignite.cluster().active(false);
    //do flush here: can I use something from the etl?
    //ignite.cluster().active(true);

  }

}
