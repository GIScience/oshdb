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
import org.apache.ignite.IgniteCheckedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.updater.OSCHandling.OSCDownloader;
import org.heigit.bigspatialdata.updater.OSCHandling.OSCParser;
import org.heigit.bigspatialdata.updater.OSHUpdating.OSHLoader;
import org.heigit.bigspatialdata.updater.util.OSCOSHTransformer;
import org.heigit.bigspatialdata.updater.util.ReplicationFile;
import org.heigit.bigspatialdata.updater.util.cmd.UpdateArgs;
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

  public static void main(String[] args)
      throws MalformedURLException, ClassNotFoundException,
      IgniteCheckedException, FileNotFoundException, IOException, SQLException {
    UpdateArgs config = new UpdateArgs();
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
    config.workDir.toFile().mkdirs();

    //for some reason postgres stops to work and is overwritten by h2 if this is not called:
    Class.forName("org.postgresql.Driver");
    Class.forName("org.h2.Driver");
    Class.forName("org.apache.ignite.IgniteJdbcDriver");
    try (Connection updateDb = DriverManager.getConnection(config.baseArgs.jdbc);
        Connection keytables = DriverManager.getConnection(config.keytables, "sa", "");
        Connection dbBit = DriverManager.getConnection(config.baseArgs.dbbit)) {
      if (config.kafka != null) {
        Properties props = new Properties();
        FileInputStream input = new FileInputStream(config.kafka);
        props.load(input);
        try (Producer<Long, Byte[]> producer = new KafkaProducer<>(props)) {
          Updater.update(config.etl, keytables, config.workDir, updateDb, config.baseURL, dbBit,
              producer);
        }
      } else {
        Updater.update(config.etl, keytables, config.workDir, updateDb, config.baseURL, dbBit);
      }
    }
  }

  /**
   * Downloads replication files, transforms them to OSHDB-Objects and stores them in a
   * JDBC-Database.At the same time it provides an index of updated entites.
   *
   * @param etlFiles The directory for the Files containing all currently known Entites
   * @param keytables Database for keytables
   * @param updateDb Connection to JDBC-Database where Updates should be stored.
   * @param workingDirectory The working-directory to download replication files to and save Update
   * states.
   * @param replicationUrl The URL to get replication files from. Determines if monthly, dayly etc.
   * updates are preferred.
   * @param dbBit Database for a BitMap flagging changed Entites
   * @param producer a producer to promote updated entites to a kafka-cluster
   * @throws java.sql.SQLException
   */
  public static void update(Path etlFiles, Connection keytables, Path workingDirectory,
      Connection updateDb, URL replicationUrl, Connection dbBit, Producer<Long, Byte[]> producer)
      throws SQLException, IOException, ClassNotFoundException {
    try (FileBasedLock fileLock = new FileBasedLock(workingDirectory.resolve(Updater.LOCK_FILE)
        .toFile())) {
      fileLock.lock();
      Iterable<ReplicationFile> replicationFiles = OSCDownloader.download(replicationUrl,
          workingDirectory);
      Iterable<ChangeContainer> changes = OSCParser.parse(replicationFiles);
      Iterable<OSHEntity> oshEntities = OSCOSHTransformer.transform(etlFiles, keytables, changes);
      OSHLoader.load(updateDb, oshEntities, dbBit, producer);
      fileLock.unlock();
    }
  }

  public static void update(Path etlFiles, Connection keytables, Path workingDirectory,
      Connection conn, URL replicationUrl, Connection dbBit) throws SQLException, IOException,
      ClassNotFoundException {
    Updater.update(etlFiles, keytables, workingDirectory, conn, replicationUrl, dbBit, null);
  }

}
