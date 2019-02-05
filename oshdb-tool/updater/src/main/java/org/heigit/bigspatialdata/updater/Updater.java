package org.heigit.bigspatialdata.updater;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
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

  public static void main(String[] args) throws MalformedURLException, SQLException, ClassNotFoundException, IgniteCheckedException, FileNotFoundException, IOException {
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

    Connection conn = DriverManager.getConnection(config.jdbc);
    Connection keytables = DriverManager.getConnection(config.keytables, "sa", "");
    config.workDir.toFile().mkdirs();
    Map<OSMType, File> etlFiles = new HashMap<>();
    etlFiles.put(OSMType.NODE, config.nodeEtl);
    etlFiles.put(OSMType.WAY, config.wayEtl);
    etlFiles.put(OSMType.RELATION, config.relationEtl);

    if (config.flush) {
      Ignition.setClientMode(true);
      IgniteConfiguration cfg = IgnitionEx.loadConfiguration(config.dbconfig.toString()).get1();
      cfg.setIgniteInstanceName("IgniteUpdateClientInstance");
      try (Ignite ignite = Ignition.start(cfg)) {
        Updater.flush(ignite, conn);
      }

    } else {
      if (config.kafka != null) {
        Properties props = new Properties();
        FileInputStream input = new FileInputStream(config.kafka);
        props.load(input);
        Producer<String, Stream<Byte[]>> producer = new KafkaProducer<>(props);
        Updater.update(etlFiles, keytables, config.workDir, conn, config.baseURL, producer);
        producer.close();
      } else {
        Updater.update(etlFiles, keytables, config.workDir, conn, config.baseURL);
      }

    }
  }

  /**
   * Downloads replication files, transforms them to OSHDB-Objects and stores them in a
   * JDBC-Database.At the same time it provides an index of updated entites.
   *
   * @param conn Connection to JDBC-Database where Updates should be stored.
   * @param workingDirectory The working-directory to download replication files to and save Update
   * states.
   * @param replicationUrl The URL to get replication files from. Determines if monthly, dayly etc.
   * updates are preferred.
   * @param producer a producer to promote updated entites to a kafka-cluster
   */
  public static void update(Map<OSMType, File> etlFiles, Connection keytables, Path workingDirectory, Connection conn, URL replicationUrl, Producer<String, Stream<Byte[]>> producer) {
    try (FileBasedLock fileLock = new FileBasedLock(workingDirectory.resolve(Updater.LOCK_FILE).toFile())) {
      fileLock.lock();
      Iterable<ReplicationFile> replicationFiles = OSCDownloader.download(replicationUrl, workingDirectory);
      Iterable<ChangeContainer> changes = OSCParser.parse(replicationFiles);
      Iterable<OSHEntity> oshEntities = OSCOSHTransformer.transform(etlFiles, keytables, changes);
      OSHLoader.load(conn, oshEntities, producer);
      fileLock.unlock();
    }
  }

  public static void update(Map<OSMType, File> etlFiles, Connection keytables, Path workingDirectory, Connection conn, URL replicationUrl) {
    Updater.update(etlFiles, keytables, workingDirectory, conn, replicationUrl, null);
  }

  /**
   * Flush updates form JDBC to real Ignite (best done once in a while, when database-usage is low).
   *
   * @param ignite Ignite-instace of actual OSHDB.
   * @param conn JDBC of intermediate Update storage.
   */
  public static void flush(Ignite ignite, Connection conn) {
    //do i need to block the cluster somehow, to prevent false operations?
    //ignite.cluster().active(false);
    //do flush here: can I use something from the etl?
    //ignite.cluster().active(true);

  }

}
