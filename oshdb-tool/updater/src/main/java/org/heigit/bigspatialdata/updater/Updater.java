package org.heigit.bigspatialdata.updater;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.apache.ignite.IgniteCheckedException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
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

  static final String LOCK_FILE = "update.lock";
  private static final Logger LOG = LoggerFactory.getLogger(Updater.class);

  public static void main(String[] args)
      throws MalformedURLException,
      ClassNotFoundException,
      IgniteCheckedException,
      FileNotFoundException,
      IOException,
      SQLException {

    //read commandline
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

    //for some reason postgres stops to work and is overwritten by h2 if this is not called:
    Class.forName("org.postgresql.Driver");
    Class.forName("org.h2.Driver");
    Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

    //establish jdbc-connections
    try (Connection updateDb = DriverManager.getConnection(config.baseArgs.jdbc);
        Connection keytablesDb = DriverManager.getConnection(config.keytables, "sa", "");
        Connection bitmapDb = DriverManager.getConnection(config.baseArgs.dbbit)) {

      if (config.kafka != null) {
        //create Kafka-promoter
        Properties props = new Properties();
        FileInputStream input = new FileInputStream(config.kafka);
        props.load(input);
        //run update with Kafka
        try (Producer<Long, Byte[]> producer = new KafkaProducer<>(props)) {
          Updater.update(
              config.baseArgs.etl,
              keytablesDb,
              updateDb,
              config.baseURL,
              bitmapDb,
              config.baseArgs.batchSize,
              producer
          );
        }
      } else {
        //run update without kafka
        Updater.update(
            config.baseArgs.etl,
            keytablesDb,
            updateDb,
            config.baseURL,
            bitmapDb,
            config.baseArgs.batchSize,
            null
        );
      }
    }
  }

  /**
   * Downloads replication files, transforms them to OSHDB-Objects and stores them in a
   * JDBC-Database.At the same time it maintains an index of updated entites.
   *
   * If working on a regional extract be aware that there is currently no method to limit imports to
   * that region. Updated entities outside the scope of the used @link{etlFile} will have missing
   * data.
   *
   * @param etlFiles The directory for the Files containing all currently known Entites
   * @param keytables Database for keytables
   * @param updateDb Connection to JDBC-Database where Updates should be stored.
   * @param replicationUrl The URL to get replication files from. Determines if monthly, dayly etc.
   * updates are preferred.
   * @param dbBit Database for a BitMap flagging changed Entites
   * @param batchSize the number of changes that are processes in batches. This may help if you
   * expect a high number of entities to be modified multiple times during this update period.
   * @param producer a producer to promote updated entites to a kafka-cluster. May be null if not
   * desired.
   * @throws java.sql.SQLException
   * @throws java.io.IOException
   * @throws java.lang.ClassNotFoundException
   */
  public static void update(
      Path etlFiles,
      Connection keytables,
      Connection updateDb,
      URL replicationUrl,
      Connection dbBit,
      int batchSize,
      Producer<Long, Byte[]> producer
  ) throws SQLException,
      IOException,
      ClassNotFoundException {
    Path wd = Paths.get("target/updaterWD/");
    wd.toFile().mkdirs();

    try (FileBasedLock fileLock = new FileBasedLock(
        wd.resolve(Updater.LOCK_FILE).toFile())) {
      fileLock.lock();
      //download replicationFiles
      Iterable<ReplicationFile> replicationFiles = OSCDownloader.download(replicationUrl, wd);
      //parse replicationFiles
      Iterable<ChangeContainer> changes = OSCParser.parse(replicationFiles);
      //transform files to OSHEntities
      Iterable<Map<OSMType, Map<Long, OSHEntity>>> oshEntities
          = OSCOSHTransformer.transform(etlFiles, keytables, batchSize, changes);
      //load data into updateDb
      OSHLoader.load(updateDb, oshEntities, dbBit, producer);
      fileLock.unlock();
    }
  }

}
