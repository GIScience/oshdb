package org.heigit.bigspatialdata.oshdb.util.export;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.stream.Stream;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class aims at providing you with the basic functions to store your analyses Result. It tries
 * to facilitate output and admin work by centrally organising outputs. Once your have created
 * output and provided your username, ClusterAdmins will know how to provide your with your results.
 */
public class OutputWriter {
  private static final Logger LOG = LoggerFactory.getLogger(OutputWriter.class);

  private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
  private static final ArrayList<String> kafkatopic = new ArrayList<>();

  /**
   * Get the handle to a standard file.
   *
   * @param username your username. It will be part of the output.
   * @return
   * @throws IOException
   */
  public static File getFile(String username) throws IOException {
    File file = new File("/opt/ignite/UserOutput/", username + "_" + dateFormat.format(new Date()));
    if (!file.exists()) {
      file.getParentFile().mkdir();
      file.createNewFile();
    }
    return file;
  }

  /**
   * Writes a simple CSV-File.
   *
   * @param username your username. It will be part of the output.
   * @param output The Data your want to store. The method call forEach on the stream.
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static void toCSV(String username, Stream<Pair<String, String>> output)
      throws IOException {
    File file = OutputWriter.getFile(username);

    try (PrintWriter out = new PrintWriter(file);) {
      output.forEach((Pair<String, String> pair) -> {
        out.println(pair.getKey() + "," + pair.getValue());
      });

    }
  }

  /**
   * Get the handle to a standard H2-DB you can fill.
   *
   * @param username your username. It will be part of the output.
   * @return
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public static Connection getH2(String username) throws ClassNotFoundException, SQLException {
    Class.forName("org.h2.Driver");
    return DriverManager.getConnection(
        "jdbc:h2:/opt/ignite/UserOutput/" + username + "_" + dateFormat.format(new Date()),
        username, "");
  }

  /**
   * Creates a H2 Database.
   *
   * @param username your username. It will be part of the output.
   * @param output The Data your want to store
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public static void toH2(String username, Stream<Pair<String, String>> output)
      throws ClassNotFoundException, SQLException {
    try (Connection conn = OutputWriter.getH2(username)) {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate(
          "drop table if exists result; create table if not exists result(key varchar(max), value varchar(max))");
      try (PreparedStatement insert =
          conn.prepareStatement("insert into result (key,value) values(?,?)")) {
        output.forEach((Pair<String, String> pair) -> {
          try {
            insert.setString(1, pair.getKey());
            insert.setString(2, pair.getValue());
            insert.addBatch();
          } catch (SQLException ex) {
            LOG.error(ex.toString());
          }

        });
        insert.executeBatch();

      }

    }
  }

  /**
   * Get the handle to a standard PostgreSQL Database, you can fill.
   *
   * @param username your username. It will be part of the output.
   * @return
   */
  public static Connection getPostgres(String username) throws ClassNotFoundException, SQLException {
    Class.forName("org.postgresql.Driver");
    String usernameDate = username + "_" + dateFormat.format(new Date());
    try (Connection conn =
        DriverManager.getConnection("jdbc:postgresql://localhost:5432/", "postgres", "HeiGit")) {
      Statement stmt = conn.createStatement();
      stmt.executeUpdate("CREATE USER " + usernameDate + " WITH PASSWORD 'password'");
      stmt.executeUpdate("CREATE DATABASE " + usernameDate);
      stmt.executeUpdate(
          "GRANT ALL PRIVILEGES ON DATABASE " + usernameDate + " TO " + usernameDate);
    }
    return DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + usernameDate,
        usernameDate, "password");
  }

  /**
   * Stores the data in the PostgreSql DB on the server.
   *
   * @param username your username. It will be part of the output.
   * @param output The Data your want to store
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  public static void toPostgres(String username, Stream<Pair<String, String>> output)
      throws ClassNotFoundException, SQLException {
    try (Connection conn2 = OutputWriter.getPostgres(username)) {
      Statement stmt2 = conn2.createStatement();

      stmt2.executeUpdate(
          "drop table if exists result; create table if not exists result(key text, value text)");
      try (PreparedStatement insert =
          conn2.prepareStatement("insert into result (key,value) values(?,?)")) {
        output.forEach(pair -> {
          try {
            insert.setString(1, pair.getKey());
            insert.setString(2, pair.getValue());
            insert.addBatch();
          } catch (SQLException ex) {
            LOG.error(ex.toString());
          }

        });
        insert.executeBatch();

      }

    }

  }

  /**
   * Creates the specified topic if it does not exist and creates a new KafkaProducer containing
   * your properties. It overwrites the "bootstrap.servers" property to make it work on the cluster.
   * Be sure to provide all important properties (See
   * <a href="https://kafka.apache.org/documentation/#producerconfigs"> Kafka Documentation</a>),
   * especially the Serialiser for your given K and V.
   *
   * @param <K> Key Type of your producer
   * @param <V> Value Type of your producer
   * @param topic Topic your want to broadcast to. Is Created if it does not exist.
   * @return
   */
  public static <K, V> Producer<K, V> getKafka(String topic, Properties props) {
    // check if it was created in this instance. If not check if it still exists in the cluster.
    // This is to prevent multiple time consuming checks on zookeeper.
    if (!kafkatopic.contains(topic)) {
      OutputWriter.createKafkaTopic(topic);
    }
    props.put("bootstrap.servers", "localhost:9092");
    return new KafkaProducer<>(props);

  }

  /**
   * Promotes the result to the Kafka broker. This is especially useful if you want to get
   * intermediate results before your Task is finished.
   *
   * @param topic Topic your want to broadcast to. Is Created if it does not exist.
   * @param output The Data your want to store
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static void toKafka(String topic, Stream<Pair<String, String>> output)
      throws FileNotFoundException, IOException {
    // check if it was created in this instance. If not check if it still exists in the cluster.
    // This is to prevent multiple time consuming checks on zookeeper.
    if (!kafkatopic.contains(topic)) {
      OutputWriter.createKafkaTopic(topic);
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    try (Producer<String, String> producer = new KafkaProducer<>(props);) {
      output.forEach(pair -> {
        producer.send(new ProducerRecord<String, String>(topic, pair.getKey(), pair.getValue()));
      });
    }
  }

  private static void createKafkaTopic(String topic) {
    String zookeeperConnect = "localhost:2181";// ,zkserver2:2181";
    int sessionTimeoutMs = 10 * 1000;
    int connectionTimeoutMs = 8 * 1000;

    int partitions = 1;
    int replication = 1;
    Properties topicConfig = new Properties(); // add per-topic configurations settings here

    // Note: You must initialize the ZkClient with ZKStringSerializer. If you don't, then
    // createTopic() will only seem to work (it will return without error). The topic will exist in
    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
    // topic.
    ZkClient zkClient = new ZkClient(zookeeperConnect, sessionTimeoutMs, connectionTimeoutMs,
        ZKStringSerializer$.MODULE$);

    // Security for Kafka was added in Kafka 0.9.0.0
    boolean isSecureKafkaCluster = false;

    ZkUtils zkUtils =
        new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
    if (!AdminUtils.topicExists(zkUtils, topic)) {
      AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig,
          RackAwareMode.Enforced$.MODULE$);
    }
    zkClient.close();
    kafkatopic.add(topic);

  }

  private OutputWriter() {}

}
