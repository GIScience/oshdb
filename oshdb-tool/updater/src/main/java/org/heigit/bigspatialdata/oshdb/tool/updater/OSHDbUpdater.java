package org.heigit.bigspatialdata.oshdb.tool.updater;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: enhance code, this is only version 1 TODO: simplify code TODO: Comment
 * code; TODO: Create Test-Classes; TODO: Write Javadoc TODO: Live Kafka
 * Datastore: änlich: https://osmlab.github.io/show-me-the-way/ TODO: Good
 * Logging TODO: Add interface for Raphaels parser to post the data to the
 * cluster TODO: Add more configuration possibilities to the config? TODO: Make
 * everything fault-tolerant by writing the current status to a file and
 * optionally reading from it on initialisation TODO: promote OSCEntities not
 * JSONObjects.
 *
 */
public class OSHDbUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(OSHDbUpdater.class);

  /**
   * @param args the command line arguments
   * @throws java.text.ParseException
   * @throws java.io.IOException
   * @throws java.lang.InterruptedException
   */
  public static void main(String[] args) throws ParseException, IOException, IllegalArgumentException, InterruptedException {
    //parse input (TODO: use JCommmander, enable .osh.pbf as input to read creation time from)
    Properties inputprops = OSHDbUpdater.parseinput(args);
    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy--HH-mm-ss");

    //initialise download
    OSCDownloader down = new OSCDownloader(sdf.parse(inputprops.getProperty("osh-create")), FileSystems.getDefault().getPath(inputprops.getProperty("tempdir")));
    //start downloading
    Thread downloadThread = new Thread(down, "OSCDownload-Thread");
    downloadThread.start();

    //initialise transformer
    OSCTransformer transform = new OSCTransformer(inputprops);
    //parse and transform file to OSCEntity

    //promote to Kafka
    Thread promoterthread = new Thread(transform, "OSCPromoter-Thread");
    promoterthread.start();
  }

  private static Properties parseinput(String[] args) throws FileNotFoundException, IOException, IllegalArgumentException {
    Properties props = new Properties(OSHDbUpdater.getdefaults());
    if (args.length < 1) {
      props = OSHDbUpdater.getdefaults();
    } else if (args.length < 2) {
      throw new IllegalArgumentException("Either give me no argument to use the defaults"
              + "or give me --> -config /path/to/config.properties <-- as argument!");
    } else if (args[0].equalsIgnoreCase("-config")) {
      File configFile = new File(args[1]);
      FileReader confreader = new FileReader(configFile);
      props.load(confreader);
    } else {
      throw new IllegalArgumentException("Could not find -config in first argument, please specify the config file first!");
    }

    return props;
  }

  private static Properties getdefaults() {
    Properties defaults = new Properties();
    defaults.put("osh-create", "04-11-2016--13-00-00");
    defaults.put("tempdir", "/opt/kafka/TempOSC");
    defaults.put("kafkaosc-records", "OSCUpdate");
    defaults.put("kafkastat", "OSCUpdateStats");
    defaults.put("geoserver", "geoserver");
    defaults.put("bootstrap.servers", "192.168.2.6:9092");
    defaults.put("acks", "all");
    defaults.put("retries", 0);
    defaults.put("batch.size", 16384);
    defaults.put("linger.ms", 1);
    defaults.put("buffer.memory", 33554432);
    defaults.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    defaults.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return defaults;
  }

}
