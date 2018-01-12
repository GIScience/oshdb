//is based on the BasicOSMParser by Panier Avide uder the GPL (https://github.com/PanierAvide/BasicOSMParser)
// does this affect anything?
package org.heigit.bigspatialdata.oshdb.tool.updater;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * OSCTransformer parses XML file and creates corresponding Java objects.
 */
public class OSCTransformer implements Runnable {
  
  private static final Logger LOG = LoggerFactory.getLogger(OSCTransformer.class);
  private static final BlockingQueue<JSONObject> outQ = new LinkedBlockingQueue<>(200);

  /**
   * This is a first possibility for implementation of an updater. The
   * JSONObject cloud then be taken by the osh-pbf parser to send the data to
   * the BigDB. Another possibility would be to have a static method in the
   * parser, that could be accessed from here and that could directly update the
   * data. Is Kafka still needed here, at the moment it seems to be a detour.
   * Not yet fully functional but will be, when final BigDB layout is set.
   *
   * @param outputprop
   * @param topics
   * @throws java.lang.InterruptedException
   */
  public static void getUpdates(Properties outputprop, ArrayList<String> topics) throws InterruptedException {
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(outputprop);
    consumer.subscribe(topics);
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Integer.parseInt(outputprop.getProperty("polltimeout")));
      for (ConsumerRecord<String, String> record : records) {
        if (!record.value().isEmpty()) {
          JSONObject tempupdate = new JSONObject(record.value());
          OSCTransformer.outQ.put(tempupdate);
          LOG.info(OSCTransformer.outQ.take().toString());
          consumer.commitSync();
        }
      }
    }
  }
  private final OSCParser parser;
  
  public OSCTransformer(Properties props) {
    parser = new OSCParser(props);
    
  }
  
  private void parse(File input) throws SAXException {
    try {
      InputSource InFile = new InputSource(new FileReader(input));

      //Start parsing
      XMLReader xr = XMLReaderFactory.createXMLReader();
      xr.setContentHandler(parser);
      xr.setErrorHandler(parser);
      xr.parse(InFile);
    } catch (FileNotFoundException ex) {
      LOG.error("Could not find file but that is unusual. Have you deleted anything?");
    } catch (IOException ex) {
      LOG.error("That file had an error, could not parse it");
    }
  }
  
  @Override
  public void run() {
    while (true) {
      try {
        File currparse = OSCDownloader.downfiles.take();
        LOG.debug(currparse.toString());
        this.parse(currparse);
        currparse.delete();
      } catch (InterruptedException | SAXException ex) {
        this.parser.kafkaProdStats.close();
        LOG.error(ex.getLocalizedMessage());
      }
    }
    
  }
  
}
