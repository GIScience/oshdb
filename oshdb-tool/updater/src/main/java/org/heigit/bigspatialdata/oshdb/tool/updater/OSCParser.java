package org.heigit.bigspatialdata.oshdb.tool.updater;

import java.util.HashMap;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.heigit.bigspatialdata.oshdb.util.export.OutputWriter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class OSCParser extends DefaultHandler {

  private static final Logger LOG = LoggerFactory.getLogger(OSCParser.class);
  private static final HashMap<String, Integer> stats = new HashMap<>(3);

  private final String kafkaOSCTopic;
  private final String kafkaStatsTopic;
  private final String kafkaLiveUpdateTopicGeoserver;
  private JSONObject current;
  private String crea;
  protected final Producer<String, String> kafkaProdStats;
  protected final Producer<String, String> kafkaProdOSCUpdate;
  protected final Producer<String, String> kafkaProdLiveUpdate;

  protected OSCParser(Properties props) {
    super();
    this.kafkaStatsTopic = props.getProperty("kafkastat");
    this.kafkaOSCTopic = props.getProperty("kafkaosc-records");
    this.kafkaLiveUpdateTopicGeoserver = props.getProperty("geoserver");

    this.kafkaProdStats = OutputWriter.getKafka(this.kafkaStatsTopic, props);
    this.kafkaProdLiveUpdate = OutputWriter.getKafka(this.kafkaLiveUpdateTopicGeoserver, props);
    this.kafkaProdOSCUpdate = OutputWriter.getKafka(this.kafkaOSCTopic, props);
  }

  @Override
  public void startDocument() throws SAXException {
    super.startDocument();
    OSCParser.stats.put("Nodes", 0);
    OSCParser.stats.put("Ways", 0);
    OSCParser.stats.put("Relations", 0);
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes)
      throws SAXException {
    super.startElement(uri, localName, qName, attributes);

    switch (localName) {

      case "node":
        OSCParser.stats.put("Nodes", OSCParser.stats.get("Nodes") + 1);
        this.current = new JSONObject();
        this.current.put("#TypeArt", "node");
        this.current.put("ID", attributes.getValue("id"));
        this.current.put("lat", attributes.getValue("lat"));
        this.current.put("lon", attributes.getValue("lon"));
        this.current.put("user", attributes.getValue("user"));
        this.current.putOpt("uid", attributes.getValue("uid"));
        this.current.putOpt("version", attributes.getValue("version"));
        this.current.putOpt("changeset", attributes.getValue("changeset"));
        this.current.put("timestamp", attributes.getValue("timestamp"));
        this.current.put("#OSC_Modi", this.crea);
        break;

      case "way":
        OSCParser.stats.put("Ways", OSCParser.stats.get("Ways") + 1);
        this.current = new JSONObject();
        this.current.put("#TypeArt", "way");
        this.current.put("ID", attributes.getValue("id"));
        this.current.put("user", attributes.getValue("user"));
        this.current.putOpt("uid", attributes.getValue("uid"));
        this.current.putOpt("version", attributes.getValue("version"));
        this.current.putOpt("changeset", attributes.getValue("changeset"));
        this.current.put("timestamp", attributes.getValue("timestamp"));
        this.current.put("#OSC_Modi", this.crea);
        break;

      case "nd":
        this.current.append("nd", attributes.getValue("ref"));
        break;

      case "relation":
        OSCParser.stats.put("Relations", OSCParser.stats.get("Relations") + 1);
        this.current = new JSONObject();
        this.current.put("#TypeArt", "relation");
        this.current.put("ID", attributes.getValue("id"));
        this.current.put("user", attributes.getValue("user"));
        this.current.putOpt("uid", attributes.getValue("uid"));
        this.current.putOpt("version", attributes.getValue("version"));
        this.current.putOpt("changeset", attributes.getValue("changeset"));
        this.current.put("timestamp", attributes.getValue("timestamp"));
        this.current.put("#OSC_Modi", this.crea);
        break;

      case "member":
        this.current.accumulate("member",
            new JSONObject().put("#TypeArt", attributes.getValue("#TypeArt"))
                .put("ref", attributes.getValue("ref")).put("role", attributes.getValue("role")));
        break;

      case "tag":
        this.current.put(attributes.getValue("k"), attributes.getValue("v"));
        break;

      case "create":
        this.crea = "create";
        break;

      case "modify":
        this.crea = "modify";
        break;

      case "delete":
        this.crea = "delete";
        break;

      default:
        break;
    }

  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    super.endElement(uri, localName, qName);
    if (localName.equals("node")) {
      this.produce(this.kafkaOSCTopic);
      this.produce(this.kafkaLiveUpdateTopicGeoserver);
    } else if (localName.equals("way") || localName.equals("relation")) {
      this.produce(this.kafkaOSCTopic);
    }

  }

  @Override
  public void endDocument() throws SAXException {
    super.endDocument();
    this.produce(this.kafkaStatsTopic);
  }

  @SuppressWarnings("unchecked")
  private void produce(String goal) {
    if (goal.equals(this.kafkaOSCTopic)) {

      this.kafkaProdOSCUpdate.send(new ProducerRecord<>(goal, this.current.toString()));
    } else if (goal.equals(this.kafkaStatsTopic)) {
      this.kafkaProdStats
          .send(new ProducerRecord<>(goal, "Nodes: " + OSCParser.stats.get("Nodes") + "; Ways: "
              + OSCParser.stats.get("Ways") + "; Relations: " + OSCParser.stats.get("Relations")));
    } else if (goal.equals(this.kafkaLiveUpdateTopicGeoserver)) {
      String location = this.current.getString("lat") + "," + this.current.getString("lon");
      this.kafkaProdLiveUpdate.send(new ProducerRecord<>(goal, location));
    }

  }

}
