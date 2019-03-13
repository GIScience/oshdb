package org.heigit.bigspatialdata.oshdb.util.test;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHNode;
import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.osm2.impl.MutableOSMEntity;
import org.heigit.bigspatialdata.oshdb.osm2.impl.MutableOSMNode;
import org.heigit.bigspatialdata.oshdb.osm2.impl.MutableOSMRelation;
import org.heigit.bigspatialdata.oshdb.osm2.impl.MutableOSMWay;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.io.Files;

public class OSMXmlReader {

  private static class Test {

    public void run() {
      OSMXmlReader db = new OSMXmlReader();

      Path testDataDir = Paths.get(getClass().getResource("data").getPath());

      db.add(testDataDir.resolve("relation/r4815251.osh.gz"));

      db.relations().asMap().forEach((id, versions) -> {
        System.out.println("id:" + id);
        versions.forEach(osm -> {
          System.out.println("\t" + osm);
        });
      });

      System.out.println("\n\n");
      db.ways.get(27913435L).forEach(osm -> {
        System.out.println("\t" + osm);
      });

      int key = 6;
      System.out.println(db.keys.inverse().get(Integer.valueOf(key)));
      System.out.println(db.keys.get("place"));

      System.out.println(db.keyValues.get(key).inverse().get(Integer.valueOf(2)));

    }
  }

  public static void main(String[] args) {
    Test t = new Test();
    t.run();
  }

  BiMap<String, Integer> keys = HashBiMap.create();
  List<BiMap<String, Integer>> keyValues = new ArrayList<>();

  BiMap<String, Integer> roles = HashBiMap.create();

  ListMultimap<Long, OSMNode> nodes = MultimapBuilder.treeKeys().arrayListValues().build();
  ListMultimap<Long, OSMWay> ways = MultimapBuilder.treeKeys().arrayListValues().build();
  ListMultimap<Long, OSMRelation> relations = MultimapBuilder.treeKeys().arrayListValues().build();

  public BiMap<String, Integer> keys() {
    return keys;
  }

  public List<BiMap<String, Integer>> keyValues() {
    return keyValues;
  }

  public BiMap<String, Integer> roles() {
    return roles;
  }

  public ListMultimap<Long, OSMNode> nodes() {
    return nodes;
  }

  public ListMultimap<Long, OSMWay> ways() {
    return ways;
  }

  public ListMultimap<Long, OSMRelation> relations() {
    return relations;
  }

  private void read(Document doc) throws IOException {
    long lastId = -1;
    long skipId = -1;

    for (Element e : elementsByTag(doc, "node")) {
      MutableOSMNode osm = new MutableOSMNode();
      entity(osm, e);

      long id = osm.getId();

      if (lastId != id && nodes.containsKey(id)) {
        skipId = id;
      }

      if (skipId != id) {
        double lon = osm.isVisible() ? attrAsDouble(e, "lon") : 0.0;
        double lat = osm.isVisible() ? attrAsDouble(e, "lat") : 0.0;

        long longitude = (long) (lon * OSHDB.GEOM_PRECISION_TO_LONG);
        long latitude = (long) (lat * OSHDB.GEOM_PRECISION_TO_LONG);

        osm.setExtension(longitude, latitude);

        OSMNode oldOSM = new OSMNode(osm.getId(), osm.getVersion() * (osm.isVisible() ? 1 : -1), osm.getTimestamp(),
            osm.getChangeset(), osm.getUserId(), osm.getTags(), osm.getLon(), osm.getLat());
        nodes.put(Long.valueOf(id), oldOSM);
      }
      lastId = id;
    }

    lastId = -1;
    skipId = -1;
    for (Element e : elementsByTag(doc, "way")) {
      MutableOSMWay osm = new MutableOSMWay();
      entity(osm, e);

      long id = osm.getId();
      if (lastId != id && ways.containsKey(id)) {
        skipId = id;
      }

      if (skipId != id) {
        NodeList ndList = e.getElementsByTagName("nd");
        OSMMember[] members = new OSMMember[ndList.getLength()];
        int idx = 0;
        for (Element m : iterableOf(ndList)) {
          long memId = attrAsLong(m, "ref");
          // members[idx++] = new OSMMemberWayIdOnly(memId);
          OSHEntity data = null;
          if (this.nodes.containsKey(memId)) {
            data = OSHNode.build(this.nodes.get(memId));
          }
          members[idx++] = new OSMMember(memId, OSMType.NODE, 0, data);
        }
        // osm.setExtension(members);
        OSMWay oldOSM = new OSMWay(osm.getId(), osm.getVersion() * (osm.isVisible() ? 1 : -1), osm.getTimestamp(),
            osm.getChangeset(), osm.getUserId(), osm.getTags(), members);
        ways.put(Long.valueOf(id), oldOSM);
      }
      lastId = id;
    }

    lastId = -1;
    skipId = -1;
    for (Element e : elementsByTag(doc, "relation")) {
      MutableOSMRelation osm = new MutableOSMRelation();
      entity(osm, e);

      long id = osm.getId();
      if (lastId != id && relations.containsKey(id)) {
        skipId = id;
      }

      if (skipId != id) {
        NodeList ndList = e.getElementsByTagName("member");
        OSMMember[] members = new OSMMember[ndList.getLength()];
        int idx = 0;
        for (Element m : iterableOf(ndList)) {
          long memId = attrAsLong(m, "ref");
          String role = attrAsString(m, "role");
          String type = attrAsString(m, "type");

          Integer r = roles.get(role);
          if (r == null) {
            r = Integer.valueOf(roles.size());
            roles.put(role, r);
          }

          OSMType t;
          if ("node".equalsIgnoreCase(type))
            t = OSMType.NODE;
          else if ("way".equalsIgnoreCase(type)) {
            t = OSMType.WAY;
          } else if ("relation".equalsIgnoreCase(type)) {
            t = OSMType.RELATION;
          } else {
            t = OSMType.UNKNOWN;
          }

          // members[idx++] = new OSMMemberRelation(memId, t, r.intValue());
          OSHEntity data = null;
          switch (t) {
            case NODE:
              if (this.nodes.containsKey(memId)) {
                data = OSHNode.build(this.nodes().get(memId));
              }
              break;
            case WAY:
              Map<Long, OSHNode> wayNodes = new TreeMap<>();
              for (OSMWay way : this.ways().get(memId)) {
                for (OSMMember wayNode : way.getRefs()) {
                  wayNodes.putIfAbsent(wayNode.getId(), (OSHNode)wayNode.getEntity());
                }
              }
              if (this.ways().containsKey(memId)) {
                data = OSHWay.build(this.ways().get(memId), wayNodes.values());
              }
              break;
          }
          members[idx++] = new OSMMember(memId, t, r.intValue(), data);
        }
        // osm.setExtension(members);
        OSMRelation oldOSM = new OSMRelation(osm.getId(), osm.getVersion() * (osm.isVisible() ? 1 : -1),
            osm.getTimestamp(), osm.getChangeset(), osm.getUserId(), osm.getTags(), members);
        relations.put(Long.valueOf(id), oldOSM);
      }
      lastId = id;
    }
  }

  public void add(String... xmlFileUrl) {
    try {
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

      for (String p : xmlFileUrl) {
        Document doc = dBuilder.parse(p);
        read(doc);
      }
    } catch (ParserConfigurationException | SAXException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void add(Path... xmlFilePath) {
    try {
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();

      for (Path p : xmlFilePath) {
        String extension = Files.getFileExtension(p.toString());

        try (InputStream fileStream = new BufferedInputStream(new FileInputStream(p.toFile()))) {
          InputStream is = fileStream;
          if ("gz".equalsIgnoreCase(extension) || "zip".equalsIgnoreCase(extension)) {
            is = new GZIPInputStream(fileStream);
          }

          Document doc = dBuilder.parse(is);
          read(doc);
        }
      }
    } catch (ParserConfigurationException | SAXException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void entity(MutableOSMEntity osm, Element e) {
    osm.setId(attrAsLong(e, "id"));
    osm.isVisible(attrAsBoolean(e, "visible"));
    osm.setVersion(attrAsInt(e, "version"));
    osm.setChangeset(attrAsLong(e, "changeset"));
    osm.setTimestamp(attrAsTimestampInSeconds(e, "timestamp"));
    osm.setUserId(attrAsInt(e, "uid"));
    osm.setTags(tags(e));
  }

  private static Iterable<Element> iterableOf(NodeList elements) {
    final int lastIndex = elements.getLength();
    return new Iterable<Element>() {
      @Override
      public Iterator<Element> iterator() {
        return new Iterator<Element>() {
          private int index = 0;

          @Override
          public boolean hasNext() {
            return index < lastIndex;
          }

          @Override
          public Element next() {
            return (Element) elements.item(index++);
          }
        };
      }
    };
  }

  private static Iterable<Element> elementsByTag(Document doc, String tag) {
    return iterableOf(doc.getElementsByTagName(tag));
  }

  private static Iterable<Element> elementsByTag(Element e, String tag) {
    return iterableOf(e.getElementsByTagName(tag));
  }

  private int[] tags(Element e) {
    NodeList tagList = e.getElementsByTagName("tag");
    int[] tags = new int[tagList.getLength() * 2];
    int idx = 0;
    for (Element t : iterableOf(tagList)) {
      String key = attrAsString(t, "k");
      String value = attrAsString(t, "v");

      Integer k = keys.get(key);
      if (k == null) {
        k = Integer.valueOf(keyValues.size());
        keys.put(key, k);
        keyValues.add(HashBiMap.create());
      }

      BiMap<String, Integer> values = keyValues.get(k.intValue());
      Integer v = values.get(value);
      if (v == null) {
        v = Integer.valueOf(values.size());
        values.put(value, v);
      }

      tags[idx++] = k.intValue();
      tags[idx++] = v.intValue();
    }
    return tags;
  }

  public static long attrAsLong(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Long.parseLong(attr.getValue());
    }
    throw new NoSuchElementException(e.getLocalName() + " doesn't have a attribute " + name);
  }

  public static double attrAsDouble(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Double.parseDouble(attr.getValue());
    }
    throw new NoSuchElementException(e.getTextContent() + " doesn't have a attribute " + name);
  }

  public static int attrAsInt(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Integer.parseInt(attr.getValue());
    }
    throw new NoSuchElementException(e.getLocalName() + " doesn't have a attribute " + name);
  }

  public static boolean attrAsBoolean(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Boolean.parseBoolean(attr.getValue());
    }
    throw new NoSuchElementException(e.getLocalName() + " doesn't have a attribute " + name);
  }

  public static long attrAsTimestampInSeconds(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Instant.parse(attr.getValue()).getEpochSecond();
    }
    throw new NoSuchElementException(e.getLocalName() + " doesn't have a attribute " + name);
  }

  public static String attrAsString(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return attr.getValue();
    }
    throw new NoSuchElementException(e.getLocalName() + " doesn't have a attribute " + name);
  }
}
