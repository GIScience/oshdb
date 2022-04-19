package org.heigit.ohsome.oshdb.util.xmlreader;

import static org.heigit.ohsome.oshdb.osm.OSMCoordinates.GEOM_PRECISION_TO_LONG;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.io.Files;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.heigit.ohsome.oshdb.impl.osh.OSHNodeImpl;
import org.heigit.ohsome.oshdb.impl.osh.OSHWayImpl;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osh.OSHNode;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * A helper class to load OSM XML data on the fly into OSHDB grid cells.
 */
public class OSMXmlReader {
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

        int longitude = Math.toIntExact(Math.round(lon * GEOM_PRECISION_TO_LONG));
        int latitude = Math.toIntExact(Math.round(lat * GEOM_PRECISION_TO_LONG));

        osm.setExtension(longitude, latitude);

        var oldOSM = OSM.node(osm.getId(), osm.getVersion() * (osm.isVisible() ? 1 : -1),
            osm.getEpochSecond(), osm.getChangeset(), osm.getUserId(), osm.getTags(), osm.getLon(),
            osm.getLat());
        nodes.put(id, oldOSM);
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
        var members = new OSMMember[ndList.getLength()];
        int idx = 0;
        for (Element m : iterableOf(ndList)) {
          long memId = attrAsLong(m, "ref");
          // members[idx++] = new OSMMemberWayIdOnly(memId);
          OSHEntity data = null;
          if (this.nodes.containsKey(memId)) {
            data = OSHNodeImpl.build(this.nodes.get(memId));
          }
          members[idx++] = new OSMMember(memId, OSMType.NODE, 0, data);
        }
        // osm.setExtension(members);
        var oldOSM = OSM.way(osm.getId(), osm.getVersion() * (osm.isVisible() ? 1 : -1),
            osm.getEpochSecond(), osm.getChangeset(), osm.getUserId(), osm.getTags(), members);
        ways.put(id, oldOSM);
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
            r = roles.size();
            roles.put(role, r);
          }

          // members[idx++] = new OSMMemberRelation(memId, t, r.intValue());
          OSHEntity data = null;
          OSMType t = null;
          // relation-relation-members do not get data, because they are unsupported
          switch (type.toLowerCase()) {
            case "node":
              t = OSMType.NODE;
              if (this.nodes.containsKey(memId)) {
                data = OSHNodeImpl.build(this.nodes().get(memId));
              }
              break;
            case "way":
              t = OSMType.WAY;
              Map<Long, OSHNode> wayNodes = new TreeMap<>();
              for (OSMWay way : this.ways().get(memId)) {
                for (OSMMember wayNode : way.getMembers()) {
                  wayNodes.putIfAbsent(wayNode.getId(), (OSHNode) wayNode.getEntity());
                }
              }
              if (this.ways().containsKey(memId)) {
                data = OSHWayImpl.build(
                    this.ways().get(memId),
                    wayNodes.values().stream().filter(Objects::nonNull).collect(Collectors.toList())
                );
              }
              break;
            case "relation":
              t = OSMType.RELATION;
              break;
            default:
              break;
          }
          members[idx++] = new OSMMember(memId, t, r, data);
        }
        // osm.setExtension(members);
        var oldOSM = OSM.relation(osm.getId(),
            osm.getVersion() * (osm.isVisible() ? 1 : -1),
            osm.getEpochSecond(), osm.getChangeset(), osm.getUserId(), osm.getTags(), members);
        relations.put(id, oldOSM);
      }
      lastId = id;
    }
  }

  /**
   * Add and read XML files to the database using their URLs.
   *
   * @param xmlFileUrl URL(s) to use
   */
  public void add(String... xmlFileUrl) {
    try {
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder = dbFactory.newDocumentBuilder();

      for (String p : xmlFileUrl) {
        Document doc = docBuilder.parse(p);
        read(doc);
      }
    } catch (ParserConfigurationException | SAXException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Add and read XML files to the database using their file paths.
   *
   * @param xmlFilePath file path(s) to use
   */
  @SuppressWarnings("UnstableApiUsage") // allow usage of getFileExtension(...), which is @Beta
  public void add(Path... xmlFilePath) {
    try {
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder = dbFactory.newDocumentBuilder();

      for (Path p : xmlFilePath) {
        String extension = Files.getFileExtension(p.toString());

        try (InputStream fileStream = new BufferedInputStream(new FileInputStream(p.toFile()))) {
          InputStream is = fileStream;
          if ("gz".equalsIgnoreCase(extension) || "zip".equalsIgnoreCase(extension)) {
            is = new GZIPInputStream(fileStream);
          }

          Document doc = docBuilder.parse(is);
          read(doc);
        }
      }
    } catch (ParserConfigurationException | SAXException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void entity(MutableOSMEntity osm, Element e) {
    osm.setId(attrAsLong(e, "id"));
    osm.isVisible(attrAsBoolean(e, "visible", true));
    osm.setVersion(attrAsInt(e, "version"));
    osm.setChangeset(attrAsLong(e, "changeset"));
    osm.setTimestamp(attrAsTimestampInSeconds(e, "timestamp"));
    osm.setUserId(attrAsInt(e, "uid", -1));
    osm.setTags(tags(e));
  }

  private static Iterable<Element> iterableOf(NodeList elements) {
    final int lastIndex = elements.getLength();
    return () -> new Iterator<>() {
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

  private static Iterable<Element> elementsByTag(Document doc, String tag) {
    return iterableOf(doc.getElementsByTagName(tag));
  }

  @SuppressWarnings("unused")
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
        k = keyValues.size();
        keys.put(key, k);
        keyValues.add(HashBiMap.create());
      }

      BiMap<String, Integer> values = keyValues.get(k);
      Integer v = values.get(value);
      if (v == null) {
        v = values.size();
        values.put(value, v);
      }

      tags[idx++] = k;
      tags[idx++] = v;
    }
    return tags;
  }

  /**
   * Get attribute {@code name} from {@link Element} {@code e} as {@code long}.
   */
  public static long attrAsLong(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Long.parseLong(attr.getValue());
    }
    throw new NoSuchElementException(e.getLocalName() + " doesn't have a attribute " + name);
  }

  /**
   * Get attribute {@code name} from {@link Element} {@code e} as {@code double}.
   */
  public static double attrAsDouble(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Double.parseDouble(attr.getValue());
    }
    throw new NoSuchElementException(e.getTextContent() + " doesn't have a attribute " + name);
  }

  /**
   * Get attribute {@code name} from {@link Element} {@code e} as {@code int}.
   */
  public static int attrAsInt(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Integer.parseInt(attr.getValue());
    }
    throw new NoSuchElementException(e.getLocalName() + " doesn't have a attribute " + name);
  }

  /**
   * Get attribute {@code name} from {@link Element} {@code e} as {@code int} with a default value
   * instead of a {@link NoSuchElementException}.
   */
  public static int attrAsInt(Element e, String name, int defaultValue) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Integer.parseInt(attr.getValue());
    }
    return defaultValue;
  }

  /**
   * Get attribute {@code name} from {@link Element} {@code e} as {@code boolean}.
   */
  public static boolean attrAsBoolean(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Boolean.parseBoolean(attr.getValue());
    }
    throw new NoSuchElementException(e.getLocalName() + " doesn't have a attribute " + name);
  }

  /**
   * Get attribute {@code name} from {@link Element} {@code e} as {@code boolean} with a default
   * value instead of a {@link NoSuchElementException}.
   */
  public static boolean attrAsBoolean(Element e, String name, boolean defaultValue) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Boolean.parseBoolean(attr.getValue());
    }
    return defaultValue;
  }

  /**
   * Get attribute {@code name} from {@link Element} {@code e} as parsed timestamp ({@code long}).
   */
  public static long attrAsTimestampInSeconds(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return Instant.parse(attr.getValue()).getEpochSecond();
    }
    throw new NoSuchElementException(e.getLocalName() + " doesn't have a attribute " + name);
  }

  /**
   * Get attribute {@code name} from {@link Element} {@code e} as {@link String}.
   */
  public static String attrAsString(Element e, String name) {
    Attr attr = e.getAttributeNode(name);
    if (attr != null) {
      return attr.getValue();
    }
    throw new NoSuchElementException(e.getLocalName() + " doesn't have a attribute " + name);
  }
}
