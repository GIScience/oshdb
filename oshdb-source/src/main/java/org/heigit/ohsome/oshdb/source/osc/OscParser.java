package org.heigit.ohsome.oshdb.source.osc;

import static com.google.common.collect.Streams.stream;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static javax.xml.stream.XMLStreamConstants.CDATA;
import static javax.xml.stream.XMLStreamConstants.CHARACTERS;
import static javax.xml.stream.XMLStreamConstants.COMMENT;
import static javax.xml.stream.XMLStreamConstants.END_DOCUMENT;
import static javax.xml.stream.XMLStreamConstants.END_ELEMENT;
import static javax.xml.stream.XMLStreamConstants.PROCESSING_INSTRUCTION;
import static javax.xml.stream.XMLStreamConstants.SPACE;
import static javax.xml.stream.XMLStreamConstants.START_ELEMENT;
import static org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator.TRANSLATE_OPTION.ADD_MISSING;
import static reactor.core.publisher.Flux.fromIterable;

import com.google.common.collect.Maps;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.management.modelmbean.XMLParseException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.osm.OSM;
import org.heigit.ohsome.oshdb.osm.OSMCoordinates;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMMember;
import org.heigit.ohsome.oshdb.osm.OSMNode;
import org.heigit.ohsome.oshdb.osm.OSMRelation;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.osm.OSMWay;
import org.heigit.ohsome.oshdb.source.OSMSource;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBException;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMRole;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class OscParser implements OSMSource, AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(OscParser.class);

  private final InputStream inputStream;

  public OscParser(InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public Flux<OSMEntity> entities(TagTranslator tagTranslator) {
    try (var parser = new Parser(inputStream)) {
      @SuppressWarnings("UnstableApiUsage")
      var entities = stream(parser).collect(toList());
      LOG.info("osc entities: {}, strings: {}, tags: {}, roles: {}",
          entities.size(),
          parser.cacheString.size(),
          parser.cacheTags.size(),
          parser.cacheRoles.size());

      var tagsMapping = tagsMapping(tagTranslator, parser);
      var rolesMapping = rolesMapping(tagTranslator, parser);

      return fromIterable(entities).map(osm -> map(osm, tagsMapping, rolesMapping));
    } catch (Exception e) {
      throw new OSHDBException(e);
    }
  }

  private Map<OSHDBTag, OSHDBTag> tagsMapping(TagTranslator tagTranslator, Parser parser) {
    var tags = parser.cacheTags;
    var tagsTranslated = tagTranslator.getOSHDBTagOf(tags.values(), ADD_MISSING);
    var tagsMapping = Maps.<OSHDBTag, OSHDBTag>newHashMapWithExpectedSize(tags.size());
    tags.forEach((oshdb, osm) -> tagsMapping.put(oshdb, tagsTranslated.get(osm)));
    return tagsMapping;
  }

  private Map<Integer, Integer> rolesMapping(TagTranslator tagTranslator, Parser parser) {
    var roles = parser.cacheRoles;
    var rolesTranslated = tagTranslator.getOSHDBRoleOf(roles.values(), ADD_MISSING);
    var rolesMapping = Maps.<Integer, Integer>newHashMapWithExpectedSize(roles.size());
    roles.forEach((oshdb, osm) -> rolesMapping.put(oshdb, rolesTranslated.get(osm).getId()));
    return rolesMapping;
  }

  private OSMEntity map(OSMEntity osm, Map<OSHDBTag, OSHDBTag> tagsMapping, Map<Integer, Integer> rolesMapping) {
    var tags = osm.getTags().stream().map(tagsMapping::get).sorted().collect(toList());
    if (osm instanceof OSMNode) {
      var node = (OSMNode) osm;
      return OSM.node(osm.getId(), version(osm.getVersion(), osm.isVisible()), osm.getEpochSecond(), osm.getChangesetId(),osm.getUserId(), tags, node.getLon(), node.getLat());
    } else if (osm instanceof OSMWay) {
      var way = (OSMWay) osm;
      return OSM.way(osm.getId(), version(osm.getVersion(), osm.isVisible()), osm.getEpochSecond(), osm.getChangesetId(),osm.getUserId(), tags, way.getMembers());
    } else {
      var relation = (OSMRelation) osm;
      var members = Arrays.stream(relation.getMembers()).map(mem -> new OSMMember(mem.getId(), mem.getType(), rolesMapping.get(mem.getRole().getId()))).toArray(OSMMember[]::new);
      return OSM.relation(osm.getId(), version(osm.getVersion(), osm.isVisible()), osm.getEpochSecond(), osm.getChangesetId(),osm.getUserId(), tags, members);
    }
  }

  private static int version(int version, boolean visible) {
    return visible ? version : -version;
  }

  @Override
  public void close() throws Exception {
    inputStream.close();
  }

  private static class Parser implements Iterator<OSMEntity>, AutoCloseable {

    private static final XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();

    private final XMLStreamReader reader;

    private long id = -1;
    private int version = -1;
    private long timestamp = -1;
    private long changeset = -1;
    private int uid = -1;
    private String user = "";
    private boolean visible = false;
    private final List<OSMTag> tags = new ArrayList<>();
    private final List<Mem> members = new ArrayList<>();

    private final Map<String, Integer> cacheString = new HashMap<>();
    private final Map<OSHDBTag, OSMTag> cacheTags = new HashMap<>();
    private final Map<Integer, OSMRole> cacheRoles = new HashMap<>();

    private Exception exception = null;
    private OSMEntity next = null;
    private double lon;
    private double lat;

    Parser(InputStream input) throws XMLStreamException, XMLParseException {
      this.reader = xmlInputFactory.createXMLStreamReader(input, "UTF8");

      var eventType = reader.nextTag();
      if (eventType != START_ELEMENT) {
        throw new XMLParseException("start of element");
      }
      var localName = reader.getLocalName();
      if (!"osmChange".equals(localName)) {
        throw new XMLParseException(format("expecting tag(osmChange) but got %s", localName));
      }
      openChangeContainer();
    }

    private int[] tags(List<OSMTag> osmTags) {
      var kvs = new int[osmTags.size() * 2];
      var i = 0;
      for (var tag : osmTags) {
        var keyId = cacheString.computeIfAbsent(tag.getKey(), x -> cacheString.size());
        var valId = cacheString.computeIfAbsent(tag.getValue(), x -> cacheString.size());
        cacheTags.put(new OSHDBTag(keyId, valId), tag);
        kvs[i++] = keyId;
        kvs[i++] = valId;
      }
      return kvs;
    }

    private int lonLatConversion(double d) {
      return (int) (d * OSMCoordinates.GEOM_PRECISION_TO_LONG);
    }

    protected OSMMember[] members(List<Mem> mems) {
      var osmMembers = new OSMMember[mems.size()];
      var i = 0;
      for (var mem : mems) {
        var roleId = cacheString.computeIfAbsent(mem.getRole().toString(), x -> cacheString.size());
        cacheRoles.put(roleId, mem.getRole());
        osmMembers[i++] = new OSMMember(mem.getId(), mem.getType(), roleId);
      }
      return osmMembers;
    }

    private boolean openChangeContainer() throws XMLParseException, XMLStreamException {
      int eventType = nextEvent(reader);
      if (eventType == END_ELEMENT || eventType == END_DOCUMENT) {
        return false;
      }
      if (eventType != START_ELEMENT) {
        throw new XMLParseException("start of element");
      }

      var localName = reader.getLocalName();
      if ("create".equals(localName) || "modify".equals(localName)) {
        visible = true;
      } else if ("delete".equals(localName)) {
        visible = false;
      } else {
        throw new XMLParseException("expecting tag (create/modify/delete) but got " + localName);
      }
      return true;
    }

    private void parseAttributes() throws XMLParseException {
      var attributeCount = reader.getAttributeCount();
      for (int i = 0; i < attributeCount; i++) {
        var attrName = reader.getAttributeLocalName(i);
        var attrValue = reader.getAttributeValue(i);
        if ("id".equals(attrName)) {
          id = Long.parseLong(attrValue);
        } else if ("version".equals(attrName)) {
          version = Integer.parseInt(attrValue);
        } else if ("timestamp".equals(attrName)) {
          timestamp = Instant.parse(attrValue).getEpochSecond();
        } else if ("uid".equals(attrName)) {
          uid = Integer.parseInt(attrValue);
        } else if ("user".equals(attrName)) {
          user = attrValue;
        } else if ("changeset".equals(attrName)) {
          changeset = Long.parseLong(attrValue);
        } else if ("lon".equals(attrName)) {
          lon = Double.parseDouble(attrValue);
        } else if ("lat".equals(attrName)) {
          lat = Double.parseDouble(attrValue);
        } else {
          throw new XMLParseException("unknown attribute: " + attrName);
        }
      }
    }

    private void parseTag() throws XMLParseException {
      String key = null;
      String value = null;
      int attributeCount = reader.getAttributeCount();
      for (int i = 0; i < attributeCount; i++) {
        var attrName = reader.getAttributeLocalName(i);
        var attrValue = reader.getAttributeValue(i);
        if ("k".equals(attrName)) {
          key = attrValue;
        } else if ("v".equals(attrName)) {
          value = attrValue;
        } else {
          unknownAttribute(attrName);
        }
      }

      if (key == null || value == null) {
        throw new XMLParseException(format("missing key(%s) or value(%s)", key, value));
      }
      tags.add(new OSMTag(key, value));
    }

    private static void unknownAttribute(String attrName) throws XMLParseException {
      throw new XMLParseException(format("unknown attribute: %s", attrName));
    }

    private void parseWayMember() throws XMLParseException {
      var memberId = -1L;
      var attributeCount = reader.getAttributeCount();
      for (int i = 0; i < attributeCount; i++) {
        var attrName = reader.getAttributeLocalName(i);
        var attrValue = reader.getAttributeValue(i);
        if ("ref".equals(attrName)) {
          memberId = Long.parseLong(attrValue);
        } else {
          unknownAttribute(attrName);
        }
      }
      if (memberId < 0) {
        throw new XMLParseException("missing member id!");
      }
      members.add(new Mem(memberId));
    }

    private void parseMember() throws XMLParseException {
      String type = null;
      long ref = -1;
      String role = null;
      var attributeCount = reader.getAttributeCount();
      for (int i = 0; i < attributeCount; i++) {
        var attrName = reader.getAttributeLocalName(i);
        var attrValue = reader.getAttributeValue(i);
        if ("type".equals(attrName)) {
          type = attrValue;
        } else if ("ref".equals(attrName)) {
          ref = Long.parseLong(attrValue);
        } else if ("role".equals(attrName)) {
          role = attrValue;
        } else {
          unknownAttribute(attrName);
        }
      }
      if (type == null || ref < 0 || role == null) {
        throw new XMLParseException(format("missing member attribute (%s,%d,%s)", type, ref, role));
      }
      members.add(new Mem(OSMType.valueOf(type.toUpperCase()), ref, role));
    }

    private void parseEntity() throws XMLStreamException, XMLParseException {
      id = timestamp = changeset = uid = version = -1;
      user = "";
      lon = lat = -999.9;
      tags.clear();
      members.clear();

      parseAttributes();
      int eventType;
      while ((eventType = reader.nextTag()) == START_ELEMENT) {
        String localName = reader.getLocalName();
        if ("tag".equals(localName)) {
          parseTag();
        } else if ("nd".equals(localName)) {
          parseWayMember();
        } else if ("member".equals(localName)) {
          parseMember();
        } else {
          throw new XMLParseException("unexpected tag, expect tag/nd/member but got " + localName);
        }
        eventType = reader.nextTag();
        if (eventType != END_ELEMENT) {
          throw new XMLParseException("unclosed " + localName);
        }
      }
      if (eventType != END_ELEMENT) {
        throw new XMLParseException(format("expect tag end but got %s", eventType));
      }
    }

    private OSMNode nextNode() throws XMLParseException, XMLStreamException {
      parseEntity();
      if (visible && !validCoordinate(lon, lat)) {
        throw new XMLParseException(format("invalid coordinates! lon:%f lat:%f", lon, lat));
      }

      LOG.debug("node/{} {} {} {} {} {} {} {} {} {}", id, version, visible, timestamp, changeset, user, uid, tags, lon, lat);
      return OSM.node(id, version(version, visible), timestamp, changeset, uid, tags(tags),
          lonLatConversion(lon), lonLatConversion(lat));
    }

    private boolean validCoordinate(double lon, double lat) {
      return Math.abs(lon) <= 180.0 && Math.abs(lat) <= 90.0;
    }

    private OSMWay nextWay() throws XMLStreamException, XMLParseException {
      parseEntity();
      LOG.debug("way/{} {} {} {} {} {} {} {} {}", id, version, visible, timestamp, changeset, user, uid, tags, members.size());
      return OSM.way(id, version(version, visible), timestamp, changeset, uid, tags(tags), members(members));
    }

    private OSMRelation nextRelation() throws XMLStreamException, XMLParseException {
      parseEntity();
      LOG.debug("relation/{} {} {} {} {} {} {} {} mems:{}", id, version, visible, timestamp, changeset, user, uid, tags, members.size());
      return OSM.relation(id, version(version, visible), timestamp, changeset, uid, tags(tags),
          members(members));
    }

    private OSMEntity computeNext() {
      try {
        var eventType = nextEvent(reader);
        if (eventType == END_DOCUMENT) {
          return null;
        }

        if (eventType == END_ELEMENT) {
          if (!openChangeContainer()) {
            return null;
          }
          eventType = reader.nextTag();
        }
        if (eventType != START_ELEMENT) {
          throw new XMLParseException("expecting start of (node/way/relation)");
        }
        String localName = reader.getLocalName();
        if ("node".equals(localName)) {
          return nextNode();
        } else if ("way".equals(localName)) {
          return nextWay();
        } else if ("relation".equals(localName)) {
          return nextRelation();
        }
        throw new XMLParseException(format("expecting (node/way/relation) but got %s", localName));
      } catch (Exception e) {
        this.exception = e;
      }
      return null;
    }

    @Override
    public boolean hasNext() {
      return (next != null) || (next = computeNext()) != null;
    }

    @Override
    public OSMEntity next() {
      if (!hasNext()) {
        throw new NoSuchElementException((exception == null ? null : exception.toString()));
      }
      var r = next;
      next = null;
      return r;
    }

    private int nextEvent(XMLStreamReader reader) throws XMLStreamException {
      while (true) {
        var event = reader.next();

        switch (event) {
          case SPACE:
          case COMMENT:
          case PROCESSING_INSTRUCTION:
          case CDATA:
          case CHARACTERS:
            continue;

          case START_ELEMENT:
          case END_ELEMENT:
          case END_DOCUMENT:
            return event;
          default:
            throw new XMLStreamException(format(
                "Received event %d, instead of START_ELEMENT or END_ELEMENT or END_DOCUMENT.",
                event));
        }
      }
    }

    @Override
    public void close() throws Exception {
      reader.close();
    }
  }

  private static class Mem {

    private final OSMType type;
    private final long id;
    private final OSMRole role;

    public Mem(OSMType type, long id, String role) {
      this.type = type;
      this.id = id;
      this.role = new OSMRole(role);
    }

    public Mem(long id) {
      this(OSMType.NODE, id, "");
    }

    public OSMType getType() {
      return type;
    }

    public long getId() {
      return id;
    }

    public OSMRole getRole() {
      return role;
    }

    @Override
    public String toString() {
      return String.format("%s/%s[%s]", type, id, role);
    }
  }
}
