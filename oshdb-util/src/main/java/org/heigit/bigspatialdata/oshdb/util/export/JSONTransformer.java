package org.heigit.bigspatialdata.oshdb.util.export;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.time.TimestampFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

public class JSONTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(JSONTransformer.class);

  /**
   * Get a GIS-compatible Transformation of your OSM-Object.
   *
   * @param <T>
   * @param entity The OSM-Entity to transform
   * @param timestamp The timestamp for which to create the geometry. NB: the geometry will be
   *        created for exactly that point in time (see this.getGeometry()).
   * @param tagtranslator a connection to a database to translate the coded integer back to human
   *        readable string
   * @param areaDecider A list of tags, that define a polygon from a linestring. A default one is
   *        available.
   * @return A GeoJSON representation of the Object
   *         (https://tools.ietf.org/html/rfc7946#section-3.3)
   */
  public static <T extends OSMEntity> JsonObject transform(T entity, OSHDBTimestamp timestamp,
      TagTranslator tagtranslator, TagInterpreter areaDecider) {
    // JSON for properties
    JsonObjectBuilder properties = Json.createObjectBuilder();
    JsonObjectBuilder result = Json.createObjectBuilder();
    StringBuilder jsonid = new StringBuilder(24);

    // add type
    if (entity instanceof OSMNode) {
      properties.add("@type", "node");
      jsonid.append("node");
    } else if (entity instanceof OSMWay) {
      properties.add("@type", "way");
      jsonid.append("way");
    } else if (entity instanceof OSMRelation) {
      properties.add("@type", "relation");
      jsonid.append("relation");
    } else {
      properties.add("@type", "unknown");
      jsonid.append("unknowntype");
    }

    // add other simple meta attributes
    properties.add("@id", entity.getId()).add("@visible", entity.isVisible())
        .add("@version", entity.getVersion()).add("@changeset", entity.getChangeset())
        .add("@timestamp", TimestampFormatter.getInstance().isoDateTime(entity.getTimestamp()))
        .add("@geomtimestamp", TimestampFormatter.getInstance().isoDateTime(timestamp));
    jsonid.append("/").append(entity.getId()).append("@")
        .append(TimestampFormatter.getInstance().isoDateTime(timestamp));

    properties.add("@uid", entity.getUserId());

    for (int i = 0; i < entity.getRawTags().length; i += 2) {
      properties = JSONTransformer.addRiskyKey(entity, entity.getRawTags()[i], entity.getRawTags()[i + 1],
          tagtranslator, properties);
    }

    // add instance specific attributes
    if (entity instanceof OSMWay) {
      JsonArrayBuilder nd = Json.createArrayBuilder();
      OSMWay way = (OSMWay) entity;
      for (OSMMember node : way.getRefs()) {
        nd.add(node.getId());
      }
      properties.add("refs", nd);

    } else if (entity instanceof OSMRelation) {
      JsonArrayBuilder JSONMembers = Json.createArrayBuilder();
      OSMRelation relation = (OSMRelation) entity;
      for (OSMMember mem : relation.getMembers()) {
        JsonObjectBuilder member = Json.createObjectBuilder();
        member.add("type", mem.getType().toString()).add("ref", mem.getId());
        try {
          member.add("role", tagtranslator.getOSMRoleOf(mem.getRoleId()).toString());
        } catch (NullPointerException ex) {
          LOG.warn(
              "The TagTranslator could not resolve the member role {} of a member of relation/{}",
              mem.getRawRoleId(), entity.getId());
          member.add("role", "<error: could not resolve>");
        }
        JSONMembers.add(member);
      }
      properties.add("members", JSONMembers);
    }

    // JSON for geometry
    GeoJSONWriter writer = new GeoJSONWriter();
    try {
      GeoJSON json = writer.write(OSHDBGeometryBuilder.getGeometry(entity, timestamp, areaDecider));
      JsonReader jsonReader = Json.createReader(new StringReader(json.toString()));
      JsonObject geom = jsonReader.readObject();
      result.add("type", "Feature").add("id", jsonid.toString()).add("properties", properties)
          .add("geometry", geom);
    } catch (NullPointerException ex) {
      LOG.warn("Could not build the geometry of entity {} at timestamp {}", entity.toString(),
          TimestampFormatter.getInstance().isoDateTime(timestamp), ex);
      result.add("type", "Feature").add("id", jsonid.toString()).add("properties", properties)
          .add("geometry", "<error: could not create geometry>");
    }

    return result.build();
  }

  private static <T extends OSMEntity> JsonObjectBuilder addRiskyKey(T entity, int key, int value,
      TagTranslator tagtranslator, JsonObjectBuilder properties) {

    try {
      OSMTag temptags = tagtranslator.getOSMTagOf(key, value);
      return properties.add(temptags.getKey(), temptags.getValue());
    } catch (NullPointerException ex) {
      try {
        LOG.warn("The TagTranslator could not resolve a value (ValueID: {}) of Entity {}", value,
            entity.toString(), ex);
        String tempkey = tagtranslator.getOSMTagKeyOf(key).toString();
        return properties.add(tempkey, "<error: could not resolve value>");
      } catch (NullPointerException ex2) {
        LOG.debug("The TagTranslator could ALSO not resolve the key (KeyID: {}) of Entity {}", key,
            entity.toString(), ex2);
        return properties.add("<error: could not resolve key>", "<error: could not resolve value>");
      }
    }
  }

  /**
   * Convert multiple features at once. Calls multiTransform for each feature but puts them in a
   * FeatureCollection, so you can see them all at once in your GIS.
   *
   * @param osmObjects A list of pairs, the left being the OSMEntity to convert and the right being
   *        the point in time to use. So this leaves you with the option to get an overview of all
   *        versions of one object, as fine grained as you wish.
   * @param tagtranslator a connection to a database to translate the coded integer back to human
   *        readable string
   * @param areaDecider A list of tags, that define a polygon from a linestring. A default one is
   *        available.
   * @return A GeoJSON-String representation of all these OSM-Objects
   *         (https://tools.ietf.org/html/rfc7946#section-3.3
   */
  public static JsonObject multiTransform(List<Pair<? extends OSMEntity, OSHDBTimestamp>> osmObjects,
      TagTranslator tagtranslator, TagInterpreter areaDecider) {
    JsonObjectBuilder builder = Json.createObjectBuilder().add("type", "FeatureCollection");
    JsonArrayBuilder aBuilder = Json.createArrayBuilder();
    osmObjects.stream().forEach((Pair<? extends OSMEntity, OSHDBTimestamp> OSMObject) -> {
      aBuilder.add(JSONTransformer.transform(OSMObject.getKey(), OSMObject.getValue(),
          tagtranslator, areaDecider));
    });
    builder.add("features", aBuilder);
    return builder.build();
  }

  /**
   * Get a GIS-compatible version of your OSH-Object. Note that this method uses the timestamps of
   * the underlying OSM-Entities to construct geometries.
   *
   * @param <T>
   * @param entity
   * @param tagtranslator a connection to a database to translate the coded integer back to human
   *        readable string
   * @param areaDecider A list of tags, that define a polygon from a linestring. A default one is
   *        available.
   * @return A string representation of the Object in GeoJSON-format
   *         (https://tools.ietf.org/html/rfc7946#section-3.3)
   */
  public static <T extends OSHEntity> JsonObject transform(T entity, TagTranslator tagtranslator,
      TagInterpreter areaDecider) {

    List<Pair<? extends OSMEntity, OSHDBTimestamp>> entities = new ArrayList<>(1);
    @SuppressWarnings("unchecked")
    Iterator<? extends OSMEntity> it = entity.iterator();
    while (it.hasNext()) {
      OSMEntity obj = it.next();
      entities.add(new ImmutablePair<>(obj, obj.getTimestamp()));
    }
    return JSONTransformer.multiTransform(entities, tagtranslator, areaDecider);
  }

  /**
   * Get a GIS-compatible version of your OSH-GridCell. Note that this method uses only the latest
   * version of the each underlying OSM-Entity to construct geometries.
   *
   * @param <T>
   * @param entity
   * @param tagtranslator a connection to a database to translate the coded integer back to human
   *        readable string
   * @param areaDecider A list of tags, that define a polygon from a linestring. A default one is
   *        available.
   * @return A string representation of the Object in GeoJSON-format
   *         (https://tools.ietf.org/html/rfc7946#section-3.3)
   */
  public static <T extends GridOSHEntity> JsonObject transform(T entity,
      TagTranslator tagtranslator, TagInterpreter areaDecider) {
    List<Pair<? extends OSMEntity, OSHDBTimestamp>> entities = new ArrayList<>(1);

    @SuppressWarnings("unchecked")
    Iterator<? extends OSHEntity> it = entity.iterator();
    while (it.hasNext()) {
      OSHEntity obj = it.next();
      entities.add(new ImmutablePair<>(obj.getLatest(), obj.getLatest().getTimestamp()));
    }
    return JSONTransformer.multiTransform(entities, tagtranslator, areaDecider);
  }

  private JSONTransformer() {}

}
