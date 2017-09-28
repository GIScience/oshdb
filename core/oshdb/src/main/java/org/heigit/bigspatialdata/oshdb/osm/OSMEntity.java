package org.heigit.bigspatialdata.oshdb.osm;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.util.*;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

public abstract class OSMEntity {
  protected static final Logger LOG = LoggerFactory.getLogger(OSMEntity.class);

  protected final long id;

  protected final int version;
  protected final long timestamp;
  protected final long changeset;
  protected final int userId;
  protected final int[] tags;

  /**
   * Constructor for a OSMEntity. Holds the basic information, every OSM-Object
   * has.
   *
   * @param id ID
   * @param version Version. Versions &lt;=0 define visible Entities, &gt;0
   * deleted Entities.
   * @param timestamp Timestamp in seconds since 01.01.1970 00:00:00 UTC.
   * @param changeset Changeset-ID
   * @param userId UserID. This is also the link to the UserName in the OSH-Db
   * (see
   * {@link org.heigit.bigspatialdata.oshdb.util.TagTranslator#TagTranslator(java.sql.Connection) TagTranslator})
   * @param tags An array of OSH-Db key-value ids. The format is
   * [KID1,VID1,KID2,VID2...KIDn,VIDn]. They can be translated to String and
   * back using the
   * {@link org.heigit.bigspatialdata.oshdb.util.TagTranslator#TagTranslator(java.sql.Connection) TagTranslator}.
   */
  public OSMEntity(final long id, final int version, final long timestamp, final long changeset, final int userId,
          final int[] tags) {
    this.id = id;
    this.version = version;
    this.timestamp = timestamp;
    this.changeset = changeset;
    this.userId = userId;
    this.tags = tags;
  }

  public long getId() {
    return id;
  }

  public abstract OSMType getType();

  public int getVersion() {
    return Math.abs(version);
  }

  public long getTimestamp() {
    return timestamp;
  }

  public long getChangeset() {
    return changeset;
  }

  public int getUserId() {
    return userId;
  }

  public boolean isVisible() {
    return (version >= 0);
  }

  public int[] getTags() {
    return tags;
  }

  public boolean hasTagKey(int key) {
    // todo: replace this with binary search (keys are sorted)
    for (int i = 0; i < tags.length; i += 2) {
      if (tags[i] < key) {
        continue;
      }
      if (tags[i] == key) {
        return true;
      }
      if (tags[i] > key) {
        return false;
      }
    }
    return false;
  }

  /**
   * Tests if any a given key is present but ignores certain values. Useful when
   * looking for example "TagKey" != "no"
   *
   * @param key the key to search for
   * @param uninterestingValues list of values, that should return false
   * although the key is actually present
   * @return true if the key is present and is NOT in a combination with the
   * given values, false otherwise
   */
  public boolean hasTagKey(int key, int[] uninterestingValues) {
    // todo: replace this with binary search (keys are sorted)
    for (int i = 0; i < tags.length; i += 2) {
      if (tags[i] < key) {
        continue;
      }
      if (tags[i] == key) {
        final int value = tags[i + 1];
        return !IntStream.of(uninterestingValues).anyMatch(x -> x == value);
      }
      if (tags[i] > key) {
        return false;
      }
    }
    return false;
  }

  public boolean hasTagValue(int key, int value) {
    // todo: replace this with binary search (keys are sorted)
    for (int i = 0; i < tags.length; i += 2) {
      if (tags[i] < key) {
        continue;
      }
      if (tags[i] == key) {
        return tags[i + 1] == value;
      }
      if (tags[i] > key) {
        return false;
      }
    }
    return false;
  }

  public boolean equalsTo(OSMEntity o) {
    return id == o.id && version == o.version && timestamp == o.timestamp && changeset == o.changeset
            && userId == o.userId && Arrays.equals(tags, o.tags);
  }

  @Override
  public String toString() {
    return String.format("ID:%d V:+%d+ TS:%d CS:%d VIS:%s UID:%d TAGS:%S", getId(), getVersion(), getTimestamp(),
            getChangeset(), isVisible(), getUserId(), Arrays.toString(getTags()));
  }

  /**
   * Returns a better String with translated tags.
   *
   * @param tagTranslator the TagTranslator to translate the Tags.
   * @return
   */
  public String toString(TagTranslator tagTranslator) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("ID:%d V:+%d+ TS:%d CS:%d VIS:%s UID:%d UName:%s TAGS:", getId(), getVersion(), getTimestamp(),
            getChangeset(), isVisible(), getUserId(), tagTranslator.usertoStr(getUserId())));
    sb.append("[");
    for (int i = 0; i < this.getTags().length; i += 2) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(tagTranslator.tag2String(new ImmutablePair(this.getTags()[i], this.getTags()[i + 1])));
    }
    sb.append("]");
    return sb.toString();

  }

  /**
   * This is the step before actually creating a GeoJSON-String. It holds the
   * option to interactively add some information (e.g. Object specific
   * statistics) before calling .build().toString() to display georeferenced
   * information e.g. in a web layer.
   *
   * @param timestamp The timestamp for which to create the geometry. NB: the
   * geometry will be created for exactly that point in time (see
   * {@link #getGeometry(long, org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter) getGeometry}).
   * @param tagtranslator a connection to a database to translate the coded
   * integer back to human readable string
   * @param areaDecider A list of tags, that define a polygon from a linestring.
   * A default one is available.
   * @return A GeoJSON representation of the Object
   * https://tools.ietf.org/html/rfc7946#section-3
   */
  protected JsonObjectBuilder toGeoJSONbuilder(long timestamp, TagTranslator tagtranslator, TagInterpreter areaDecider) {
    //JSON for properties
    JsonObjectBuilder properties;
    try {
      properties = Json.createObjectBuilder().add("visible", isVisible()).add("version", getVersion()).add("changeset", getChangeset()).add("timestamp", TimestampFormatter.getInstance().isoDateTime(getTimestamp())).add("user", tagtranslator.usertoStr(getUserId())).add("uid", getUserId());
      for (int i = 0; i < getTags().length; i += 2) {
        @SuppressWarnings("unchecked")
        Pair<String, String> tags = tagtranslator.tag2String(new ImmutablePair(getTags()[i], getTags()[i + 1]));
        properties.add(tags.getKey(), tags.getValue());
      }
    } catch (NullPointerException ex) {
      LOG.warn("The TagTranslator could not resolve the Tags. Therefore Integer values will be printed: {}", ex);
      properties = Json.createObjectBuilder().add("visible", isVisible()).add("version", getVersion()).add("changeset", getChangeset()).add("timestamp", TimestampFormatter.getInstance().isoDateTime(getTimestamp())).add("user", getUserId()).add("uid", getUserId());
      for (int i = 0; i < getTags().length; i += 2) {
        properties.add(Integer.toString(getTags()[i]), getTags()[i + 1]);
      }

    }
    //JSON for geometry
    GeoJSONWriter writer = new GeoJSONWriter();
    try {
      GeoJSON json = writer.write(this.getGeometry(timestamp, areaDecider));
      JsonReader jsonReader = Json.createReader(new StringReader(json.toString()));
      JsonObject geom = jsonReader.readObject();
      return Json.createObjectBuilder().add("type", "Feature").add("id", getId()).add("properties", properties).add("geometry", geom);
    } catch (NullPointerException ex) {
      LOG.warn("The geometry could not be built. Therefore it will not be part of the resulting JSON. {}", ex);
      return Json.createObjectBuilder().add("type", "Feature").add("id", getId()).add("properties", properties);
    }
  }

  /**
   * Convert multiple features at once. Calls toGeoJSON for each feature but
   * puts them in a FeatureCollection so you can see the all at once in your
   * GIS.
   *
   * @param osmObjects A list of pairs, the left being the OSMEntity to convert
   * and the right being the point in time to use. So this leaves you with the
   * option to get an overview of all versions of one object, as fine grained as
   * you wish.
   * @param tagtranslator a connection to a database to translate the coded
   * integer back to human readable string
   * @param areaDecider A list of tags, that define a polygon from a linestring.
   * A default one is available.
   * @return A GeoJSON-String representation of all these OSM-Objects
   * (https://tools.ietf.org/html/rfc7946#section-3.3
   */
  public static String toGeoJSON(List<Pair<? extends OSMEntity, Long>> osmObjects, TagTranslator tagtranslator, TagInterpreter areaDecider) {
    JsonObjectBuilder builder = Json.createObjectBuilder().add("type", "FeatureCollection");
    JsonArrayBuilder aBuilder = Json.createArrayBuilder();
    osmObjects.stream().forEach((Pair<? extends OSMEntity, Long> OSMObject) -> {
      aBuilder.add(OSMObject.getKey().toGeoJSONbuilder(OSMObject.getValue(), tagtranslator, areaDecider));
    });
    builder.add("features", aBuilder);
    return builder.build().toString();
  }

  /**
   * Get a GIS-compatible String version of your OSM-Object.
   *
   * @param timestamp The timestamp for which to create the geometry. NB: the
   * geometry will be created for exactly that point in time (see
   * this.getGeometry()).
   * @param tagtranslator a connection to a database to translate the coded
   * integer back to human readable string
   * @param areaDecider A list of tags, that define a polygon from a linestring.
   * A default one is available.
   * @return A string representation of the Object in GeoJSON-format
   * (https://tools.ietf.org/html/rfc7946#section-3.3)
   */
  public abstract String toGeoJSON(long timestamp, TagTranslator tagtranslator, TagInterpreter areaDecider);

  // helpers to determine underlying structure of osm objects
  // returns true if object is only used to define another object (e.g. nodes of a way without own tags)
  public abstract boolean isAuxiliary(Set<Integer> uninterestingTagKeys);
  // geometry: does it represent a point/line/polygon feature?

  public abstract boolean isPoint();

  public abstract boolean isPointLike(TagInterpreter areaDecider);

  public abstract boolean isArea(TagInterpreter areaDecider);

  public abstract boolean isLine(TagInterpreter areaDecider);

  // gets the geometry of this object at a specific timestamp
  public abstract Geometry getGeometry(long timestamp, TagInterpreter areaDecider);

  public Geometry getGeometryClipped(long timestamp, TagInterpreter areaDecider, BoundingBox clipBbox) {
    Geometry geom = this.getGeometry(timestamp, areaDecider);
    if (geom == null) {
      return null;
    }
    return Geo.clip(geom, clipBbox);
  }

  public Geometry getGeometryClipped(long timestamp, TagInterpreter areaDecider, Polygon clipPoly) {
    Geometry geom = this.getGeometry(timestamp, areaDecider);
    if (geom == null) {
      return null;
    }
    return Geo.clip(geom, clipPoly);
  }

}
