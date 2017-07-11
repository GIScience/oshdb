package org.heigit.bigspatialdata.oshdb.osm;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.json.simple.parser.ParseException;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

public abstract class OSMEntity {

  protected final long id;

  protected final int version;
  protected final long timestamp;
  protected final long changeset;
  protected final int userId;
  protected final int[] tags;

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

  public abstract int getType();

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
    return String.format("ID:%d V:+%d+ TS:%d CS:%d VIS:%s USER:%d TAGS:%S", id, getVersion(), getTimestamp(),
            getChangeset(), isVisible(), getUserId(), Arrays.toString(getTags()));
  }

  public String toGeoJSON() throws IOException, ParseException {
    //JSON for properties
    JsonObjectBuilder properties = Json.createObjectBuilder().add("version", this.version).add("changeset", this.changeset).add("timestamp", this.timestamp).add("userid", this.userId);
    for (int i = 0; i < this.tags.length; i += 2) {
      properties.add(Integer.toString(this.tags[i]), this.tags[i + 1]);
    }
    //JSON for geometry
    GeoJSONWriter writer = new GeoJSONWriter();
    GeoJSON json = writer.write(this.getGeometry(1L, new TagInterpreter(1,1,new HashMap<>(),new HashMap<>(), new HashSet<>(),1,1,1)));
    JsonReader jsonReader = Json.createReader(new StringReader(json.toString()));
    JsonObject geom = jsonReader.readObject();
    

    String result = Json.createObjectBuilder().add("type", "Feature").add("id", this.id).add("properties", properties).add("geometry",geom).build().toString();
    return result;

  }

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
