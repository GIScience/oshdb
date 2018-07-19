package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializablePredicate;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;

import java.util.Collection;
import java.util.EnumSet;
import java.util.regex.Pattern;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;

/**
 * Interface defining the common setting methods found on MapReducer or MapAggregator objects
 *
 * @param <M> the class returned by all setting methods
 */
interface MapReducerSettings<M> {

  /**
   * Set the area of interest to the given bounding box.
   * Only objects inside or clipped by this bbox will be passed on to the analysis' `mapper` function.
   *
   * @param bboxFilter the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M areaOfInterest(OSHDBBoundingBox bboxFilter);

  /**
   * Set the area of interest to the given polygon.
   * Only objects inside or clipped by this polygon will be passed on to the analysis' `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  <P extends Geometry & Polygonal> M areaOfInterest(P polygonFilter);

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param typeFilter the set of osm types to filter (e.g. `EnumSet.of(OSMType.WAY)`)
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M osmType(EnumSet<OSMType> typeFilter);

  /**
   * replaced by {@link #osmType(OSMType, OSMType...)}
   */
  @Deprecated
  default M osmTypes(EnumSet<OSMType> typeFilter) {
    return this.osmType(typeFilter);
  }

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param type1 the set of osm types to filter (e.g. `OSMType.NODE`)
   * @param otherTypes more osm types which should be analyzed
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  default M osmType(OSMType type1, OSMType... otherTypes) {
    return osmType(EnumSet.of(type1, otherTypes));
  }

  /**
   * replaced by {@link #osmType(OSMType, OSMType...)}
   */
  @Deprecated
  default M osmTypes(OSMType type1, OSMType... otherTypes) {
    return this.osmType(type1, otherTypes);
  }

  /**
   * Adds a custom arbitrary filter that gets executed for each osm entity and determines if it should be considered for this analyis or not.
   *
   * @param f the filter function to call for each osm entity
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M osmEntityFilter(SerializablePredicate<OSMEntity> f);

  /**
   * @deprecated replaced by {@link #osmEntityFilter}
   */
  @Deprecated
  default M where(SerializablePredicate<OSMEntity> f) {
    return this.osmEntityFilter(f);
  }

  /**
   * @deprecated replaced by {@link #osmTag(OSMTagKey)}
   */
  @Deprecated
  default M where(OSMTagKey key) {
    return this.osmTag(key);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key (with an arbitrary value).
   *
   * @param key the tag key to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M osmTag(String key);

  /**
   * @deprecated replaced by {@link #osmTag(String)}
   */
  @Deprecated
  default M where(String key) {
    return this.osmTag(key);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * (with an arbitrary value), or this tag key and value.
   *
   * @param tag the tag (key, or key and value) to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M osmTag(OSMTagInterface tag);

  /**
   * @deprecated replaced by {@link #osmTag(OSMTag)}
   */
  @Deprecated
  default M where(OSMTag tag) {
    return this.osmTag(tag);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key and value.
   *
   * @param key the tag key to filter the osm entities for
   * @param value the tag value to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M osmTag(String key, String value);

  /**
   * @deprecated replaced by {@link #osmTag(String, String)}
   */
  @Deprecated
  default M where(String key, String value) {
    return this.osmTag(key, value);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key and one of the
   * given values.
   *
   * @param key the tag key to filter the osm entities for
   * @param values an array of tag values to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M osmTag(String key, Collection<String> values);

  /**
   * @deprecated replaced by {@link #osmTag(String, Collection<String>)}
   */
  @Deprecated
  default M where(String key, Collection<String> values) {
    return this.osmTag(key, values);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have a tag with the given key and
   * whose value matches the given regular expression pattern.
   *
   * @param key the tag key to filter the osm entities for
   * @param valuePattern a regular expression which the tag value of the osm entity must match
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M osmTag(String key, Pattern valuePattern);

  /**
   * @deprecated replaced by {@link #osmTag(String, Pattern)}
   */
  @Deprecated
  default M where(String key, Pattern valuePattern) {
    return this.osmTag(key, valuePattern);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have at least one of the supplied
   * tags (key=value pairs)
   *
   * @param keyValuePairs the tags (key/value pairs) to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M osmTag(Collection<OSMTag> keyValuePairs);

  /**
   * @deprecated replaced by {@link #osmTag(Collection)}
   */
  @Deprecated
  default M where(Collection<OSMTag> keyValuePairs) {
    return this.osmTag(keyValuePairs);
  }
}
