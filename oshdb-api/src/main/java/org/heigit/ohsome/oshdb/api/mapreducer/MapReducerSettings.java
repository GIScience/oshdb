package org.heigit.ohsome.oshdb.api.mapreducer;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.filter.Filter;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.function.OSMEntityFilter;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTagKey;
import org.jetbrains.annotations.Contract;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;

/**
 * Interface defining the common setting methods found on MapReducer or MapAggregator objects.
 *
 * @param <M> the class returned by all setting methods
 */
interface MapReducerSettings<M> {

  /**
   * Set the area of interest to the given bounding box.
   * Only objects inside or clipped by this bbox will be passed on to the analysis'
   * `mapper` function.
   *
   * @param bboxFilter the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M areaOfInterest(OSHDBBoundingBox bboxFilter);

  /**
   * Set the area of interest to the given polygon.
   * Only objects inside or clipped by this polygon will be passed on to the analysis'
   * `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  <P extends Geometry & Polygonal> M areaOfInterest(P polygonFilter);

  /**
   * Apply a textual filter to this query.
   *
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#syntax">oshdb-filter
   *      readme</a> for a description of the filter syntax.
   *
   * @param f the filter string to apply
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  M filter(String f);

  /**
   * Apply a custom filter expression to this query.
   *
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#readme">oshdb-filter
   *      readme</a> and {@link org.heigit.ohsome.oshdb.filter} for further information about how
   *      to create such a filter expression object.
   *
   * @param f the {@link org.heigit.ohsome.oshdb.filter.FilterExpression} to apply
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  M filter(FilterExpression f);

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param typeFilter the set of osm types to filter (e.g. `EnumSet.of(OSMType.WAY)`)
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Deprecated(since = "0.7.0", forRemoval = true)
  M osmType(Set<OSMType> typeFilter);

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param type1 the set of osm types to filter (e.g. `OSMType.NODE`)
   * @param otherTypes more osm types which should be analyzed
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Deprecated(since = "0.7.0", forRemoval = true)
  default M osmType(OSMType type1, OSMType... otherTypes) {
    return osmType(EnumSet.of(type1, otherTypes));
  }

  /**
   * Adds a custom arbitrary filter that gets executed for each osm entity and determines if
   * it should be considered for this analyis or not.
   *
   * @param f the filter function to call for each osm entity
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(FilterExpression)} with {@link
   *             org.heigit.ohsome.oshdb.filter.Filter#byOSMEntity(Filter.SerializablePredicate)}
   *             instead
   */
  @Deprecated(since = "0.7.0", forRemoval = true)
  M osmEntityFilter(OSMEntityFilter f);

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have
   * this tag key (with an arbitrary value).
   *
   * @param key the tag key to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Deprecated(since = "0.7.0", forRemoval = true)
  M osmTag(String key);

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have
   * this tag key (with an arbitrary value), or this tag key and value.
   *
   * @param tag the tag (key, or key and value) to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Deprecated(since = "0.7.0", forRemoval = true)
  M osmTag(OSMTagInterface tag);

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have
   * this tag key and value.
   *
   * @param key the tag key to filter the osm entities for
   * @param value the tag value to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Deprecated(since = "0.7.0", forRemoval = true)
  M osmTag(String key, String value);

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have
   * this tag key and one of the given values.
   *
   * @param key the tag key to filter the osm entities for
   * @param values an array of tag values to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Deprecated(since = "0.7.0", forRemoval = true)
  M osmTag(String key, Collection<String> values);

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have
   * a tag with the given key and whose value matches the given regular expression pattern.
   *
   * @param key the tag key to filter the osm entities for
   * @param valuePattern a regular expression which the tag value of the osm entity must match
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  M osmTag(String key, Pattern valuePattern);

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have at least one
   * of the supplied tags (key=value pairs or key=*).
   *
   * @param keyValuePairs the tags (key/value pairs or key=*) to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Deprecated(since = "0.7.0", forRemoval = true)
  M osmTag(Collection<? extends OSMTagInterface> keyValuePairs);

  /** deprecated.
   *
   * @deprecated replaced by {@link #osmType(Set)}
   */
  @Deprecated(since = "0.5.0", forRemoval = true)
  default M osmTypes(EnumSet<OSMType> typeFilter) {
    return this.osmType(typeFilter);
  }

  /** deprecated.
   *
   * @deprecated replaced by {@link #osmType(OSMType, OSMType...)}
   */
  @Deprecated(since = "0.5.0", forRemoval = true)
  default M osmTypes(OSMType type1, OSMType... otherTypes) {
    return this.osmType(type1, otherTypes);
  }

  /** deprecated.
   *
   * @deprecated replaced by {@link #osmEntityFilter}
   */
  @Deprecated(since = "0.5.0", forRemoval = true)
  default M where(OSMEntityFilter f) {
    return this.osmEntityFilter(f);
  }

  /** deprecated.
   *
   * @deprecated replaced by {@link #osmTag(OSMTagInterface)}
   */
  @Deprecated(since = "0.5.0", forRemoval = true)
  default M where(OSMTagKey key) {
    return this.osmTag(key);
  }

  /** deprecated.
   *
   * @deprecated replaced by {@link #osmTag(String)}
   */
  @Deprecated(since = "0.5.0", forRemoval = true)
  default M where(String key) {
    return this.osmTag(key);
  }

  /** deprecated.
   *
   * @deprecated replaced by {@link #osmTag(OSMTagInterface)}
   */
  @Deprecated(since = "0.5.0", forRemoval = true)
  default M where(OSMTag tag) {
    return this.osmTag(tag);
  }

  /** deprecated.
   *
   * @deprecated replaced by {@link #osmTag(String, String)}
   */
  @Deprecated(since = "0.5.0", forRemoval = true)
  default M where(String key, String value) {
    return this.osmTag(key, value);
  }

  /** deprecated.
   *
   * @deprecated replaced by {@link #osmTag(String, Collection)}
   */
  @Deprecated(since = "0.5.0", forRemoval = true)
  default M where(String key, Collection<String> values) {
    return this.osmTag(key, values);
  }

  /** deprecated.
   *
   * @deprecated replaced by {@link #osmTag(String, Pattern)}
   */
  @Deprecated(since = "0.5.0", forRemoval = true)
  default M where(String key, Pattern valuePattern) {
    return this.osmTag(key, valuePattern);
  }

  /** deprecated.
   *
   * @deprecated replaced by {@link #osmTag(Collection)}
   */
  @Deprecated(since = "0.5.0", forRemoval = true)
  default M where(Collection<OSMTag> keyValuePairs) {
    return this.osmTag(keyValuePairs);
  }
}
