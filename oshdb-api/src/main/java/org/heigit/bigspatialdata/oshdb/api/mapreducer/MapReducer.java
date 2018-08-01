package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import com.google.common.collect.Iterables;
import java.sql.Connection;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.SpatialRelations.GEOMETRY_OPTIONS;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.celliterator.ContributionType;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.bigspatialdata.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTag;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.bigspatialdata.oshdb.util.tagtranslator.TagTranslator;
import com.vividsolutions.jts.geom.*;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBJdbc;
import org.heigit.bigspatialdata.oshdb.api.generic.*;
import org.heigit.bigspatialdata.oshdb.api.generic.function.*;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.util.geometry.Geo;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.time.ISODateTimeParser;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampList;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.*;
import org.heigit.bigspatialdata.oshdb.util.time.TimestampFormatter;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class of oshdb's "functional programming" API.
 *
 * It accepts a list of filters, transformation `map` functions a produces a result when calling the
 * `reduce` method (or one of its shorthand versions like `sum`, `count`, etc.).
 *
 * You can set a list of filters that are applied on the raw OSM data, for example you can filter:
 * <ul>
 * <li>geometrically by an area of interest (bbox or polygon)</li>
 * <li>by osm tags (key only or key/value)</li>
 * <li>by OSM type</li>
 * <li>custom filter callback</li>
 * </ul>
 *
 * Depending on the used data "view", the MapReducer produces either "snapshots" or evaluated all
 * modifications ("contributions") of the matching raw OSM data.
 *
 * These data can then be transformed arbitrarily by user defined `map` functions (which take one of
 * these entity snapshots or modifications as input an produce an arbitrary output) or `flatMap`
 * functions (which can return an arbitrary number of results per entity snapshot/contribution). It
 * is possible to chain together any number of transformation functions.
 *
 * Finally, one can either use one of the pre-defined result-generating functions (e.g. `sum`,
 * `count`, `average`, `uniq`), or specify a custom `reduce` procedure.
 *
 * If one wants to get results that are aggregated by timestamp (or some other index), one can use
 * the `aggregateByTimestamp` or `aggregateBy` functionality that automatically handles the grouping
 * of the output data.
 *
 * For more complex analyses, it is also possible to enable the grouping of the input data by the
 * respective OSM ID. This can be used to view at the whole history of entities at once.
 *
 * @param <X> the type that is returned by the currently set of mapper function. the next added
 *        mapper function will be called with a parameter of this type as input
 */
public abstract class MapReducer<X> implements
    MapReducerSettings<MapReducer<X>>, Mappable<X>, MapReducerAggregations<X>,
    MapAggregatable<MapAggregator<? extends Comparable<?>, X>, X>, Serializable
{

  private static final Logger LOG = LoggerFactory.getLogger(MapReducer.class);

  protected OSHDBDatabase _oshdb;
  protected transient OSHDBJdbc _oshdbForTags;

  // internal state
  Class<? extends OSHDBMapReducible> _forClass = null;

  enum Grouping {
    NONE, BY_ID
  }

  Grouping _grouping = Grouping.NONE;

  // utility objects
  private transient TagTranslator _tagTranslator = null;
  private TagInterpreter _tagInterpreter = null;

  // settings and filters
  protected OSHDBTimestampList _tstamps = new OSHDBTimestamps(
      "2008-01-01",
      TimestampFormatter.getInstance().date(new Date()),
      OSHDBTimestamps.Interval.MONTHLY);
  protected OSHDBBoundingBox _bboxFilter = new OSHDBBoundingBox(-180, -90, 180, 90);
  private Geometry _polyFilter = null;
  protected EnumSet<OSMType> _typeFilter = EnumSet.of(OSMType.NODE, OSMType.WAY, OSMType.RELATION);
  private final List<SerializablePredicate<OSHEntity>> _preFilters = new ArrayList<>();
  private final List<SerializablePredicate<OSMEntity>> _filters = new ArrayList<>();
  final List<SerializableFunction> _mappers = new LinkedList<>();
  private final Set<SerializableFunction> _flatMappers = new HashSet<>();

  // basic constructor
  protected MapReducer(OSHDBDatabase oshdb, Class<? extends OSHDBMapReducible> forClass) {
    this._oshdb = oshdb;
    this._forClass = forClass;
  }

  // copy constructor
  protected MapReducer(MapReducer<?> obj) {
    this._oshdb = obj._oshdb;
    this._oshdbForTags = obj._oshdbForTags;

    this._forClass = obj._forClass;
    this._grouping = obj._grouping;

    this._tagTranslator = obj._tagTranslator;
    this._tagInterpreter = obj._tagInterpreter;

    this._tstamps = obj._tstamps;
    this._bboxFilter = obj._bboxFilter;
    this._polyFilter = obj._polyFilter;
    this._typeFilter = obj._typeFilter.clone();
    this._preFilters.addAll(obj._preFilters);
    this._filters.addAll(obj._filters);
    this._mappers.addAll(obj._mappers);
    this._flatMappers.addAll(obj._flatMappers);
  }

  @NotNull
  protected abstract MapReducer<X> copy();

  // -----------------------------------------------------------------------------------------------
  // "Setting" methods and associated internal helpers
  // -----------------------------------------------------------------------------------------------

  /**
   * Sets the keytables database to use in the calculations to resolve strings (osm tags, roles)
   * into internally used identifiers. If this function is never called, the main database
   * (specified during the construction of this object) is used for this.
   *
   * @param keytablesOshdb the database to use for resolving strings into internal identifiers
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> keytables(OSHDBJdbc keytablesOshdb) {
    if (keytablesOshdb != this._oshdb && this._oshdb instanceof OSHDBJdbc) {
      Connection c = ((OSHDBJdbc) this._oshdb).getConnection();
      try {
        new TagTranslator(c);
        LOG.warn("It looks like as if the current OSHDB comes with keytables included. " +
            "Usually this means that you should use this file's keytables " +
            "and should not set the keytables manually.");
      } catch (OSHDBKeytablesNotFoundException e) {
        // this is the expected path -> the oshdb doesn't have the key tables
      }
    }
    MapReducer<X> ret = this.copy();
    ret._oshdbForTags = keytablesOshdb;
    return ret;
  }

  /**
   * Sets the tagInterpreter to use in the analysis. The tagInterpreter is used internally to
   * determine the geometry type of osm entities (e.g. an osm way can become either a LineString or
   * a Polygon, depending on its tags). Normally, this is generated automatically for the user. But
   * for example, if one doesn't want to use the DefaultTagInterpreter, it is possible to use this
   * function to supply their own tagInterpreter.
   *
   * @param tagInterpreter the tagInterpreter object to use in the processing of osm entities
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @SuppressWarnings("unused")
  @Contract(pure = true)
  public MapReducer<X> tagInterpreter(TagInterpreter tagInterpreter) {
    MapReducer<X> ret = this.copy();
    ret._tagInterpreter = tagInterpreter;
    return ret;
  }

  /**
   * Gets the tagInterpreter
   *
   * @return tagInterpreter the tagInterpreter object
   */
  @SuppressWarnings("unused")
  @Contract(pure = true)
  public TagInterpreter getTagInterpreter() {
    return this._tagInterpreter;
  }

  // -----------------------------------------------------------------------------------------------
  // Filtering methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Set the area of interest to the given bounding box. Only objects inside or clipped by this bbox
   * will be passed on to the analysis' `mapper` function.
   *
   * @param bboxFilter the bounding box to query the data in
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> areaOfInterest(@NotNull OSHDBBoundingBox bboxFilter) {
    MapReducer<X> ret = this.copy();
    if (this._polyFilter == null) {
      ret._bboxFilter = OSHDBBoundingBox.intersect(bboxFilter, ret._bboxFilter);
    } else {
      ret._polyFilter = Geo.clip(ret._polyFilter, bboxFilter);
      ret._bboxFilter = OSHDBGeometryBuilder.boundingBoxOf(ret._polyFilter.getEnvelopeInternal());
    }
    return ret;
  }

  /**
   * Set the area of interest to the given polygon. Only objects inside or clipped by this polygon
   * will be passed on to the analysis' `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public <P extends Geometry & Polygonal> MapReducer<X> areaOfInterest(@NotNull P polygonFilter) {
    MapReducer<X> ret = this.copy();
    if (this._polyFilter == null) {
      ret._polyFilter = Geo.clip(polygonFilter, ret._bboxFilter);
    } else {
      ret._polyFilter = Geo.clip(polygonFilter, ret._getPolyFilter());
    }
    ret._bboxFilter = OSHDBGeometryBuilder.boundingBoxOf(ret._polyFilter.getEnvelopeInternal());
    return ret;
  }

  /**
   * Set the timestamps for which to perform the analysis.
   *
   * Depending on the *View*, this has slightly different semantics: * For the OSMEntitySnapshotView
   * it will set the time slices at which to take the "snapshots" * For the OSMContributionView it
   * will set the time interval in which to look for osm contributions (only the first and last
   * timestamp of this list are contributing). Additionally, the timestamps are used in the
   * `aggregateByTimestamp` functionality.
   *
   * @param tstamps an object (implementing the OSHDBTimestampList interface) which provides the
   *        timestamps to do the analysis for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> timestamps(OSHDBTimestampList tstamps) {
    MapReducer<X> ret = this.copy();
    ret._tstamps = tstamps;
    return ret;
  }

  /**
   * Set the timestamps for which to perform the analysis in a regular interval between a start and
   * end date.
   *
   * See {@link #timestamps(OSHDBTimestampList)} for further information.
   *
   * @param isoDateStart an ISO 8601 date string representing the start date of the analysis
   * @param isoDateEnd an ISO 8601 date string representing the end date of the analysis
   * @param interval the interval between the timestamps to be used in the analysis
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> timestamps(
      String isoDateStart, String isoDateEnd, OSHDBTimestamps.Interval interval
  ) {
    return this.timestamps(new OSHDBTimestamps(isoDateStart, isoDateEnd, interval));
  }

  /**
   * Sets a single timestamp for which to perform the analysis at.
   *
   * Useful in combination with the OSMEntitySnapshotView when not performing further aggregation by
   * timestamp.
   *
   * See {@link #timestamps(OSHDBTimestampList)} for further information.
   *
   * @param isoDate an ISO 8601 date string representing the date of the analysis
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> timestamps(String isoDate) {
    if (this._forClass.equals(OSMContribution.class)) {
      LOG.warn("OSMContributionView requires two or more timestamps, but only one was supplied.");
    }
    return this.timestamps(isoDate, isoDate, new String[] {});
  }

  /**
   * Sets two timestamps (start and end date) for which to perform the analysis.
   *
   * Useful in combination with the OSMContributionView when not performing further aggregation by
   * timestamp.
   *
   * See {@link #timestamps(OSHDBTimestampList)} for further information.
   *
   * @param isoDateStart an ISO 8601 date string representing the start date of the analysis
   * @param isoDateEnd an ISO 8601 date string representing the end date of the analysis
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> timestamps(String isoDateStart, String isoDateEnd) {
    return this.timestamps(isoDateStart, isoDateEnd, new String[] {});
  }

  /**
   * Sets multiple arbitrary timestamps for which to perform the analysis.
   *
   * Note for programmers wanting to use this method to supply an arbitrary number (n>=1) of
   * timestamps: You may supply the same time string multiple times, which will be de-duplicated
   * internally. E.g. you can call the method like this:
   *   .timestamps(dateArr[0], dateArr[0], dateArr)
   *
   * See {@link #timestamps(OSHDBTimestampList)} for further information.
   *
   * @param isoDateFirst an ISO 8601 date string representing the start date of the analysis
   * @param isoDateSecond an ISO 8601 date string representing the second date of the analysis
   * @param isoDateMore more ISO 8601 date strings representing the remaining timestamps of the
   *        analysis
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> timestamps(String isoDateFirst, String isoDateSecond, String... isoDateMore) {
    SortedSet<OSHDBTimestamp> timestamps = new TreeSet<>();
    try {
      timestamps.add(new OSHDBTimestamp(ISODateTimeParser.parseISODateTime(isoDateFirst).toEpochSecond()));
      timestamps.add(new OSHDBTimestamp(ISODateTimeParser.parseISODateTime(isoDateSecond).toEpochSecond()));
      for (String isoDate : isoDateMore) {
        timestamps.add(new OSHDBTimestamp(ISODateTimeParser.parseISODateTime(isoDate).toEpochSecond()));
      }
    } catch (Exception e) {
      LOG.error("unable to parse ISO date string: " + e.getMessage());
    }
    return this.timestamps(() -> timestamps);
  }

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param typeFilter the set of osm types to filter (e.g. `EnumSet.of(OSMType.WAY)`)
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> osmType(EnumSet<OSMType> typeFilter) {
    MapReducer<X> ret = this.copy();
    ret._typeFilter = typeFilter;
    return ret;
  }

  /**
   * Adds a custom arbitrary filter that gets executed for each osm entity and determines if it
   * should be considered for this analyis or not.
   *
   * @param f the filter function to call for each osm entity
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> osmEntityFilter(SerializablePredicate<OSMEntity> f) {
    MapReducer<X> ret = this.copy();
    ret._filters.add(f);
    return ret;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * (with an arbitrary value).
   *
   * @param key the tag key to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> osmTag(String key) {
    return this.osmTag(new OSMTagKey(key));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * (with an arbitrary value), or this tag key and value.
   *
   * @param tag the tag (key, or key and value) to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> osmTag(OSMTagInterface tag) {
    if (tag instanceof OSMTag) return this._osmTag((OSMTag) tag);
    if (tag instanceof OSMTagKey) return this._osmTag((OSMTagKey) tag);
    throw new UnsupportedOperationException("Unknown object implementing OSMTagInterface.");
  }

    /**
     * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
     * (with an arbitrary value).
     *
     * @param key the tag key to filter the osm entities for
     * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
     */
  @Contract(pure = true)
  private MapReducer<X> _osmTag(OSMTagKey key) {
    MapReducer<X> ret = this.copy();
    OSHDBTagKey keyId = this._getTagTranslator().getOSHDBTagKeyOf(key);
    if (!keyId.isPresentInKeytables()) {
      LOG.warn("Tag key {} not found. No data will match this filter.", key.toString());
      ret._preFilters.add(ignored -> false);
      ret._filters.add(ignored -> false);
      return ret;
    }
    ret._preFilters.add(oshEntitiy -> oshEntitiy.hasTagKey(keyId));
    ret._filters.add(osmEntity -> osmEntity.hasTagKey(keyId));
    return ret;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * and value.
   *
   * @param key the tag to filter the osm entities for
   * @param value the tag value to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> osmTag(String key, String value) {
    return this.osmTag(new OSMTag(key, value));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * and value.
   *
   * @param tag the tag (key-value pair) to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  private MapReducer<X> _osmTag(OSMTag tag) {
    MapReducer<X> ret = this.copy();
    OSHDBTag keyValueId = this._getTagTranslator().getOSHDBTagOf(tag);
    if (!keyValueId.isPresentInKeytables()) {
      LOG.warn("Tag {}={} not found. No data will match this filter.",
          tag.getKey(), tag.getValue());
      ret._preFilters.add(ignored -> false);
      ret._filters.add(ignored -> false);
      return ret;
    }
    int keyId = keyValueId.getKey();
    int valueId = keyValueId.getValue();
    ret._preFilters.add(oshEntitiy -> oshEntitiy.hasTagKey(keyId));
    ret._filters.add(osmEntity -> osmEntity.hasTagValue(keyId, valueId));
    return ret;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * and one of the given values.
   *
   * @param key the tag key to filter the osm entities for
   * @param values an array of tag values to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> osmTag(String key, Collection<String> values) {
    MapReducer<X> ret = this.copy();
    OSHDBTagKey oshdbKey = this._getTagTranslator().getOSHDBTagKeyOf(key);
    int keyId = oshdbKey.toInt();
    if (!oshdbKey.isPresentInKeytables() || values.size() == 0) {
      LOG.warn((values.size() > 0 ? "Tag key {} not found." : "Empty tag value list.")
          + " No data will match this filter.", key);
      ret._preFilters.add(ignored -> false);
      ret._filters.add(ignored -> false);
      return ret;
    }
    Set<Integer> valueIds = new HashSet<>();
    for (String value : values) {
      OSHDBTag keyValueId = this._getTagTranslator().getOSHDBTagOf(key, value);
      if (!keyValueId.isPresentInKeytables()) {
        LOG.warn("Tag {}={} not found. No data will match this tag value.", key, value);
      } else {
        valueIds.add(keyValueId.getValue());
      }
    }
    ret._preFilters.add(oshEntitiy -> oshEntitiy.hasTagKey(keyId));
    ret._filters.add(osmEntity -> {
      int[] tags = osmEntity.getRawTags();
      for (int i = 0; i < tags.length; i += 2) {
        if (tags[i] > keyId) break;
        if (tags[i] == keyId) {
          return valueIds.contains(tags[i+1]);
        }
      }
      return false;
    });
    return ret;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have a tag with
   * the given key and whose value matches the given regular expression pattern.
   *
   * @param key the tag key to filter the osm entities for
   * @param valuePattern a regular expression which the tag value of the osm entity must match
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> osmTag(String key, Pattern valuePattern) {
    MapReducer<X> ret = this.copy();
    OSHDBTagKey oshdbKey = this._getTagTranslator().getOSHDBTagKeyOf(key);
    int keyId = oshdbKey.toInt();
    if (!oshdbKey.isPresentInKeytables()) {
      LOG.warn("Tag key {} not found. No data will match this filter.", key);
      ret._preFilters.add(ignored -> false);
      ret._filters.add(ignored -> false);
      return ret;
    }
    ret._preFilters.add(oshEntitiy -> oshEntitiy.hasTagKey(keyId));
    ret._filters.add(osmEntity -> {
      int[] tags = osmEntity.getRawTags();
      for (int i = 0; i < tags.length; i += 2) {
        if (tags[i] > keyId) return false;
        if (tags[i] == keyId) {
          String value = this._getTagTranslator().getOSMTagOf(keyId, tags[i + 1]).getValue();
          return valuePattern.matcher(value).matches();
        }
      }
      return false;
    });
    return ret;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have at least one
   * of the supplied tags (key=value pairs)
   *
   * @param tags the tags (key/value pairs) to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> osmTag(Collection<OSMTag> tags) {
    MapReducer<X> ret = this.copy();
    if (tags.size() == 0) {
      LOG.warn("Empty tag list. No data will match this filter.");
      ret._preFilters.add(ignored -> false);
      ret._filters.add(ignored -> false);
      return ret;
    }
    Set<Integer> keyIds = new HashSet<>();
    Set<OSHDBTag> keyValueIds = new HashSet<>();
    for (OSMTag tag : tags) {
      OSHDBTag keyValueId = this._getTagTranslator().getOSHDBTagOf(tag);
      if (!keyValueId.isPresentInKeytables()) {
        LOG.warn("Tag {}={} not found. No data will match this tag value.",
            tag.getKey(), tag.getValue());
      } else {
        keyIds.add(keyValueId.getKey());
        keyValueIds.add(keyValueId);
      }
    }
    ret._preFilters.add(oshEntitiy -> {
      for (int key : oshEntitiy.getRawTagKeys()) {
        if (keyIds.contains(key)) return true;
      }
      return false;
    });
    ret._filters.add(osmEntity -> {
      for (OSHDBTag oshdbTag : osmEntity.getTags()) {
        if (keyValueIds.contains(oshdbTag)) return true;
      }
      return false;
    });
    return ret;
  }

  // -----------------------------------------------------------------------------------------------
  // "map", "flatMap" transformation methods
  // -----------------------------------------------------------------------------------------------

  /**
   * Set an arbitrary `map` transformation function.
   *
   * @param mapper function that will be applied to each data entry (osm entity snapshot or
   *        contribution)
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of this MapReducer object operating on the transformed type (&lt;R&gt;)
   */
  @Contract(pure = true)
  public <R> MapReducer<R> map(SerializableFunction<X, R> mapper) {
    MapReducer<?> ret = this.copy();
    ret._mappers.add(mapper);
    //noinspection unchecked – after applying this mapper, we have a mapreducer of type R
    return (MapReducer<R>) ret;
  }

  /**
   * Set an arbitrary `map` transformation function.
   *
   * @param mapper1 function that will be applied to each data entry (osm entity snapshot or
   *        contribution)
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of this MapReducer object operating on the transformed type (&lt;R&gt;)
   */
  @Contract(pure = true)
  public <R, S> MapReducer<Pair<R, S>> mapPair(SerializableFunction<X, R> mapper1, SerializableFunction<X, S> mapper2) {
    return this.map(x -> Pair.of(mapper1.apply(x), mapper2.apply(x)));
  }

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with an arbitrary number
   * of results per input data entry. The results of this function will be "flattened", meaning that
   * they can be for example transformed again by setting additional `map` functions.
   *
   * @param flatMapper function that will be applied to each data entry (osm entity snapshot or
   *        contribution) and returns a list of results
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return a modified copy of this MapReducer object operating on the transformed type (&lt;R&gt;)
   */
  @Contract(pure = true)
  public <R> MapReducer<R> flatMap(SerializableFunction<X, Iterable<R>> flatMapper) {
    MapReducer<?> ret = this.copy();
    ret._mappers.add(flatMapper);
    ret._flatMappers.add(flatMapper);
    //noinspection unchecked – after applying this mapper, we have a mapreducer of type R
    return (MapReducer<R>) ret;
  }

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param f the filter function that determines if the respective data should be passed on (when f
   *        returns true) or discarded (when f returns false)
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> filter(SerializablePredicate<X> f) {
    return this
        .flatMap(data -> f.test(data) ? Collections.singletonList(data) : Collections.emptyList());
  }

  // --------------------------------------------------------------------------------------------
  // Neighbourhood
  // Functions for querying and filtering objects based on other objects in the neighbourhood
  // --------------------------------------------------------------------------------------------

  /**
   * Get objects (snapshots or contributions) in the neighbourhood filterd using call back function
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param mapReduce MapReducer function with search parameters for neighbourhoood filter
   * @param queryContributions If true, nearby contributions are queried. If false, snapshots.
   * @param contributionType Filter neighbours by contribution type. If null, all contribution types are considered.
   * @param <S> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of the MapReducer
   * @throws UnsupportedOperationException
   **/
  @Contract(pure = true)
  public <S, Y> MapReducer<Pair<X, Y>> neighbourhood(
      Double distanceInMeter,
      SerializableFunctionWithException<MapReducer<S>, Y> mapReduce,
      boolean queryContributions,
      ContributionType contributionType) {
    return this.map(data -> {
      try {
        if (this._forClass == OSMEntitySnapshot.class) {
          return Pair.of(data, SpatialRelations.neighbourhood(
              this._oshdbForTags,
              this._tstamps,
              distanceInMeter,
              mapReduce,
              (OSMEntitySnapshot) data,
              queryContributions,
              contributionType));
        } else if (this._forClass == OSMContribution.class) {
          return Pair.of(data, SpatialRelations.neighbourhood(
              this._oshdbForTags,
              distanceInMeter,
              mapReduce,
              (OSMContribution) data,
              GEOMETRY_OPTIONS.BOTH));
        } else {
          throw new UnsupportedOperationException("Operation for mapReducer of this class is not implemented.");
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
        return Pair.of(data, null);
      }
    });
  }

  /**
   * Get objects (snapshots or contributions) in the neighbourhood filtered by key and value
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @param queryContributions If true, nearby OSMCOntributions are queried. If false, OSMEntitySnapshots are queried
   * @param contributionType Filter neighbours by contribution type. If null, all contribution types are considered.
   * @param <S> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S>  MapReducer<Pair<X, List<S>>> neighbourhood(
      Double distanceInMeter,
      String key,
      String value,
      boolean queryContributions,
      ContributionType contributionType) {
    return this.neighbourhood(
        distanceInMeter,
        (SerializableFunctionWithException<MapReducer<S>, List<S>>) mapReduce -> mapReduce.osmTag(key, value).collect(),
        queryContributions,
        contributionType);
  }

  /**
   * Get objects (snapshots or contributions) in the neighbourhood filtered by key
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param key OSM tag key for filtering neighbouring objects
   * @param queryContributions If true, nearby OSMContributions are queried. If false, OSMEntitySnapshots are queried
   * @param contributionType Filter neighbours by contribution type. If null, all contribution types are considered.
   * @param <S> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S>  MapReducer<Pair<X, List<S>>> neighbourhood(
      Double distanceInMeter,
      String key,
      boolean queryContributions,
      ContributionType contributionType) {
    return this.neighbourhood(
        distanceInMeter,
        (SerializableFunctionWithException<MapReducer<S>, List<S>>) mapReduce -> mapReduce.osmTag(key).collect(),
        queryContributions,
        contributionType);
  }

  /**
   * Get objects (snapshots or contributions) in the neighbourhood without filtering
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param queryContributions If true, nearby OSMCOntributions are queried. If false, OSMEntitySnapshots are queried
   * @param <S> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S>  MapReducer<Pair<X, List<S>>> neighbourhood(
      Double distanceInMeter,
      boolean queryContributions,
      ContributionType contributionType) {
    return this.neighbourhood(
        distanceInMeter,
        (SerializableFunctionWithException<MapReducer<S>, List<S>>) null,
        queryContributions,
        contributionType);
  }

  /**
   * Get snapshots in the neighbourhood filtered by call back function
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param mapReduce MapReducer function with search parameters for neighbourhoood filter
   * @param <S> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S, Y>  MapReducer<Pair<X, Y>> neighbourhood(
      Double distanceInMeter,
      SerializableFunctionWithException<MapReducer<S>, Y> mapReduce) {
    return this.neighbourhood(
        distanceInMeter,
        mapReduce,
        false,
        null);
  }

  /**
   * Get snapshots in the neighbourhood filtered by key and value
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @param <S> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S> MapReducer<Pair<X, List<S>>> neighbourhood(
      Double distanceInMeter,
      String key,
      String value) {
    return this.neighbourhood(
        distanceInMeter,
        (SerializableFunctionWithException<MapReducer<S>, List<S>>) mapReduce -> mapReduce.osmTag(key, value).collect());
  }

  /**
   * Get snapshots in the neighbourhood filtered by key
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param key OSM tag key for filtering neighbouring objects
   * @param <S> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S> MapReducer<Pair<X, List<S>>> neighbourhood(
      Double distanceInMeter,
      String key) {
    return this.neighbourhood(
        distanceInMeter,
        (SerializableFunctionWithException<MapReducer<S>, List<S>>) mapReduce -> mapReduce.osmTag(key).collect());
  }

  /**
   * Get snapshots in the neighbourhood without filtering
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param <S> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S>  MapReducer<Pair<X, List<S>>> neighbourhood(
      Double distanceInMeter) {
    return this.neighbourhood(
        distanceInMeter,
        (SerializableFunctionWithException<MapReducer<S>, List<S>>) null);
  }

  // -----------------------------------------------------------------------------------------------
  // Neighbouring
  // -----------------------------------------------------------------------------------------------

  /**
   * Filter by neighbouring snapshots or contributions with call back function
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @param queryContributions If true, nearby contributions are queried. If false, snapshots.
   * @param contributionType Filter neighbours by contribution type. If null, all contribution types are considered.
   * @param <S> Class of neighbouring objects (OSMEntitySnapshot or OSMContribution)
   * @param <Y> Return type of mapReduce function
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public <S, Y extends Boolean> MapReducer<X> neighbouring(
      Double distanceInMeter,
      SerializableFunctionWithException<MapReducer<S>, Y> mapReduce,
      boolean queryContributions,
      ContributionType contributionType) {
    if (this._forClass == OSMEntitySnapshot.class) {
      MapReducer<Pair<X, Y>> pairMapReducer = this.neighbourhood(
          distanceInMeter,
          mapReduce,
          queryContributions,
          contributionType);
      return pairMapReducer.filter(p -> p.getRight()).map(p -> p.getKey());
    } else if (this._forClass == OSMContribution.class){
      MapReducer<Pair<X,Y>> pairMapReducer = this.neighbourhood(
          distanceInMeter,
          mapReduce,
          false,
          null);
      return pairMapReducer.filter(p -> p.getRight()).map(p -> p.getKey());
    } else {
      throw new UnsupportedOperationException("Operation for mapReducer of this class is not implemented.");
    }
  }

  /**
   * Filter by neighbouring contributions with key and value
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param key OSM key for filtering neighbouring objects
   * @param value OSM value for filtering neighbouring objects
   * @param queryContributions If true, nearby contributions are queried. If false, snapshots.
   * @param contributionType Filter neighbours by contribution type. If null, all contribution types are considered.
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapReducer<X> neighbouring(
      Double distanceInMeter,
      String key,
      String value,
      boolean queryContributions,
      ContributionType contributionType) {
    return this.neighbouring(
        distanceInMeter,
        mapReducer -> mapReducer.osmTag(key, value).count() > 0,
        queryContributions,
        contributionType);
  }

  /**
   * Filter by neighbouring contributions with key
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param key OSM tag key for filtering neighbouring objects
   * @param queryContributions If true, nearby contributions are queried. If false, snapshots.
   * @param contributionType Filter neighbours by contribution type. If null, all contribution types are considered.
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapReducer<X> neighbouring(
      Double distanceInMeter,
      String key,
      boolean queryContributions,
      ContributionType contributionType) {
    return this.neighbouring(
        distanceInMeter,
        mapReducer -> mapReducer.osmTag(key).count() > 0,
        queryContributions,
        contributionType);
  }

  /**
   * Filter by neighbouring snapshots with key and value
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param key OSM tag key for filtering neighbouring objects
   * @param value OSM tag value for filtering neighbouring objects
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public MapReducer<X> neighbouring(
      Double distanceInMeter,
      String key,
      String value) {
    return this.neighbouring(
        distanceInMeter,
        mapReducer -> mapReducer.osmTag(key, value).count() > 0);
  }

  /**
   * Filter by neighbouring snapshots with key
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param key OSM tag key for filtering neighbouring objects
   * @return a modified copy of this MapReducer
  **/
  @Contract(pure = true)
  public MapReducer<X> neighbouring(Double distanceInMeter, String key) {
    return this.neighbouring(
        distanceInMeter,
        mapReducer -> mapReducer.osmTag(key).count() > 0);
  }

  /**
   * Filter by neighbouring snapshots with call back function
   *
   * @param distanceInMeter radius that defines neighbourhood in meters
   * @param mapReduce MapReducer function to identify the objects of interest in the neighbourhood
   * @return a modified copy of this MapReducer
   **/
  @Contract(pure = true)
  public <S, Y extends Boolean> MapReducer<X> neighbouring(
      Double distanceInMeter,
      SerializableFunctionWithException<MapReducer<S>, Y> mapReduce) {
    return this.neighbouring(
        distanceInMeter,
        mapReduce,
        false,
        null);
  }

  // -----------------------------------------------------------------------------------------------
  // Egenhofer Relations
  // -----------------------------------------------------------------------------------------------

  /**
   * Filter objects by querying whether they are located inside other elements
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <X> return type of MapReducer
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <X> MapReducer<X> contains(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.contains((OSMEntitySnapshot) data))
        .filter(p -> p.getRight().size() > 0)
        .map(p -> (X) p.getKey());
  }

  /** Map all objects within which an object of mapReducer is located
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <S> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S> MapReducer<Pair<OSMEntitySnapshot, List<S>>> containsWhich(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.contains((OSMEntitySnapshot) data));
  }

  /**
   * Filter objects by querying whether they are located inside other elements
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <X> return type of MapReducer
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <X> MapReducer<X> covers(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.covers((OSMEntitySnapshot) data))
        .filter(p -> p.getRight().size() > 0)
        .map(p -> (X) p.getKey());
  }

  /** Map all objects within which an object of mapReducer is located
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <S> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S> MapReducer<Pair<OSMEntitySnapshot, List<S>>> coversWhich(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.covers((OSMEntitySnapshot) data));
  }

  /**
   * Filter objects by querying whether they are located inside other elements
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <X> return type of MapReducer
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <X> MapReducer<X> coveredBy(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.coveredBy((OSMEntitySnapshot) data))
        .filter(p -> p.getRight().size() > 0)
        .map(p -> (X) p.getKey());
  }

  /** Map all objects within which an object of mapReducer is located
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <S> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S> MapReducer<Pair<OSMEntitySnapshot, List<S>>> coveredByWhich(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.coveredBy((OSMEntitySnapshot) data));
  }

  /** Map all elements filtered by key that contain a given OSMEntitySnapshot
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <X> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <X> MapReducer<X> equals(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.equals((OSMEntitySnapshot) data))
        .filter(p -> p.getRight().size() > 0)
        .map(p -> (X) p.getKey());
  }

  /**
   * Map all elements filtered by key that contain a given OSMEntitySnapshot
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <S> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S> MapReducer<Pair<OSMEntitySnapshot, List<S>>> equalsWhich(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.equals((OSMEntitySnapshot) data));
  }

  /** Map all elements filtered by key that contain a given OSMEntitySnapshot
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <X> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <X> MapReducer<X> disjoint(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.disjoint((OSMEntitySnapshot) data))
        .filter(p -> p.getRight().size() > 0)
        .map(p -> (X) p.getKey());
  }

  /**
   * Map all elements filtered by key that contain a given OSMEntitySnapshot
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <S> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S> MapReducer<Pair<OSMEntitySnapshot, List<S>>> disjointWhich(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.disjoint((OSMEntitySnapshot) data));
  }

  /**
   * Filter objects by querying whether they are located inside other elements
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <X> return type of MapReducer
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <X> MapReducer<X> inside(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.inside((OSMEntitySnapshot) data))
        .filter(p -> p.getRight().size() > 0)
        .map(p -> (X) p.getKey());
  }

  /** Map all objects within which an object of mapReducer is located
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <S> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S> MapReducer<Pair<OSMEntitySnapshot, List<S>>> insideWhich(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.inside((OSMEntitySnapshot) data));
  }

  /**
   * Filter objects by querying whether they overlap other elements
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <X> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <X> MapReducer<X> overlaps(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.overlaps((OSMEntitySnapshot) data))
    .filter(p -> p.getRight().size() > 0)
        .map(p -> (X) p.getKey());
  }

  /**
   * Map all elements filtered by key that contain a given OSMEntitySnapshot
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <S> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S> MapReducer<Pair<OSMEntitySnapshot, List<S>>> overlapsWhich(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.overlaps((OSMEntitySnapshot) data));
  }

  /** Map all elements filtered by key that contain a given OSMEntitySnapshot
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <X> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <X> MapReducer<X> touches(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.touches((OSMEntitySnapshot) data))
        .filter(p -> p.getRight().size() > 0)
        .map(p -> (X) p.getKey());
  }

  /**
   * Map all elements filtered by key that contain a given OSMEntitySnapshot
   * @param key OSMtag key
   * @param value OSMtag value
   * @param <S> return type either OSMContribution or OSMEntitySnapshot
   * @return a modified copy of the MapReducer
   **/
  @Contract(pure = true)
  public <S> MapReducer<Pair<OSMEntitySnapshot, List<S>>> touchesWhich(
      String key,
      String value,
      boolean queryContributions)
      throws Exception {
    DE9IM egenhoferRelation = new DE9IM(
        this._oshdbForTags,
        this._bboxFilter,
        this._tstamps,
        key,
        value,
        queryContributions);
    return this.map(data -> egenhoferRelation.touches((OSMEntitySnapshot) data));
  }


  // -----------------------------------------------------------------------------------------------
  // Grouping and Aggregation
  // Sets how the input data is "grouped", or the output data is "aggregated" into separate chunks.
  // -----------------------------------------------------------------------------------------------

  /**
   * Groups the input data (osm entity snapshot or contributions) by their respective entity's ids
   * before feeding them into further transformation functions. This can be used to do more complex
   * analysis on the osm data, that requires one to know about the full editing history of
   * individual osm entities.
   *
   * This needs to be called before any `map` or `flatMap` transformation functions have been set.
   * Otherwise a runtime exception will be thrown.
   *
   * @return the MapReducer object which applies its transformations on (by entity id grouped) lists
   *         of the input data
   * @throws UnsupportedOperationException if this is called after some map (or flatMap) functions
   *         have already been set
   * @throws UnsupportedOperationException if this is called when a grouping has already been
   *         activated
   */
  @Contract(pure = true)
  public MapReducer<List<X>> groupByEntity() throws UnsupportedOperationException {
    if (!this._mappers.isEmpty()) {
      throw new UnsupportedOperationException(
          "groupByEntity() must be called before any `map` or `flatMap` transformation functions have been set");
    }
    if (this._grouping != Grouping.NONE) {
      throw new UnsupportedOperationException("A grouping is already active on this MapReducer");
    }
    MapReducer<X> ret = this.copy();
    ret._grouping = Grouping.BY_ID;
    //noinspection unchecked – now in the reduce() step, the backend will return a list of items
    return (MapReducer<List<X>>) ret;
  }

  /**
   * Sets a custom aggregation function that is used to group output results into.
   *
   * @param indexer a function that will be called for each input element and returns a value that
   *        will be used to group the results by
   * @param <U> the data type of the values used to aggregate the output. has to be a comparable
   *        type
   * @param zerofill a collection of values that are expected to be present in the result
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   */
  @Contract(pure = true)
  public <U extends Comparable<U>> MapAggregator<U, X> aggregateBy(
      SerializableFunction<X, U> indexer,
      Collection<U> zerofill
  ) {
    return new MapAggregator<>(this, indexer, zerofill);
  }

  /**
   * Sets a custom aggregation function that is used to group output results into.
   *
   * @param indexer a function that will be called for each input element and returns a value that
   *        will be used to group the results by
   * @param <U> the data type of the values used to aggregate the output. has to be a comparable
   *        type
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   */
  @Contract(pure = true)
  public <U extends Comparable<U>> MapAggregator<U, X> aggregateBy(
      SerializableFunction<X, U> indexer
  ) {
    return this.aggregateBy(indexer, Collections.emptyList());
  }

  /**
   * Sets up automatic aggregation by timestamp.
   *
   * In the OSMEntitySnapshotView, the snapshots' timestamp will be used directly to aggregate
   * results into. In the OSMContributionView, the timestamps of the respective data modifications
   * will be matched to corresponding time intervals (that are defined by the `timestamps` setting
   * here).
   *
   * Cannot be used together with the `groupByEntity()` setting enabled.
   *
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has been
   *         activated
   */
  @Contract(pure = true)
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp() throws UnsupportedOperationException {
    if (this._grouping != Grouping.NONE) {
      throw new UnsupportedOperationException(
          "automatic aggregateByTimestamp() cannot be used together with the groupByEntity() "+
          "functionality -> try using aggregateByTimestamp(customTimestampIndex) instead"
      );
    }

    // by timestamp indexing function -> for some views we need to match the input data to the list
    SerializableFunction<X, OSHDBTimestamp> indexer;
    if (this._forClass.equals(OSMContribution.class)) {
      final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this._tstamps.get());
      indexer = data -> timestamps.floor(((OSMContribution) data).getTimestamp());
    } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
      indexer = data -> ((OSMEntitySnapshot) data).getTimestamp();
    } else {
      throw new UnsupportedOperationException(
          "automatic aggregateByTimestamp() only implemented for OSMContribution and "+
          "OSMEntitySnapshot -> try using aggregateByTimestamp(customTimestampIndex) instead"
      );
    }

    if (this._mappers.size() > 0) {
      // for convenience we allow one to set this function even after some map functions were set.
      // if some map / flatMap functions were already set:
      // "rewind" them first, apply the indexer and then re-apply the map/flatMap functions
      // accordingly
      MapReducer<X> ret = this.copy();
      List<SerializableFunction> mappers = new LinkedList<>(ret._mappers);
      Set<SerializableFunction> flatMappers = new HashSet<>(ret._flatMappers);
      ret._mappers.clear();
      ret._flatMappers.clear();
      MapAggregator<OSHDBTimestamp, ?> mapAggregator =
          new MapAggregator<>(ret, indexer, this.getZerofillTimestamps());
      for (SerializableFunction action : mappers) {
        if (flatMappers.contains(action)) {
          //noinspection unchecked – applying untyped function (we don't know intermediate types)
          mapAggregator = mapAggregator.flatMap(action);
        } else {
          //noinspection unchecked – applying untyped function (we don't know intermediate types)
          mapAggregator = mapAggregator.map(action);
        }
      }
      //noinspection unchecked – after applying all (flat)map functions, the final type is X
      return (MapAggregator<OSHDBTimestamp, X>)mapAggregator;
    } else {
      return new MapAggregator<>(this, indexer, this.getZerofillTimestamps());
    }
  }

  /**
   * Sets up aggregation by a custom time index.
   *
   * The timestamps returned by the supplied indexing function are matched to the corresponding time intervals
   *
   * @param indexer a callback function that return a timestamp object for each given data. Note that
   *                if this function returns timestamps outside of the supplied timestamps() interval
   *                results may be undefined
   * @return a MapAggregator object with the equivalent state (settings,
   *         filters, map function, etc.) of the current MapReducer object
   */
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer
  ) throws UnsupportedOperationException {
    final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this._tstamps.get());
    return new MapAggregator<OSHDBTimestamp, X>(this, data -> {
      // match timestamps to the given timestamp list
      return timestamps.floor(indexer.apply(data));
    }, getZerofillTimestamps());
  }

  /**
   * Sets up automatic aggregation by geometries.
   *
   * Cannot be used together with the `groupByEntity()` setting enabled.
   *
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has been
   *         activated
   * @throws UnsupportedOperationException when called after any map or flatMap functions are set
   */
  @Contract(pure = true)
  public <U extends Comparable<U>, P extends Geometry & Polygonal>
      MapAggregator<U, X> aggregateByGeometry(Map<U, P> geometries) throws
      UnsupportedOperationException
  {
    if (this._grouping != Grouping.NONE) {
      throw new UnsupportedOperationException(
          "aggregateByGeometry() cannot be used together with the groupByEntity() functionality"
      );
    }

    GeometrySplitter<U> gs = new GeometrySplitter<>(geometries);
    if (this._mappers.size() > 0) {
      throw new UnsupportedOperationException(
          "please call aggregateByGeometry before setting any map or flatMap functions"
      );
    } else {
      MapAggregator<U, ? extends OSHDBMapReducible> ret;
      if (this._forClass.equals(OSMContribution.class)) {
        ret = this.flatMap(x -> gs.splitOSMContribution((OSMContribution) x))
            .aggregateBy(Pair::getKey, geometries.keySet()).map(Pair::getValue);
      } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
        ret = this.flatMap(x -> gs.splitOSMEntitySnapshot((OSMEntitySnapshot) x))
            .aggregateBy(Pair::getKey, geometries.keySet()).map(Pair::getValue);
      } else {
        throw new UnsupportedOperationException(
            "aggregateByGeometry not implemented for objects of type: " + this._forClass.toString()
        );
      }
      //noinspection unchecked – no mapper functions have been applied, so the type is still X
      return (MapAggregator<U, X>) ret;
    }
  }

    // -----------------------------------------------------------------------------------------------
  // Exposed generic reduce.
  // Can be used by experienced users of the api to implement complex queries.
  // These offer full flexibility, but are potentially a bit tricky to work with (see javadoc).
  // -----------------------------------------------------------------------------------------------

  /**
   * Generic map-reduce routine
   *
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href=
   * "https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a>
   * interface.
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   * @throws Exception
   */
  @Contract(pure = true)
  public <S> S reduce(SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    switch (this._grouping) {
      case NONE:
        if (this._flatMappers.size() == 0) {
          final SerializableFunction<Object, X> mapper = this._getMapper();
          if (this._forClass.equals(OSMContribution.class)) {
            //noinspection Convert2MethodRef having just `mapper::apply` here is problematic, see https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/commit/adeb425d969fe58116989d9b2e678c623a26de11#note_2094
            final SerializableFunction<OSMContribution, X> contributionMapper =
                data -> mapper.apply(data);
            return this.mapReduceCellsOSMContribution(
                contributionMapper,
                identitySupplier,
                accumulator,
                combiner
            );
          } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
            //noinspection Convert2MethodRef having just `mapper::apply` here is problematic, see https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/oshdb/commit/adeb425d969fe58116989d9b2e678c623a26de11#note_2094
            final SerializableFunction<OSMEntitySnapshot, X> snapshotMapper =
                data -> mapper.apply(data);
            return this.mapReduceCellsOSMEntitySnapshot(
                snapshotMapper,
                identitySupplier,
                accumulator,
                combiner
            );
          } else {
            throw new UnsupportedOperationException(
                "Unimplemented data view: " + this._forClass.toString());
          }
        } else {
          final SerializableFunction<Object, Iterable<X>> flatMapper = this._getFlatMapper();
          if (this._forClass.equals(OSMContribution.class)) {
            return this.flatMapReduceCellsOSMContributionGroupedById(
                (List<OSMContribution> inputList) -> {
                  List<X> outputList = new LinkedList<>();
                  inputList.stream()
                      .map((SerializableFunction<OSMContribution, Iterable<X>>) flatMapper::apply)
                      .forEach(data -> Iterables.addAll(outputList, data));
                  return outputList;
                }, identitySupplier, accumulator, combiner);
          } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
            return this.flatMapReduceCellsOSMEntitySnapshotGroupedById(
                (List<OSMEntitySnapshot> inputList) -> {
                  List<X> outputList = new LinkedList<>();
                  inputList.stream()
                      .map((SerializableFunction<OSMEntitySnapshot, Iterable<X>>) flatMapper::apply)
                      .forEach(data -> Iterables.addAll(outputList, data));
                  return outputList;
                }, identitySupplier, accumulator, combiner);
          } else {
            throw new UnsupportedOperationException(
                "Unimplemented data view: " + this._forClass.toString());
          }
        }
      case BY_ID:
        final SerializableFunction<Object, Iterable<X>> flatMapper;
        if (this._flatMappers.size() == 0) {
          final SerializableFunction<Object, X> mapper = this._getMapper();
          flatMapper = data -> Collections.singletonList(mapper.apply(data));
          // todo: check if this is actually necessary, doesn't getFlatMapper() do the "same" in this
          // case? should we add this as optimization case to getFlatMapper()??
        } else {
          flatMapper = this._getFlatMapper();
        }
        if (this._forClass.equals(OSMContribution.class)) {
          return this.flatMapReduceCellsOSMContributionGroupedById(
              (SerializableFunction<List<OSMContribution>, Iterable<X>>) flatMapper::apply,
              identitySupplier,
              accumulator,
              combiner
          );
        } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
          return this.flatMapReduceCellsOSMEntitySnapshotGroupedById(
              (SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>>) flatMapper::apply,
              identitySupplier, accumulator, combiner);
        } else {
          throw new UnsupportedOperationException(
              "Unimplemented data view: " + this._forClass.toString());
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported grouping: " + this._grouping.toString());
    }
  }

  /**
   * Generic map-reduce routine (shorthand syntax)
   *
   * This variant is shorter to program than `reduce(identitySupplier, accumulator, combiner)`, but
   * can only be used if the result type is the same as the current `map`ped type &lt;X&gt;. Also
   * this variant can be less efficient since it cannot benefit from the mutability freedoms the
   * accumulator+combiner approach has.
   *
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * <ul>
   * <li>the accumulator function needs to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the accumulator
   * function: `accumulator(identitySupplier(),x)` must be equal to `x`,</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href=
   * "https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-T-java.util.function.BinaryOperator-">reduce(identity,accumulator)</a>
   * interface.
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;X&gt;)
   *        and an accumulation value (also of type &lt;X&gt;, e.g. the result of
   *        `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this
   *        function is not to alter (mutate) the state of the accumulation value (e.g. directly
   *        adding new values to an existing Set object)
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  @Contract(pure = true)
  public X reduce(SerializableSupplier<X> identitySupplier,
      SerializableBinaryOperator<X> accumulator) throws Exception {
    return this.reduce(identitySupplier, accumulator::apply, accumulator);
  }

  // -----------------------------------------------------------------------------------------------
  // "Quality of life" helper methods to use the map-reduce functionality more directly and easily
  // for typical queries.
  // Available are: sum, count, average, weightedAverage and uniq.
  // Each one can be used to get results aggregated by timestamp, aggregated by a custom index and
  // not aggregated totals.
  // -----------------------------------------------------------------------------------------------

  /**
   * Sums up the results.
   *
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime
   * exception will be thrown.
   *
   * @return the sum of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   */
  @Contract(pure = true)
  public Number sum() throws Exception {
    return this.makeNumeric().reduce(() -> 0, NumberUtils::add);
  }

  /**
   * Sums up the results provided by a given `mapper` function.
   *
   * This is a shorthand for `.map(mapper).sum()`, with the difference that here the numerical
   * return type of the `mapper` is ensured.
   *
   * @param mapper function that returns the numbers to sum up
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the summed up results of the `mapper` function
   */
  @Contract(pure = true)
  public <R extends Number> R sum(SerializableFunction<X, R> mapper) throws Exception {
    return this.map(mapper).reduce(() -> (R) (Integer) 0, NumberUtils::add);
  }

  /**
   * Counts the number of results.
   *
   * @return the total count of features or modifications, summed up over all timestamps
   */
  @Contract(pure = true)
  public Integer count() throws Exception {
    return this.sum(ignored -> 1);
  }

  /**
   * Gets all unique values of the results.
   *
   * For example, this can be used together with the OSMContributionView to get the total amount of
   * unique users editing specific feature types.
   *
   * @return the set of distinct values
   */
  @Contract(pure = true)
  public Set<X> uniq() throws Exception {
    return this.reduce(HashSet::new, (acc, cur) -> {
      acc.add(cur);
      return acc;
    }, (a, b) -> {
      HashSet<X> result = new HashSet<>(a);
      result.addAll(b);
      return result;
    });
  }

  /**
   * Gets all unique values of the results provided by a given mapper function.
   *
   * This is a shorthand for `.map(mapper).uniq()`.
   *
   * @param mapper function that returns some values
   * @param <R> the type that is returned by the `mapper` function
   * @return a set of distinct values returned by the `mapper` function
   */
  @Contract(pure = true)
  public <R> Set<R> uniq(SerializableFunction<X, R> mapper) throws Exception {
    return this.map(mapper).uniq();
  }

  /**
   * Counts all unique values of the results.
   *
   * For example, this can be used together with the OSMContributionView to get the number of unique
   * users editing specific feature types.
   *
   * @return the set of distinct values
   */
  @Contract(pure = true)
  public Integer countUniq() throws Exception {
    return this.uniq().size();
  }

  /**
   * Calculates the averages of the results.
   *
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime
   * exception will be thrown.
   *
   * @return the average of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   */
  @Contract(pure = true)
  public Double average() throws Exception {
    return this.makeNumeric().average(n -> n);
  }

  /**
   * Calculates the average of the results provided by a given `mapper` function.
   *
   * @param mapper function that returns the numbers to average
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the average of the numbers returned by the `mapper` function
   */
  @Contract(pure = true)
  public <R extends Number> Double average(SerializableFunction<X, R> mapper) throws Exception {
    PayloadWithWeight<Double> runningSums =
        this.map(mapper).reduce(() -> new PayloadWithWeight<Double>(0.0, 0.0), (acc, cur) -> {
          acc.num = NumberUtils.add(acc.num, cur.doubleValue());
          acc.weight += 1;
          return acc;
        }, (a, b) -> new PayloadWithWeight<>(NumberUtils.add(a.num, b.num), a.weight + b.weight));
    return runningSums.num / runningSums.weight;
  }

  /**
   * Calculates the weighted average of the results provided by the `mapper` function.
   *
   * The mapper must return an object of the type `WeightedValue` which contains a numeric value
   * associated with a (floating point) weight.
   *
   * @param mapper function that gets called for each entity snapshot or modification, needs to
   *        return the value and weight combination of numbers to average
   * @return the weighted average of the numbers returned by the `mapper` function
   */
  @Contract(pure = true)
  public Double weightedAverage(SerializableFunction<X, WeightedValue> mapper) throws Exception {
    PayloadWithWeight<Double> runningSums =
        this.map(mapper).reduce(() -> new PayloadWithWeight<>(0.0, 0.0), (acc, cur) -> {
          acc.num = NumberUtils.add(acc.num, cur.getValue().doubleValue() * cur.getWeight());
          acc.weight += cur.getWeight();
          return acc;
        }, (a, b) -> new PayloadWithWeight<>(NumberUtils.add(a.num, b.num), a.weight + b.weight));
    return runningSums.num / runningSums.weight;
  }

  // -----------------------------------------------------------------------------------------------
  // "Iterator" like helpers (forEach, collect), mostly intended for testing purposes
  // -----------------------------------------------------------------------------------------------

  /**
   * Iterates over each entity snapshot or contribution, and performs a single `action` on each one
   * of them.
   *
   * This method can be handy for testing purposes. But note that since the `action` doesn't produce
   * a return value, it must facilitate its own way of producing output.
   *
   * If you'd like to use such a "forEach" in a non-test use case, use `.collect().forEach()`
   * instead.
   *
   * @param action function that gets called for each transformed data entry
   * @deprecated only for testing purposes
   */
  @Deprecated
  public void forEach(SerializableConsumer<X> action) throws Exception {
    // noinspection ResultOfMethodCallIgnored
    this.map(data -> {
      action.accept(data);
      return null;
    }).reduce(() -> null, (ignored, ignored2) -> null);
  }

  /**
   * Collects all results into a List
   *
   * @return a list with all results returned by the `mapper` function
   */
  @Contract(pure = true)
  public List<X> collect() throws Exception {
    return this.reduce(LinkedList::new, (acc, cur) -> {
      acc.add(cur);
      return acc;
    }, (list1, list2) -> {
      LinkedList<X> combinedLists = new LinkedList<>(list1);
      combinedLists.addAll(list2);
      return combinedLists;
    });
  }

  // -----------------------------------------------------------------------------------------------
  // Generic map-reduce functions (internal).
  // These need to be implemented by the actual db/processing backend!
  // -----------------------------------------------------------------------------------------------

  /**
   * Generic map-reduce used by the `OSMContributionView`
   *
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href=
   * "https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a>
   * interface.
   *
   * @param mapper a function that's called for each `OSMContribution`
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }

  /**
   * Generic "flat" version of the map-reduce used by the `OSMContributionView`, with by-osm-id
   * grouped input to the `mapper` function
   *
   * Contrary to the "normal" map-reduce, the "flat" version adds the possibility to return any
   * number of results in the `mapper` function. Additionally, this interface provides the `mapper`
   * function with a list of all `OSMContribution`s of a particular OSM entity. This is used to do
   * more complex analyses that require the full edit history of the respective OSM entities as
   * input.
   *
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href=
   * "https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a>
   * interface.
   *
   * @param mapper a function that's called for all `OSMContribution`s of a particular OSM entity;
   *        returns a list of results (which can have any number of entries).
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }

  /**
   * Generic map-reduce used by the `OSMEntitySnapshotView`
   *
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href=
   * "https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a>
   * interface.
   *
   * @param mapper a function that's called for each `OSMEntitySnapshot`
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }

  /**
   * Generic "flat" version of the map-reduce used by the `OSMEntitySnapshotView`, with by-osm-id
   * grouped input to the `mapper` function
   *
   * Contrary to the "normal" map-reduce, the "flat" version adds the possibility to return any
   * number of results in the `mapper` function. Additionally, this interface provides the `mapper`
   * function with a list of all `OSMContribution`s of a particular OSM entity. This is used to do
   * more complex analyses that require the full list of snapshots of the respective OSM entities as
   * input.
   *
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href=
   * "https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a>
   * interface.
   *
   * @param mapper a function that's called for all `OSMEntitySnapshot`s of a particular OSM entity;
   *        returns a list of results (which can have any number of entries)
   * @param identitySupplier a factory function that returns a new starting value to reduce results
   *        into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;)
   *        and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and
   *        returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter
   *        (mutate) the state of the accumulation value (e.g. directly adding new values to an
   *        existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function
   *        must be pure (have no side effects), and is not allowed to alter the state of the two
   *        input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the
   *         `combiner` function, after all `mapper` results have been aggregated (in the
   *         `accumulator` and `combiner` steps)
   */
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }

  // -----------------------------------------------------------------------------------------------
  // Some helper methods for internal use in the mapReduce functions
  // -----------------------------------------------------------------------------------------------

  protected TagInterpreter _getTagInterpreter() throws ParseException, SQLException, IOException {
    if (this._tagInterpreter == null) {
      this._tagInterpreter = new DefaultTagInterpreter(this._getTagTranslator());
    }
    return this._tagInterpreter;
  }

  protected TagTranslator _getTagTranslator() {
    if (this._tagTranslator == null) {
      try {
        this._tagTranslator = new TagTranslator(this._oshdbForTags.getConnection());
      } catch (OSHDBKeytablesNotFoundException e) {
        LOG.error(e.getMessage());
        throw new RuntimeException(e);
      }
    }
    return this._tagTranslator;
  }

  // Helper that chains multiple oshEntity filters together
  protected CellIterator.OSHEntityFilter _getPreFilter() {
    return (this._preFilters.isEmpty()) ? (oshEntity -> true) : (oshEntity -> {
      for (SerializablePredicate<OSHEntity> filter : this._preFilters) {
        if (!filter.test(oshEntity)) {
          return false;
        }
      }
      return true;
    });
  }

  // Helper that chains multiple osmEntity filters together
  protected CellIterator.OSMEntityFilter _getFilter() {
    return (this._filters.isEmpty()) ? (osmEntity -> true) : (osmEntity -> {
      for (SerializablePredicate<OSMEntity> filter : this._filters) {
        if (!filter.test(osmEntity)) {
          return false;
        }
      }
      return true;
    });
  }

  // get all cell ids covered by the current area of interest's bounding box
  protected Iterable<Pair<CellId, CellId>> _getCellIdRanges() {
    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
    if (this._bboxFilter == null || (this._bboxFilter.getMinLon() >= this._bboxFilter.getMaxLon()
        || this._bboxFilter.getMinLat() >= this._bboxFilter.getMaxLat())) {
      // return an empty iterable if bbox is not set or empty
      LOG.warn("area of interest not set or empty");
      return Collections.emptyList();
    }
    return grid.bbox2CellIdRanges(this._bboxFilter, false);
  }

  // hack, so that we can use a variable that is of both Geometry and implements Polygonal (i.e.
  // Polygon or MultiPolygon) as required in further processing steps
  protected <P extends Geometry & Polygonal> P _getPolyFilter() {
    //noinspection unchecked – all setters only accept Polygonal geometries
    return (P) this._polyFilter;
  }

  // concatenates all applied `map` functions
  private SerializableFunction<Object, X> _getMapper() {
    // todo: maybe we can somehow optimize this?? at least for special cases like
    // this._mappers.size() == 1
    return (SerializableFunction<Object, X>) (data -> {
      Object result = data;
      for (SerializableFunction mapper : this._mappers) {
        if (this._flatMappers.contains(mapper)) {
          throw new UnsupportedOperationException("cannot flat map this");
        } else {
          //noinspection unchecked – we don't know the actual intermediate types ¯\_(ツ)_/¯
          result = mapper.apply(result);
        }
      }
      //noinspection unchecked – after applying all mapper functions, the result type is X
      return (X) result;
    });
  }

  // concatenates all applied `flatMap` and `map` functions
  private SerializableFunction<Object, Iterable<X>> _getFlatMapper() {
    // todo: maybe we can somehow optimize this?? at least for special cases like
    // this._mappers.size() == 1
    return (SerializableFunction<Object, Iterable<X>>) (data -> {
      List<Object> results = new LinkedList<>();
      results.add(data);
      for (SerializableFunction mapper : this._mappers) {
        List<Object> newResults = new LinkedList<>();
        if (this._flatMappers.contains(mapper)) {
          //noinspection unchecked – we don't know the actual intermediate types ¯\_(ツ)_/¯
          results.forEach(result -> Iterables.addAll(newResults, (Iterable<?>) mapper.apply(result)));
        } else {
          //noinspection unchecked – we don't know the actual intermediate types ¯\_(ツ)_/¯
          results.forEach(result -> newResults.add(mapper.apply(result)));
        }
        results = newResults;
      }
      //noinspection unchecked – after applying all mapper functions, the result type is List<X>
      return (Iterable<X>) results;
    });
  }

  // casts current results to a numeric type, for summing and averaging
  private MapReducer<Number> makeNumeric() {
    return this.map(x -> {
      if (!Number.class.isInstance(x)) // todo: slow??
      {
        throw new UnsupportedOperationException(
            "Cannot convert to non-numeric values of type: " + x.getClass().toString());
      }
      return (Number) x;
    });
  }

  // gets list of timestamps to use for zerofilling
  Collection<OSHDBTimestamp> getZerofillTimestamps() {
    if (this._forClass.equals(OSMEntitySnapshot.class)) {
      return this._tstamps.get();
    } else {
      SortedSet<OSHDBTimestamp> result = new TreeSet<>(this._tstamps.get());
      result.remove(result.last());
      return result;
    }
  }
}

// -------------------------------------------------------------------------------------------------
// Auxiliary classes and interfaces
// -------------------------------------------------------------------------------------------------


// mutable version of WeightedValue type (for internal use to do faster aggregation)
class PayloadWithWeight<X> implements Serializable {

  X num;
  double weight;

  PayloadWithWeight(X num, double weight) {
    this.num = num;
    this.weight = weight;
  }
}
