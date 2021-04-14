package org.heigit.ohsome.oshdb.api.mapreducer;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.tdunning.math.stats.TDigest;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.DoubleUnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.heigit.ohsome.oshdb.OSHDB;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTag;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBJdbc;
import org.heigit.ohsome.oshdb.api.generic.NumberUtils;
import org.heigit.ohsome.oshdb.api.generic.WeightedValue;
import org.heigit.ohsome.oshdb.api.object.OSHDBMapReducible;
import org.heigit.ohsome.oshdb.api.object.OSMContribution;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.filter.AndOperator;
import org.heigit.ohsome.oshdb.filter.Filter;
import org.heigit.ohsome.oshdb.filter.FilterExpression;
import org.heigit.ohsome.oshdb.filter.FilterParser;
import org.heigit.ohsome.oshdb.filter.GeometryTypeFilter;
import org.heigit.ohsome.oshdb.filter.TagFilterEquals;
import org.heigit.ohsome.oshdb.filter.TagFilterEqualsAny;
import org.heigit.ohsome.oshdb.filter.TypeFilter;
import org.heigit.ohsome.oshdb.index.XYGridTree;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.osm.OSMType;
import org.heigit.ohsome.oshdb.util.OSHDBTagKey;
import org.heigit.ohsome.oshdb.util.celliterator.ContributionType;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBInvalidTimestampException;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBKeytablesNotFoundException;
import org.heigit.ohsome.oshdb.util.function.OSHEntityFilter;
import org.heigit.ohsome.oshdb.util.function.OSMEntityFilter;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableConsumer;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializablePredicate;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.heigit.ohsome.oshdb.util.geometry.Geo;
import org.heigit.ohsome.oshdb.util.geometry.OSHDBGeometryBuilder;
import org.heigit.ohsome.oshdb.util.taginterpreter.DefaultTagInterpreter;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTag;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTagInterface;
import org.heigit.ohsome.oshdb.util.tagtranslator.OSMTagKey;
import org.heigit.ohsome.oshdb.util.tagtranslator.TagTranslator;
import org.heigit.ohsome.oshdb.util.time.IsoDateTimeParser;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestampList;
import org.heigit.ohsome.oshdb.util.time.OSHDBTimestamps;
import org.heigit.ohsome.oshdb.util.time.TimestampFormatter;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class of oshdb's "functional programming" API.
 *
 * <p>It accepts a list of filters, transformation `map` functions a produces a result when calling
 * the `reduce` method (or one of its shorthand versions like `sum`, `count`, etc.).</p>
 *
 * <p>
 * You can set a list of filters that are applied on the raw OSM data, for example you can filter:
 * </p>
 * <ul>
 * <li>geometrically by an area of interest (bbox or polygon)</li>
 * <li>by osm tags (key only or key/value)</li>
 * <li>by OSM type</li>
 * <li>custom filter callback</li>
 * </ul>
 *
 * <p>Depending on the used data "view", the MapReducer produces either "snapshots" or evaluated
 * all modifications ("contributions") of the matching raw OSM data.</p>
 *
 * <p>These data can then be transformed arbitrarily by user defined `map` functions (which take one
 * of these entity snapshots or modifications as input an produce an arbitrary output) or `flatMap`
 * functions (which can return an arbitrary number of results per entity snapshot/contribution). It
 * is possible to chain together any number of transformation functions.</p>
 *
 * <p>Finally, one can either use one of the pre-defined result-generating functions (e.g. `sum`,
 * `count`, `average`, `uniq`), or specify a custom `reduce` procedure.</p>
 *
 * <p>If one wants to get results that are aggregated by timestamp (or some other index), one can
 * use the `aggregateByTimestamp` or `aggregateBy` functionality that automatically handles the
 * grouping of the output data.</p>
 *
 * <p>For more complex analyses, it is also possible to enable the grouping of the input data by
 * the respective OSM ID. This can be used to view at the whole history of entities at once.</p>
 *
 * @param <X> the type that is returned by the currently set of mapper function. the next added
 *        mapper function will be called with a parameter of this type as input
 */
public abstract class MapReducer<X> implements
    MapReducerSettings<MapReducer<X>>, Mappable<X>, MapReducerAggregations<X>,
    MapAggregatable<MapAggregator<? extends Comparable<?>, X>, X>, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(MapReducer.class);

  protected OSHDBDatabase oshdb;
  protected transient OSHDBJdbc keytables;

  protected Long timeout = null;

  // internal state
  Class<? extends OSHDBMapReducible> forClass;

  enum Grouping {
    NONE, BY_ID
  }

  Grouping grouping = Grouping.NONE;

  /**
   * Returns if the current backend can be canceled (e.g. in a query timeout).
   */
  public boolean isCancelable() {
    return false;
  }

  // utility objects
  private transient TagTranslator tagTranslator = null;
  private TagInterpreter tagInterpreter = null;

  // settings and filters
  protected OSHDBTimestampList tstamps = new OSHDBTimestamps(
      "2008-01-01",
      TimestampFormatter.getInstance().date(new Date()),
      OSHDBTimestamps.Interval.MONTHLY
  );
  protected OSHDBBoundingBox bboxFilter = new OSHDBBoundingBox(-180, -90, 180, 90);
  private Geometry polyFilter = null;
  protected EnumSet<OSMType> typeFilter = EnumSet.of(OSMType.NODE, OSMType.WAY, OSMType.RELATION);
  private final List<SerializablePredicate<OSHEntity>> preFilters = new ArrayList<>();
  private final List<SerializablePredicate<OSMEntity>> filters = new ArrayList<>();
  final List<MapFunction> mappers = new LinkedList<>();


  // basic constructor
  protected MapReducer(OSHDBDatabase oshdb, Class<? extends OSHDBMapReducible> forClass) {
    this.oshdb = oshdb;
    this.forClass = forClass;
  }

  // copy constructor
  protected MapReducer(MapReducer<?> obj) {
    this.oshdb = obj.oshdb;
    this.keytables = obj.keytables;

    this.forClass = obj.forClass;
    this.grouping = obj.grouping;

    this.tagTranslator = obj.tagTranslator;
    this.tagInterpreter = obj.tagInterpreter;

    this.tstamps = obj.tstamps;
    this.bboxFilter = obj.bboxFilter;
    this.polyFilter = obj.polyFilter;
    this.typeFilter = obj.typeFilter.clone();
    this.preFilters.addAll(obj.preFilters);
    this.filters.addAll(obj.filters);
    this.mappers.addAll(obj.mappers);
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
   * @param keytables the database to use for resolving strings into internal identifiers
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> keytables(OSHDBJdbc keytables) {
    if (keytables != this.oshdb && this.oshdb instanceof OSHDBJdbc) {
      Connection c = ((OSHDBJdbc) this.oshdb).getConnection();
      boolean oshdbContainsKeytables = true;
      try {
        (new TagTranslator(c)).close();
      } catch (OSHDBKeytablesNotFoundException e) {
        // this is the expected path -> the oshdb doesn't have the key tables
        oshdbContainsKeytables = false;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      if (oshdbContainsKeytables) {
        LOG.warn("It looks like as if the current OSHDB comes with keytables included. "
            + "Usually this means that you should use this file's keytables "
            + "and should not set the keytables manually.");
      }
    }
    MapReducer<X> ret = this.copy();
    ret.keytables = keytables;
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
    ret.tagInterpreter = tagInterpreter;
    return ret;
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
  @Override
  @Contract(pure = true)
  public MapReducer<X> areaOfInterest(@NotNull OSHDBBoundingBox bboxFilter) {
    MapReducer<X> ret = this.copy();
    if (this.polyFilter == null) {
      ret.bboxFilter = OSHDBBoundingBox.intersect(bboxFilter, ret.bboxFilter);
    } else {
      ret.polyFilter = Geo.clip(ret.polyFilter, bboxFilter);
      ret.bboxFilter = OSHDBGeometryBuilder.boundingBoxOf(ret.polyFilter.getEnvelopeInternal());
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
  @Override
  @Contract(pure = true)
  public <P extends Geometry & Polygonal> MapReducer<X> areaOfInterest(@NotNull P polygonFilter) {
    MapReducer<X> ret = this.copy();
    if (this.polyFilter == null) {
      ret.polyFilter = Geo.clip(polygonFilter, ret.bboxFilter);
    } else {
      ret.polyFilter = Geo.clip(polygonFilter, ret.getPolyFilter());
    }
    ret.bboxFilter = OSHDBGeometryBuilder.boundingBoxOf(ret.polyFilter.getEnvelopeInternal());
    return ret;
  }

  /**
   * Set the timestamps for which to perform the analysis.
   *
   * <p>
   * Depending on the *View*, this has slightly different semantics:
   * </p>
   * <ul><li>
   * For the OSMEntitySnapshotView it will set the time slices at which to take the "snapshots"
   * </li><li>
   * For the OSMContributionView it will set the time interval in which to look for
   * osm contributions (only the first and last timestamp of this list are contributing).
   * </li></ul>
   * Additionally, these timestamps are used in the `aggregateByTimestamp` functionality.
   *
   * @param tstamps an object (implementing the OSHDBTimestampList interface) which provides the
   *        timestamps to do the analysis for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> timestamps(OSHDBTimestampList tstamps) {
    MapReducer<X> ret = this.copy();
    ret.tstamps = tstamps;
    return ret;
  }

  /**
   * Set the timestamps for which to perform the analysis in a regular interval between a start and
   * end date.
   *
   * <p>See {@link #timestamps(OSHDBTimestampList)} for further information.</p>
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
   * <p>Useful in combination with the OSMEntitySnapshotView when not performing further aggregation
   * by timestamp.</p>
   *
   * <p>See {@link #timestamps(OSHDBTimestampList)} for further information.</p>
   *
   * @param isoDate an ISO 8601 date string representing the date of the analysis
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> timestamps(String isoDate) {
    if (this.forClass.equals(OSMContribution.class)) {
      LOG.warn("OSMContributionView requires two or more timestamps, but only one was supplied.");
    }
    return this.timestamps(isoDate, isoDate, new String[] {});
  }

  /**
   * Sets two timestamps (start and end date) for which to perform the analysis.
   *
   * <p>Useful in combination with the OSMContributionView when not performing further aggregation
   * by timestamp.</p>
   *
   * <p>See {@link #timestamps(OSHDBTimestampList)} for further information.</p>
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
   * <p>Note for programmers wanting to use this method to supply an arbitrary number (n&gt;=1) of
   * timestamps: You may supply the same time string multiple times, which will be de-duplicated
   * internally. E.g. you can call the method like this:
   * <code>.timestamps(dateArr[0], dateArr[0], dateArr)</code>
   * </p>
   *
   * <p>See {@link #timestamps(OSHDBTimestampList)} for further information.</p>
   *
   * @param isoDateFirst an ISO 8601 date string representing the start date of the analysis
   * @param isoDateSecond an ISO 8601 date string representing the second date of the analysis
   * @param isoDateMore more ISO 8601 date strings representing the remaining timestamps of the
   *        analysis
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Contract(pure = true)
  public MapReducer<X> timestamps(
      String isoDateFirst, String isoDateSecond, String... isoDateMore) {
    SortedSet<OSHDBTimestamp> timestamps = new TreeSet<>();
    try {
      timestamps.add(
          new OSHDBTimestamp(IsoDateTimeParser.parseIsoDateTime(isoDateFirst).toEpochSecond())
      );
      timestamps.add(
          new OSHDBTimestamp(IsoDateTimeParser.parseIsoDateTime(isoDateSecond).toEpochSecond())
      );
      for (String isoDate : isoDateMore) {
        timestamps.add(
            new OSHDBTimestamp(IsoDateTimeParser.parseIsoDateTime(isoDate).toEpochSecond())
        );
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
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Override
  @Deprecated
  @Contract(pure = true)
  public MapReducer<X> osmType(Set<OSMType> typeFilter) {
    return osmTypeInternal(typeFilter);
  }

  @Contract(pure = true)
  private MapReducer<X> osmTypeInternal(Set<OSMType> typeFilter) {
    MapReducer<X> ret = this.copy();
    typeFilter = Sets.intersection(ret.typeFilter, typeFilter);
    if (typeFilter.isEmpty()) {
      ret.typeFilter = EnumSet.noneOf(OSMType.class);
    } else {
      ret.typeFilter = EnumSet.copyOf(typeFilter);
    }
    return ret;
  }

  /**
   * Adds a custom arbitrary filter that gets executed for each osm entity and determines if it
   * should be considered for this analyis or not.
   *
   * @param f the filter function to call for each osm entity
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(FilterExpression)} with {@link
   *             org.heigit.ohsome.oshdb.filter.Filter#byOSMEntity(OSMEntityFilter)}
   *             instead
   */
  @Override
  @Deprecated
  @Contract(pure = true)
  public MapReducer<X> osmEntityFilter(OSMEntityFilter f) {
    MapReducer<X> ret = this.copy();
    ret.filters.add(f);
    return ret;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * (with an arbitrary value).
   *
   * @param key the tag key to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Override
  @Deprecated
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
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Override
  @Deprecated
  @Contract(pure = true)
  public MapReducer<X> osmTag(OSMTagInterface tag) {
    if (tag instanceof OSMTag) {
      return this.osmTag((OSMTag) tag);
    }
    if (tag instanceof OSMTagKey) {
      return this.osmTag((OSMTagKey) tag);
    }
    throw new UnsupportedOperationException("Unknown object implementing OSMTagInterface.");
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * (with an arbitrary value).
   *
   * @param key the tag key to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Deprecated
  @Contract(pure = true)
  private MapReducer<X> osmTag(OSMTagKey key) {
    OSHDBTagKey keyId = this.getTagTranslator().getOSHDBTagKeyOf(key);
    if (!keyId.isPresentInKeytables()) {
      LOG.warn("Tag key {} not found. No data will match this filter.", key.toString());
      return osmTagEmptyResult();
    }
    return osmTag(keyId);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * and value.
   *
   * @param key the tag to filter the osm entities for
   * @param value the tag value to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Override
  @Deprecated
  @Contract(pure = true)
  public MapReducer<X> osmTag(String key, String value) {
    return this.osmTag(new OSMTag(key, value));
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * and value.
   *
   * @param tag the tag (key-value pair or key=*) to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Deprecated
  @Contract(pure = true)
  private MapReducer<X> osmTag(OSMTag tag) {
    OSHDBTag keyValueId = this.getTagTranslator().getOSHDBTagOf(tag);
    if (!keyValueId.isPresentInKeytables()) {
      LOG.warn("Tag {}={} not found. No data will match this filter.",
          tag.getKey(), tag.getValue());
      return osmTagEmptyResult();
    }
    return osmTag(keyValueId);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key
   * and one of the given values.
   *
   * @param key the tag key to filter the osm entities for
   * @param values an array of tag values to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Override
  @Deprecated
  @Contract(pure = true)
  public MapReducer<X> osmTag(String key, Collection<String> values) {
    OSHDBTagKey oshdbKey = this.getTagTranslator().getOSHDBTagKeyOf(key);
    int keyId = oshdbKey.toInt();
    if (!oshdbKey.isPresentInKeytables() || values.isEmpty()) {
      LOG.warn(
          (values.isEmpty()
              ? "Empty tag value list. No data will match this filter."
              : "Tag key {} not found. No data will match this filter."),
          key
      );
      return osmTagEmptyResult();
    }
    Set<Integer> valueIds = new HashSet<>();
    for (String value : values) {
      OSHDBTag keyValueId = this.getTagTranslator().getOSHDBTagOf(key, value);
      if (!keyValueId.isPresentInKeytables()) {
        LOG.warn("Tag {}={} not found. No data will match this tag value.", key, value);
      } else {
        valueIds.add(keyValueId.getValue());
      }
    }
    MapReducer<X> ret = this.copy();
    ret.preFilters.add(oshEntity -> oshEntity.hasTagKey(keyId));
    ret.filters.add(osmEntity -> {
      int[] tags = osmEntity.getRawTags();
      for (int i = 0; i < tags.length; i += 2) {
        if (tags[i] > keyId) {
          break;
        }
        if (tags[i] == keyId) {
          return valueIds.contains(tags[i + 1]);
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
  @Override
  @Contract(pure = true)
  public MapReducer<X> osmTag(String key, Pattern valuePattern) {
    OSHDBTagKey oshdbKey = this.getTagTranslator().getOSHDBTagKeyOf(key);
    int keyId = oshdbKey.toInt();
    if (!oshdbKey.isPresentInKeytables()) {
      LOG.warn("Tag key {} not found. No data will match this filter.", key);
      return osmTagEmptyResult();
    }
    MapReducer<X> ret = this.copy();
    ret.preFilters.add(oshEntity -> oshEntity.hasTagKey(keyId));
    ret.filters.add(osmEntity -> {
      int[] tags = osmEntity.getRawTags();
      for (int i = 0; i < tags.length; i += 2) {
        if (tags[i] > keyId) {
          return false;
        }
        if (tags[i] == keyId) {
          String value = this.getTagTranslator().getOSMTagOf(keyId, tags[i + 1]).getValue();
          return valuePattern.matcher(value).matches();
        }
      }
      return false;
    });
    return ret;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have at least one
   * of the supplied tags (key=value pairs or key=*).
   *
   * @param tags the tags (key/value pairs or key=*) to filter the osm entities for
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   * @deprecated use oshdb-filter {@link #filter(String)} instead
   */
  @Override
  @Deprecated
  @Contract(pure = true)
  public MapReducer<X> osmTag(Collection<? extends OSMTagInterface> tags) {
    if (tags.isEmpty()) {
      LOG.warn("Empty tag list. No data will match this filter.");
      return osmTagEmptyResult();
    }
    // for the "pre"-filter which removes all entities which don't match at least one of the
    // given tag keys
    Set<Integer> preKeyIds = new HashSet<>();
    // sets of tag keys and tags for the concrete entity filter: either one of these must match
    Set<Integer> keyIds = new HashSet<>();
    Set<OSHDBTag> keyValueIds = new HashSet<>();
    for (OSMTagInterface tag : tags) {
      if (tag instanceof OSMTag) {
        OSMTag keyValue = (OSMTag) tag;
        OSHDBTag keyValueId = this.getTagTranslator().getOSHDBTagOf(keyValue);
        if (!keyValueId.isPresentInKeytables()) {
          LOG.warn("Tag {}={} not found. No data will match this tag value.",
              keyValue.getKey(), keyValue.getValue());
        } else {
          preKeyIds.add(keyValueId.getKey());
          keyValueIds.add(keyValueId);
        }
      } else {
        OSHDBTagKey keyId = this.getTagTranslator().getOSHDBTagKeyOf((OSMTagKey) tag);
        preKeyIds.add(keyId.toInt());
        keyIds.add(keyId.toInt());
      }
    }
    MapReducer<X> ret = this.copy();
    ret.preFilters.add(oshEntity -> {
      for (int key : oshEntity.getRawTagKeys()) {
        if (preKeyIds.contains(key)) {
          return true;
        }
      }
      return false;
    });
    ret.filters.add(osmEntity -> {
      for (OSHDBTag oshdbTag : osmEntity.getTags()) {
        if (keyIds.contains(oshdbTag.getKey()) || keyValueIds.contains(oshdbTag)) {
          return true;
        }
      }
      return false;
    });
    return ret;
  }

  @Contract(pure = true)
  private MapReducer<X> osmTag(OSHDBTag tag) {
    MapReducer<X> ret = this.copy();
    ret.preFilters.add(oshEntity -> oshEntity.hasTagKey(tag.getKey()));
    ret.filters.add(osmEntity -> osmEntity.hasTagValue(tag.getKey(), tag.getValue()));
    return ret;
  }

  @Contract(pure = true)
  private MapReducer<X> osmTag(OSHDBTagKey tagKey) {
    MapReducer<X> ret = this.copy();
    ret.preFilters.add(oshEntity -> oshEntity.hasTagKey(tagKey));
    ret.filters.add(osmEntity -> osmEntity.hasTagKey(tagKey));
    return ret;
  }

  @Contract(pure = true)
  private MapReducer<X> osmTagEmptyResult() {
    MapReducer<X>  ret = this.copy();
    ret.preFilters.add(ignored -> false);
    ret.filters.add(ignored -> false);
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
  @Override
  @Contract(pure = true)
  public <R> MapReducer<R> map(SerializableFunction<X, R> mapper) {
    MapReducer<?> ret = this.copy();
    ret.mappers.add(new MapFunction(mapper, false));
    @SuppressWarnings("unchecked") // after applying this mapper, we have a mapreducer of type R
    MapReducer<R> result = (MapReducer<R>) ret;
    return result;
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
  @Override
  @Contract(pure = true)
  public <R> MapReducer<R> flatMap(SerializableFunction<X, Iterable<R>> flatMapper) {
    MapReducer<?> ret = this.copy();
    ret.mappers.add(new MapFunction(flatMapper, true));
    @SuppressWarnings("unchecked") // after applying this mapper, we have a mapreducer of type R
    MapReducer<R> result = (MapReducer<R>) ret;
    return result;
  }

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param f the filter function that determines if the respective data should be passed on (when f
   *        returns true) or discarded (when f returns false)
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapReducer<X> filter(SerializablePredicate<X> f) {
    return this
        .flatMap(data -> f.test(data) ? Collections.singletonList(data) : Collections.emptyList());
  }

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
  @Override
  @Contract(pure = true)
  public MapReducer<X> filter(FilterExpression f) {
    MapReducer<X> ret = this.copy();
    ret.preFilters.add(f::applyOSH);
    ret.filters.add(f::applyOSM);
    // apply geometry filter as first map function
    final List<MapFunction> remainingMappers = List.copyOf(ret.mappers);
    ret.mappers.clear();
    if (this.grouping == Grouping.NONE) {
      // no grouping -> directly filter using the geometries of the snapshot / contribution
      if (ret.forClass.equals(OSMEntitySnapshot.class)) {
        ret = ret.filter(x -> {
          OSMEntitySnapshot s = (OSMEntitySnapshot) x;
          return f.applyOSMGeometry(s.getEntity(), s::getGeometry);
        });
      } else if (ret.forClass.equals(OSMContribution.class)) {
        ret = ret.filter(x -> {
          OSMContribution c = (OSMContribution) x;
          if (c.is(ContributionType.CREATION)) {
            return f.applyOSMGeometry(c.getEntityAfter(), c::getGeometryAfter);
          } else if (c.is(ContributionType.DELETION)) {
            return f.applyOSMGeometry(c.getEntityBefore(), c::getGeometryBefore);
          } else {
            return f.applyOSMGeometry(c.getEntityBefore(), c::getGeometryBefore)
                || f.applyOSMGeometry(c.getEntityAfter(), c::getGeometryAfter);
          }
        });
      }
    } else if (this.grouping == Grouping.BY_ID) {
      // grouping by entity -> filter each list entry individually
      if (ret.forClass.equals(OSMEntitySnapshot.class)) {
        @SuppressWarnings("unchecked") MapReducer<X> filteredListMapper = (MapReducer<X>)
            ret.map(x -> (Collection<OSMEntitySnapshot>) x)
                .map(snapshots -> snapshots.stream()
                    .filter(s -> f.applyOSMGeometry(s.getEntity(), s::getGeometry))
                    .collect(Collectors.toCollection(ArrayList::new)))
                .filter(snapshots -> !snapshots.isEmpty());
        ret = filteredListMapper;
      } else if (ret.forClass.equals(OSMContribution.class)) {
        @SuppressWarnings("unchecked") MapReducer<X> filteredListMapper = (MapReducer<X>)
            ret.map(x -> (Collection<OSMContribution>) x)
                .map(contributions -> contributions.stream()
                    .filter(c -> {
                      if (c.is(ContributionType.CREATION)) {
                        return f.applyOSMGeometry(c.getEntityAfter(), c::getGeometryAfter);
                      } else if (c.is(ContributionType.DELETION)) {
                        return f.applyOSMGeometry(c.getEntityBefore(), c::getGeometryBefore);
                      } else {
                        return f.applyOSMGeometry(c.getEntityBefore(), c::getGeometryBefore)
                            || f.applyOSMGeometry(c.getEntityAfter(), c::getGeometryAfter);
                      }
                    })
                    .collect(Collectors.toCollection(ArrayList::new)))
                .filter(contributions -> !contributions.isEmpty());
        ret = filteredListMapper;
      }
    } else {
      throw new UnsupportedOperationException(
          "filtering not implemented in grouping mode " + this.grouping.toString());
    }
    ret.mappers.addAll(remainingMappers);
    return optimizeFilters(ret, f);
  }

  /**
   * Apply a textual filter to this query.
   *
   * @see <a href="https://github.com/GIScience/oshdb/tree/master/oshdb-filter#syntax">oshdb-filter
   *      readme</a> for a description of the filter syntax.
   *
   * @param f the filter string to apply
   * @return a modified copy of this mapReducer (can be used to chain multiple commands together)
   */
  @Override
  @Contract(pure = true)
  public MapReducer<X> filter(String f) {
    return this.filter(new FilterParser(this.getTagTranslator()).parse(f));
  }

  // -----------------------------------------------------------------------------------------------
  // Grouping and Aggregation
  // Sets how the input data is "grouped", or the output data is "aggregated" into separate chunks.
  // -----------------------------------------------------------------------------------------------

  /**
   * Groups the input data (osm entity snapshot or contributions) by their respective entity's ids
   * before feeding them into further transformation functions. This can be used to do more complex
   * analysis on the osm data, that requires one to know about the full editing history of
   * individual osm entities, e.g., when looking for contributions which got reverted at a later
   * point in time.
   *
   * <p>The values in the returned lists of snapshot or contribution objects are returned in their
   * natural order: i.e. sorted ascending by timestamp.</p>
   *
   * <p>This needs to be called before any `map` or `flatMap` transformation functions have been
   * set. Otherwise a runtime exception will be thrown.</p>
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
    if (!this.mappers.isEmpty()) {
      throw new UnsupportedOperationException(
          "groupByEntity() must be called before any `map` or `flatMap` "
              + "transformation functions have been set"
      );
    }
    if (this.grouping != Grouping.NONE) {
      throw new UnsupportedOperationException("A grouping is already active on this MapReducer");
    }
    MapReducer<X> ret = this.copy();
    ret.grouping = Grouping.BY_ID;
    @SuppressWarnings("unchecked") // now in the reduce step the backend will return a list of items
    MapReducer<List<X>> result = (MapReducer<List<X>>) ret;
    return result;
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
  public <U extends Comparable<U> & Serializable> MapAggregator<U, X> aggregateBy(
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
  @Override
  @Contract(pure = true)
  public <U extends Comparable<U> & Serializable> MapAggregator<U, X> aggregateBy(
      SerializableFunction<X, U> indexer
  ) {
    return this.aggregateBy(indexer, Collections.emptyList());
  }

  /**
   * Sets up automatic aggregation by timestamp.
   *
   * <p>In the OSMEntitySnapshotView, the snapshots' timestamp will be used directly to aggregate
   * results into. In the OSMContributionView, the timestamps of the respective data modifications
   * will be matched to corresponding time intervals (that are defined by the `timestamps` setting
   * here).</p>
   *
   * <p>Cannot be used together with the `groupByEntity()` setting enabled.</p>
   *
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has
   *         been activated
   */
  @Contract(pure = true)
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp()
      throws UnsupportedOperationException {
    if (this.grouping != Grouping.NONE) {
      throw new UnsupportedOperationException(
          "automatic aggregateByTimestamp() cannot be used together with the groupByEntity() "
              + "functionality -> try using aggregateByTimestamp(customTimestampIndex) instead"
      );
    }

    // by timestamp indexing function -> for some views we need to match the input data to the list
    SerializableFunction<X, OSHDBTimestamp> indexer;
    if (this.forClass.equals(OSMContribution.class)) {
      final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this.tstamps.get());
      indexer = data -> timestamps.floor(((OSMContribution) data).getTimestamp());
    } else if (this.forClass.equals(OSMEntitySnapshot.class)) {
      indexer = data -> ((OSMEntitySnapshot) data).getTimestamp();
    } else {
      throw new UnsupportedOperationException(
          "automatic aggregateByTimestamp() only implemented for OSMContribution and "
              + "OSMEntitySnapshot -> try using aggregateByTimestamp(customTimestampIndex) instead"
      );
    }

    if (this.mappers.size() > 0) {
      // for convenience we allow one to set this function even after some map functions were set.
      // if some map / flatMap functions were already set:
      // "rewind" them first, apply the indexer and then re-apply the map/flatMap functions
      // accordingly
      MapReducer<X> ret = this.copy();
      List<MapFunction> mappers = new LinkedList<>(ret.mappers);
      ret.mappers.clear();
      MapAggregator<OSHDBTimestamp, ?> mapAggregator =
          new MapAggregator<>(ret, indexer, this.getZerofillTimestamps());
      for (MapFunction action : mappers) {
        if (action.isFlatMapper()) {
          @SuppressWarnings("unchecked") // applying untyped function (we don't know interm. types)
          MapAggregator<OSHDBTimestamp, ?> flatMappedMapAggregator = mapAggregator.flatMap(action);
          mapAggregator = flatMappedMapAggregator;
        } else {
          @SuppressWarnings("unchecked") // applying untyped function (we don't know interm. types)
          MapAggregator<OSHDBTimestamp, ?> mappedMapAggregator = mapAggregator.map(action);
          mapAggregator = mappedMapAggregator;
        }
      }
      @SuppressWarnings("unchecked") // after applying all (flat)map functions the final type is X
      MapAggregator<OSHDBTimestamp, X> result = (MapAggregator<OSHDBTimestamp, X>) mapAggregator;
      return result;
    } else {
      return new MapAggregator<>(this, indexer, this.getZerofillTimestamps());
    }
  }

  /**
   * Sets up aggregation by a custom time index.
   *
   * <p>The timestamps returned by the supplied indexing function are matched to the corresponding
   * time intervals.</p>
   *
   * @param indexer a callback function that return a timestamp object for each given data. Note
   *                that if this function returns timestamps outside of the supplied timestamps()
   *                interval results may be undefined
   * @return a MapAggregator object with the equivalent state (settings,
   *         filters, map function, etc.) of the current MapReducer object
   */
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp(
      SerializableFunction<X, OSHDBTimestamp> indexer
  ) throws UnsupportedOperationException {
    final TreeSet<OSHDBTimestamp> timestamps = new TreeSet<>(this.tstamps.get());
    final OSHDBTimestamp minTime = timestamps.first();
    final OSHDBTimestamp maxTime = timestamps.last();
    return new MapAggregator<>(this, data -> {
      // match timestamps to the given timestamp list
      OSHDBTimestamp aggregationTimestamp = indexer.apply(data);
      if (aggregationTimestamp == null
          || aggregationTimestamp.compareTo(minTime) < 0
          || aggregationTimestamp.compareTo(maxTime) > 0) {
        throw new OSHDBInvalidTimestampException(
            "Aggregation timestamp outside of time query interval."
        );
      }
      return timestamps.floor(aggregationTimestamp);
    }, getZerofillTimestamps());
  }

  /**
   * Sets up automatic aggregation by geometries.
   *
   * <p>Cannot be used together with the `groupByEntity()` setting enabled.</p>
   *
   * @return a MapAggregator object with the equivalent state (settings, filters, map function,
   *         etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupByEntity()` mode has
   *         been activated
   * @throws UnsupportedOperationException when called after any map or flatMap functions are set
   */
  @Contract(pure = true)
  public <U extends Comparable<U> & Serializable, P extends Geometry & Polygonal>
      MapAggregator<U, X> aggregateByGeometry(Map<U, P> geometries) throws
      UnsupportedOperationException {
    if (this.grouping != Grouping.NONE) {
      throw new UnsupportedOperationException(
          "aggregateByGeometry() cannot be used together with the groupByEntity() functionality"
      );
    }

    GeometrySplitter<U> gs = new GeometrySplitter<>(geometries);
    if (this.mappers.size() > 0) {
      throw new UnsupportedOperationException(
          "please call aggregateByGeometry before setting any map or flatMap functions"
      );
    } else {
      MapAggregator<U, ? extends OSHDBMapReducible> ret;
      if (this.forClass.equals(OSMContribution.class)) {
        ret = this.flatMap(x -> gs.splitOSMContribution((OSMContribution) x).entrySet())
            .aggregateBy(Entry::getKey, geometries.keySet()).map(Entry::getValue);
      } else if (this.forClass.equals(OSMEntitySnapshot.class)) {
        ret = this.flatMap(x -> gs.splitOSMEntitySnapshot((OSMEntitySnapshot) x).entrySet())
            .aggregateBy(Entry::getKey, geometries.keySet()).map(Entry::getValue);
      } else {
        throw new UnsupportedOperationException(
            "aggregateByGeometry not implemented for objects of type: " + this.forClass.toString()
        );
      }
      @SuppressWarnings("unchecked") // no mapper functions have been applied so the type is still X
      MapAggregator<U, X> result = (MapAggregator<U, X>) ret;
      return result;
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Exposed generic reduce.
  // Can be used by experienced users of the api to implement complex queries.
  // These offer full flexibility, but are potentially a bit tricky to work with (see javadoc).
  // -----------------------------------------------------------------------------------------------

  /**
   * Generic map-reduce routine.
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
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
   * @throws UnsupportedOperationException if the used oshdb database backend doesn't implement
   *         the required reduce operation.
   * @throws Exception if during the reducing operation an exception happens (see the respective
   *         implementations for details).
   */
  @Override
  @Contract(pure = true)
  public <S> S reduce(
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, X, S> accumulator,
      SerializableBinaryOperator<S> combiner)
      throws Exception {
    checkTimeout();
    switch (this.grouping) {
      case NONE:
        if (this.mappers.stream().noneMatch(MapFunction::isFlatMapper)) {
          final SerializableFunction<Object, X> mapper = this.getMapper();
          if (this.forClass.equals(OSMContribution.class)) {
            @SuppressWarnings("Convert2MethodRef")
            // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
            final SerializableFunction<OSMContribution, X> contributionMapper =
                data -> mapper.apply(data);
            return this.mapReduceCellsOSMContribution(
                contributionMapper,
                identitySupplier,
                accumulator,
                combiner
            );
          } else if (this.forClass.equals(OSMEntitySnapshot.class)) {
            @SuppressWarnings("Convert2MethodRef")
            // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
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
                "Unimplemented data view: " + this.forClass.toString());
          }
        } else {
          final SerializableFunction<Object, Iterable<X>> flatMapper = this.getFlatMapper();
          if (this.forClass.equals(OSMContribution.class)) {
            return this.flatMapReduceCellsOSMContributionGroupedById(
                (List<OSMContribution> inputList) -> {
                  List<X> outputList = new LinkedList<>();
                  inputList.stream()
                      .map((SerializableFunction<OSMContribution, Iterable<X>>) flatMapper::apply)
                      .forEach(data -> Iterables.addAll(outputList, data));
                  return outputList;
                }, identitySupplier, accumulator, combiner);
          } else if (this.forClass.equals(OSMEntitySnapshot.class)) {
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
                "Unimplemented data view: " + this.forClass.toString());
          }
        }
      case BY_ID:
        final SerializableFunction<Object, Iterable<X>> flatMapper;
        if (this.mappers.stream().noneMatch(MapFunction::isFlatMapper)) {
          final SerializableFunction<Object, X> mapper = this.getMapper();
          flatMapper = data -> Collections.singletonList(mapper.apply(data));
          // todo: check if this is actually necessary, doesn't getFlatMapper() do the "same" in
          // this case? should we add this as optimization case to getFlatMapper()??
        } else {
          flatMapper = this.getFlatMapper();
        }
        if (this.forClass.equals(OSMContribution.class)) {
          @SuppressWarnings("Convert2MethodRef")
          // having just `flatMapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
          final SerializableFunction<List<OSMContribution>, Iterable<X>> contributionFlatMapper =
              data -> flatMapper.apply(data);
          return this.flatMapReduceCellsOSMContributionGroupedById(
              contributionFlatMapper,
              identitySupplier,
              accumulator,
              combiner
          );
        } else if (this.forClass.equals(OSMEntitySnapshot.class)) {
          @SuppressWarnings("Convert2MethodRef")
          // having just `flatMapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
          final SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> snapshotFlatMapper =
              data -> flatMapper.apply(data);
          return this.flatMapReduceCellsOSMEntitySnapshotGroupedById(
              snapshotFlatMapper,
              identitySupplier,
              accumulator,
              combiner
          );
        } else {
          throw new UnsupportedOperationException(
              "Unimplemented data view: " + this.forClass.toString());
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported grouping: " + this.grouping.toString());
    }
  }

  /**
   * Generic map-reduce routine (shorthand syntax).
   *
   * <p>
   * This variant is shorter to program than `reduce(identitySupplier, accumulator, combiner)`, but
   * can only be used if the result type is the same as the current `map`ped type &lt;X&gt;. Also
   * this variant can be less efficient since it cannot benefit from the mutability freedoms the
   * accumulator+combiner approach has.
   * </p>
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator function needs to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the accumulator
   * function: `accumulator(identitySupplier(),x)` must be equal to `x`,</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(T,java.util.function.BinaryOperator)">reduce(identity,accumulator)</a>
   * interface.
   * </p>
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
  @Override
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
   * <p>The current data values need to be numeric (castable to "Number" type), otherwise a runtime
   * exception will be thrown.</p>
   *
   * @return the sum of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   */
  @Override
  @Contract(pure = true)
  public Number sum() throws Exception {
    return this.makeNumeric().reduce(() -> 0, NumberUtils::add);
  }

  /**
   * Sums up the results provided by a given `mapper` function.
   *
   * <p>This is a shorthand for `.map(mapper).sum()`, with the difference that here the numerical
   * return type of the `mapper` is ensured.</p>
   *
   * @param mapper function that returns the numbers to sum up
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the summed up results of the `mapper` function
   */
  @Override
  @Contract(pure = true)
  public <R extends Number> R sum(SerializableFunction<X, R> mapper) throws Exception {
    return this.map(mapper).reduce(() -> (R) (Integer) 0, NumberUtils::add);
  }

  /**
   * Counts the number of results.
   *
   * @return the total count of features or modifications, summed up over all timestamps
   */
  @Override
  @Contract(pure = true)
  public Integer count() throws Exception {
    return this.sum(ignored -> 1);
  }

  /**
   * Gets all unique values of the results.
   *
   * <p>For example, this can be used together with the OSMContributionView to get the total amount
   * of unique users editing specific feature types.</p>
   *
   * @return the set of distinct values
   */
  @Override
  @Contract(pure = true)
  public Set<X> uniq() throws Exception {
    return this.reduce(
        MapReducer::uniqIdentitySupplier,
        MapReducer::uniqAccumulator,
        MapReducer::uniqCombiner
    );
  }

  /**
   * Gets all unique values of the results provided by a given mapper function.
   *
   * <p>This is a shorthand for `.map(mapper).uniq()`.</p>
   *
   * @param mapper function that returns some values
   * @param <R> the type that is returned by the `mapper` function
   * @return a set of distinct values returned by the `mapper` function
   */
  @Override
  @Contract(pure = true)
  public <R> Set<R> uniq(SerializableFunction<X, R> mapper) throws Exception {
    return this.map(mapper).uniq();
  }

  /**
   * Counts all unique values of the results.
   *
   * <p>For example, this can be used together with the OSMContributionView to get the number of
   * unique users editing specific feature types.</p>
   *
   * @return the set of distinct values
   */
  @Override
  @Contract(pure = true)
  public Integer countUniq() throws Exception {
    return this.uniq().size();
  }

  /**
   * Calculates the averages of the results.
   *
   * <p>The current data values need to be numeric (castable to "Number" type), otherwise a runtime
   * exception will be thrown.</p>
   *
   * @return the average of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   */
  @Override
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
  @Override
  @Contract(pure = true)
  public <R extends Number> Double average(SerializableFunction<X, R> mapper) throws Exception {
    return this.weightedAverage(data -> new WeightedValue(mapper.apply(data), 1.0));
  }

  /**
   * Calculates the weighted average of the results provided by the `mapper` function.
   *
   * <p>The mapper must return an object of the type `WeightedValue` which contains a numeric value
   * associated with a (floating point) weight.</p>
   *
   * @param mapper function that gets called for each entity snapshot or modification, needs to
   *        return the value and weight combination of numbers to average
   * @return the weighted average of the numbers returned by the `mapper` function
   */
  @Override
  @Contract(pure = true)
  public Double weightedAverage(SerializableFunction<X, WeightedValue> mapper) throws Exception {
    MutableWeightedDouble runningSums =
        this.map(mapper).reduce(
            MutableWeightedDouble::identitySupplier,
            MutableWeightedDouble::accumulator,
            MutableWeightedDouble::combiner
        );
    return runningSums.num / runningSums.weight;
  }

  /**
   * Returns an estimate of the median of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @return estimated median
   */
  @Override
  @Contract(pure = true)
  public Double estimatedMedian() throws Exception {
    return this.estimatedQuantile(0.5);
  }

  /**
   * Returns an estimate of the median of the results after applying the given map function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the mean for
   * @return estimated median
   */
  @Override
  @Contract(pure = true)
  public <R extends Number> Double estimatedMedian(SerializableFunction<X, R> mapper)
      throws Exception {
    return this.estimatedQuantile(mapper, 0.5);
  }

  /**
   * Returns an estimate of a requested quantile of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param q the desired quantile to calculate (as a number between 0 and 1)
   * @return estimated quantile boundary
   */
  @Override
  @Contract(pure = true)
  public Double estimatedQuantile(double q) throws Exception {
    return this.makeNumeric().estimatedQuantile(n -> n, q);
  }

  /**
   * Returns an estimate of a requested quantile of the results after applying the given map
   * function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the quantile for
   * @param q the desired quantile to calculate (as a number between 0 and 1)
   * @return estimated quantile boundary
   */
  @Override
  @Contract(pure = true)
  public <R extends Number> Double estimatedQuantile(SerializableFunction<X, R> mapper, double q)
      throws Exception {
    return this.estimatedQuantiles(mapper).applyAsDouble(q);
  }

  /**
   * Returns an estimate of the quantiles of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param q the desired quantiles to calculate (as a collection of numbers between 0 and 1)
   * @return estimated quantile boundaries
   */
  @Override
  @Contract(pure = true)
  public List<Double> estimatedQuantiles(Iterable<Double> q) throws Exception {
    return this.makeNumeric().estimatedQuantiles(n -> n, q);
  }

  /**
   * Returns an estimate of the quantiles of the results after applying the given map function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the quantiles for
   * @param q the desired quantiles to calculate (as a collection of numbers between 0 and 1)
   * @return estimated quantile boundaries
   */
  @Override
  @Contract(pure = true)
  public <R extends Number> List<Double> estimatedQuantiles(
      SerializableFunction<X, R> mapper,
      Iterable<Double> q
  ) throws Exception {
    return Streams.stream(q)
        .mapToDouble(Double::doubleValue)
        .map(this.estimatedQuantiles(mapper))
        .boxed()
        .collect(Collectors.toList());
  }

  /**
   * Returns a function that computes estimates of arbitrary quantiles of the results.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @return a function that computes estimated quantile boundaries
   */
  @Override
  @Contract(pure = true)
  public DoubleUnaryOperator estimatedQuantiles() throws Exception {
    return this.makeNumeric().estimatedQuantiles(n -> n);
  }

  /**
   * Returns a function that computes estimates of arbitrary quantiles of the results after applying
   * the given map function.
   *
   * <p>
   * Uses the t-digest algorithm to calculate estimates for the quantiles in a map-reduce system:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   * </p>
   *
   * @param mapper function that returns the numbers to generate the quantiles for
   * @return a function that computes estimated quantile boundaries
   */
  @Override
  @Contract(pure = true)
  public <R extends Number> DoubleUnaryOperator estimatedQuantiles(
      SerializableFunction<X, R> mapper
  ) throws Exception {
    TDigest digest = this.digest(mapper);
    return digest::quantile;
  }

  /**
   * generates the t-digest of the complete result set. see:
   * https://raw.githubusercontent.com/tdunning/t-digest/master/docs/t-digest-paper/histo.pdf
   */
  @Contract(pure = true)
  private <R extends Number> TDigest digest(SerializableFunction<X, R> mapper) throws Exception {
    return this.map(mapper).reduce(
        TdigestReducer::identitySupplier,
        TdigestReducer::accumulator,
        TdigestReducer::combiner
    );
  }

  // -----------------------------------------------------------------------------------------------
  // "Iterator" like helpers (stream, collect)
  // -----------------------------------------------------------------------------------------------

  /**
   * Iterates over each entity snapshot or contribution, and performs a single `action` on each one
   * of them.
   *
   * <p>This method can be handy for testing purposes. But note that since the `action` doesn't
   * produce a return value, it must facilitate its own way of producing output.</p>
   *
   * <p>If you'd like to use such a "forEach" in a non-test use case, use `.stream().forEach()`
   * instead.</p>
   *
   * @param action function that gets called for each transformed data entry
   * @deprecated only for testing purposes, use `.stream().forEach()` instead
   */
  @Deprecated
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void forEach(SerializableConsumer<X> action) throws Exception {
    this.map(data -> {
      action.accept(data);
      return null;
    }).reduce(() -> null, (ignored, ignored2) -> null);
  }

  /**
   * Collects all results into a List.
   *
   * @return a list with all results returned by the `mapper` function
   */
  @Override
  @Contract(pure = true)
  public List<X> collect() throws Exception {
    return this.reduce(
        MapReducer::collectIdentitySupplier,
        MapReducer::collectAccumulator,
        MapReducer::collectCombiner
    );
  }

  /**
   * Returns all results as a Stream.
   *
   * <p>If the used oshdb database backend doesn't implement the stream operation directly, this
   * will fall back to executing `.collect().stream()` instead, which buffers all results in
   * memory first before returning them as a stream.</p>
   *
   * @return a stream with all results returned by the `mapper` function
   */
  @Override
  @Contract(pure = true)
  public Stream<X> stream() throws Exception {
    try {
      return this.streamInternal();
    } catch (UnsupportedOperationException e) {
      LOG.info("stream not directly supported by chosen backend, falling back to "
          + ".collect().stream()"
      );
      return this.collect().stream();
    }
  }

  @Contract(pure = true)
  private Stream<X> streamInternal() throws Exception {
    checkTimeout();
    switch (this.grouping) {
      case NONE:
        if (this.mappers.stream().noneMatch(MapFunction::isFlatMapper)) {
          final SerializableFunction<Object, X> mapper = this.getMapper();
          if (this.forClass.equals(OSMContribution.class)) {
            @SuppressWarnings("Convert2MethodRef")
            // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
            final SerializableFunction<OSMContribution, X> contributionMapper =
                data -> mapper.apply(data);
            return this.mapStreamCellsOSMContribution(contributionMapper);
          } else if (this.forClass.equals(OSMEntitySnapshot.class)) {
            @SuppressWarnings("Convert2MethodRef")
            // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
            final SerializableFunction<OSMEntitySnapshot, X> snapshotMapper =
                data -> mapper.apply(data);
            return this.mapStreamCellsOSMEntitySnapshot(snapshotMapper);
          } else {
            throw new UnsupportedOperationException(
                "Unimplemented data view: " + this.forClass.toString());
          }
        } else {
          final SerializableFunction<Object, Iterable<X>> flatMapper = this.getFlatMapper();
          if (this.forClass.equals(OSMContribution.class)) {
            return this.flatMapStreamCellsOSMContributionGroupedById(
                (List<OSMContribution> inputList) -> {
                  List<X> outputList = new LinkedList<>();
                  inputList.stream()
                      .map((SerializableFunction<OSMContribution, Iterable<X>>) flatMapper::apply)
                      .forEach(data -> Iterables.addAll(outputList, data));
                  return outputList;
                });
          } else if (this.forClass.equals(OSMEntitySnapshot.class)) {
            return this.flatMapStreamCellsOSMEntitySnapshotGroupedById(
                (List<OSMEntitySnapshot> inputList) -> {
                  List<X> outputList = new LinkedList<>();
                  inputList.stream()
                      .map((SerializableFunction<OSMEntitySnapshot, Iterable<X>>) flatMapper::apply)
                      .forEach(data -> Iterables.addAll(outputList, data));
                  return outputList;
                });
          } else {
            throw new UnsupportedOperationException("Unimplemented data view: "
                + this.forClass.toString());
          }
        }
      case BY_ID:
        final SerializableFunction<Object, Iterable<X>> flatMapper;
        if (this.mappers.stream().noneMatch(MapFunction::isFlatMapper)) {
          final SerializableFunction<Object, X> mapper = this.getMapper();
          flatMapper = data -> Collections.singletonList(mapper.apply(data));
          // todo: check if this is actually necessary, doesn't getFlatMapper() do the "same" in
          // this case? should we add this as optimization case to getFlatMapper()??
        } else {
          flatMapper = this.getFlatMapper();
        }
        if (this.forClass.equals(OSMContribution.class)) {
          @SuppressWarnings("Convert2MethodRef")
          // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
          final SerializableFunction<List<OSMContribution>, Iterable<X>> contributionFlatMapper =
              data -> flatMapper.apply(data);
          return this.flatMapStreamCellsOSMContributionGroupedById(contributionFlatMapper);
        } else if (this.forClass.equals(OSMEntitySnapshot.class)) {
          @SuppressWarnings("Convert2MethodRef")
          // having just `mapper::apply` here is problematic, see https://github.com/GIScience/oshdb/pull/37
          final SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> snapshotFlatMapper =
              data -> flatMapper.apply(data);
          return this.flatMapStreamCellsOSMEntitySnapshotGroupedById(snapshotFlatMapper);
        } else {
          throw new UnsupportedOperationException(
              "Unimplemented data view: " + this.forClass.toString());
        }
      default:
        throw new UnsupportedOperationException(
            "Unsupported grouping: " + this.grouping.toString());
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Generic map-stream functions (internal).
  // These need to be implemented by the actual db/processing backend!
  // -----------------------------------------------------------------------------------------------

  protected abstract Stream<X> mapStreamCellsOSMContribution(
      SerializableFunction<OSMContribution, X> mapper
  ) throws Exception;

  protected abstract Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper
  ) throws Exception;

  protected abstract Stream<X> mapStreamCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, X> mapper
  ) throws Exception;

  protected abstract Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper
  ) throws Exception;

  // -----------------------------------------------------------------------------------------------
  // Generic map-reduce functions (internal).
  // These need to be implemented by the actual db/processing backend!
  // -----------------------------------------------------------------------------------------------

  /**
   * Generic map-reduce used by the `OSMContributionView`.
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
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
  protected abstract <R, S> S mapReduceCellsOSMContribution(
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception;

  /**
   * Generic "flat" version of the map-reduce used by the `OSMContributionView`, with by-osm-id
   * grouped input to the `mapper` function.
   *
   * <p>
   * Contrary to the "normal" map-reduce, the "flat" version adds the possibility to return any
   * number of results in the `mapper` function. Additionally, this interface provides the `mapper`
   * function with a list of all `OSMContribution`s of a particular OSM entity. This is used to do
   * more complex analyses that require the full edit history of the respective OSM entities as
   * input.
   * </p>
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
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
  protected abstract <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception;

  /**
   * Generic map-reduce used by the `OSMEntitySnapshotView`.
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
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
  protected abstract <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception;

  /**
   * Generic "flat" version of the map-reduce used by the `OSMEntitySnapshotView`, with by-osm-id
   * grouped input to the `mapper` function.
   *
   * <p>
   * Contrary to the "normal" map-reduce, the "flat" version adds the possibility to return any
   * number of results in the `mapper` function. Additionally, this interface provides the `mapper`
   * function with a list of all `OSMContribution`s of a particular OSM entity. This is used to do
   * more complex analyses that require the full list of snapshots of the respective OSM entities as
   * input.
   * </p>
   *
   * <p>
   * The combination of the used types and identity/reducer functions must make "mathematical"
   * sense:
   * </p>
   * <ul>
   * <li>the accumulator and combiner functions need to be associative,</li>
   * <li>values generated by the identitySupplier factory must be an identity for the combiner
   * function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   * <li>the combiner function must be compatible with the accumulator function: `combiner(u,
   * accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * <p>
   * Functionally, this interface is similar to Java11 Stream's
   * <a href="https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/stream/Stream.html#reduce(U,java.util.function.BiFunction,java.util.function.BinaryOperator)">reduce(identity,accumulator,combiner)</a>
   * interface.
   * </p>
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
  protected abstract <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception;

  // -----------------------------------------------------------------------------------------------
  // Some helper methods for internal use in the mapReduce functions
  // -----------------------------------------------------------------------------------------------

  protected TagInterpreter getTagInterpreter() throws ParseException, IOException {
    if (this.tagInterpreter == null) {
      this.tagInterpreter = new DefaultTagInterpreter(this.getTagTranslator());
    }
    return this.tagInterpreter;
  }

  protected TagTranslator getTagTranslator() {
    if (this.tagTranslator == null) {
      try {
        if (this.keytables == null) {
          throw new OSHDBKeytablesNotFoundException();
        }
        this.tagTranslator = new TagTranslator(this.keytables.getConnection());
      } catch (OSHDBKeytablesNotFoundException e) {
        LOG.error(e.getMessage());
        throw new RuntimeException(e);
      }
    }
    return this.tagTranslator;
  }

  // Helper that chains multiple oshEntity filters together
  protected OSHEntityFilter getPreFilter() {
    return this.preFilters.isEmpty()
        ? oshEntity -> true
        : oshEntity -> {
          for (SerializablePredicate<OSHEntity> filter : this.preFilters) {
            if (!filter.test(oshEntity)) {
              return false;
            }
          }
          return true;
        };
  }

  // Helper that chains multiple osmEntity filters together
  protected OSMEntityFilter getFilter() {
    return this.filters.isEmpty()
        ? osmEntity -> true
        : osmEntity -> {
          for (SerializablePredicate<OSMEntity> filter : this.filters) {
            if (!filter.test(osmEntity)) {
              return false;
            }
          }
          return true;
        };
  }

  // get all cell ids covered by the current area of interest's bounding box
  protected Iterable<CellIdRange> getCellIdRanges() {
    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
    if (this.bboxFilter == null
        || this.bboxFilter.getMinLon() >= this.bboxFilter.getMaxLon()
        || this.bboxFilter.getMinLat() >= this.bboxFilter.getMaxLat()) {
      // return an empty iterable if bbox is not set or empty
      LOG.warn("area of interest not set or empty");
      return Collections.emptyList();
    }
    return grid.bbox2CellIdRanges(this.bboxFilter, true);
  }

  // hack, so that we can use a variable that is of both Geometry and implements Polygonal (i.e.
  // Polygon or MultiPolygon) as required in further processing steps
  @SuppressWarnings("unchecked") // all setters only accept Polygonal geometries
  protected <P extends Geometry & Polygonal> P getPolyFilter() {
    return (P) this.polyFilter;
  }

  // concatenates all applied `map` functions
  private SerializableFunction<Object, X> getMapper() {
    // todo: maybe we can somehow optimize this?? at least for special cases like
    // this.mappers.size() == 1
    return (SerializableFunction<Object, X>) (data -> {
      // working with raw Objects since we don't know the actual intermediate types ¯\_(ツ)_/¯
      Object result = data;
      for (MapFunction mapper : this.mappers) {
        if (mapper.isFlatMapper()) {
          assert false : "flatMap callback requested in getMapper";
          throw new UnsupportedOperationException("cannot flat map this");
        } else {
          result = mapper.apply(result);
        }
      }
      @SuppressWarnings("unchecked")
      // after applying all mapper functions, the result type is X
      X mappedResult = (X) result;
      return mappedResult;
    });
  }

  // concatenates all applied `flatMap` and `map` functions
  private SerializableFunction<Object, Iterable<X>> getFlatMapper() {
    // todo: maybe we can somehow optimize this?? at least for special cases like
    // this.mappers.size() == 1
    return (SerializableFunction<Object, Iterable<X>>) (data -> {
      // working with raw objects since we don't know the actual intermediate types ¯\_(ツ)_/¯
      List<Object> results = new LinkedList<>();
      results.add(data);
      for (MapFunction mapper : this.mappers) {
        List<Object> newResults = new LinkedList<>();
        if (mapper.isFlatMapper()) {
          results.forEach(result ->
              Iterables.addAll(newResults, (Iterable<?>) mapper.apply(result))
          );
        } else {
          results.forEach(result -> newResults.add(mapper.apply(result)));
        }
        results = newResults;
      }
      @SuppressWarnings("unchecked")
      // after applying all mapper functions, the result type is List<X>
      Iterable<X> mappedResults = (Iterable<X>) results;
      return mappedResults;
    });
  }

  // gets list of timestamps to use for zerofilling
  Collection<OSHDBTimestamp> getZerofillTimestamps() {
    if (this.forClass.equals(OSMEntitySnapshot.class)) {
      return this.tstamps.get();
    } else {
      SortedSet<OSHDBTimestamp> result = new TreeSet<>(this.tstamps.get());
      result.remove(result.last());
      return result;
    }
  }

  // casts current results to a numeric type, for summing and averaging
  @Contract(pure = true)
  private MapReducer<Number> makeNumeric() {
    return this.map(MapReducer::checkAndMapToNumeric);
  }

  /**
   * Checks if an input object can be cast to numeric, and if possible returns it.
   *
   * @param x Arbitrary object
   * @return x casted to Numeric type
   * @throws UnsupportedOperationException if the supplied value is not numeric
   */
  @Contract(pure = true)
  static Number checkAndMapToNumeric(Object x) {
    // todo: slow??
    if (!Number.class.isInstance(x)) {
      throw new UnsupportedOperationException(
          "Cannot convert to non-numeric values of type: " + x.getClass().toString());
    }
    return (Number) x;
  }

  /**
   * Checks if the current request should be run on a cancelable backend.
   * Produces a log message if not.
    */
  private void checkTimeout() {
    if (this.oshdb.timeoutInMilliseconds().isPresent()) {
      if (!this.isCancelable()) {
        LOG.error("A query timeout was set but the database backend isn't cancelable");
      } else {
        this.timeout = this.oshdb.timeoutInMilliseconds().getAsLong();
      }
    }
  }

  @Contract(pure = true)
  static <T> List<T> collectIdentitySupplier() {
    return new LinkedList<>();
  }

  @Contract(pure = false)
  static <T> List<T> collectAccumulator(List<T> acc, T cur) {
    acc.add(cur);
    return acc;
  }

  @Contract(pure = true)
  static <T> List<T> collectCombiner(List<T> a, List<T> b) {
    ArrayList<T> combinedLists = new ArrayList<>(a.size() + b.size());
    combinedLists.addAll(a);
    combinedLists.addAll(b);
    return combinedLists;
  }

  @Contract(pure = true)
  static <T> Set<T> uniqIdentitySupplier() {
    return new HashSet<>();
  }

  @Contract(pure = false)
  static <T> Set<T> uniqAccumulator(Set<T> acc, T cur) {
    acc.add(cur);
    return acc;
  }

  @Contract(pure = true)
  static <T> Set<T> uniqCombiner(Set<T> a, Set<T> b) {
    HashSet<T> result = new HashSet<>((int) Math.ceil(Math.max(a.size(), b.size()) / 0.75));
    result.addAll(a);
    result.addAll(b);
    return result;
  }

  /**
   * Performs optimizations when filtering by a filter expression.
   *
   * <p>It is not always optimal to apply filter expressions directly "out of the box", because
   * it is using the flexible `osmEntityFilter` in the general case. If a filter expression can
   * be rewritten to use the more performant, but less flexible, OSHDB filters (i.e., `osmTag` or
   * `osmType`) this can result in a large performance boost.</p>
   *
   * <p>Currently, the following two optimizations are performed (but more could be feasibly be
   * added in the future:</p>
   *
   * <p><b>basic optimizations:</b> includes simple filter expressions witch can be directly
   * transformed to an (and-chain) of OSHDB filters (like OSM Tags or Types</p>
   *
   * @param mapRed the mapReducer whis the given filter was already applied on.
   * @param filter the filter to optimize.
   * @param <O> the type of the mapReducer to optimize (can be anything).
   * @return a mapReducer with the same semantics as the original one, after some optimizations
   *         were applied.
   */
  private <O> MapReducer<O> optimizeFilters(MapReducer<O> mapRed, FilterExpression filter) {
    // basic optimizations
    mapRed = optimizeFilters0(mapRed, filter);
    // more advanced optimizations that rely on analyzing the DNF of a filter expression
    mapRed = optimizeFilters1(mapRed, filter);
    return mapRed;
  }

  private <O> MapReducer<O> optimizeFilters0(MapReducer<O> mapRed, FilterExpression filter) {
    // basic optimizations (“low hanging fruit”):
    // single filters, and-combination of single filters, etc.
    if (filter instanceof TagFilterEquals) {
      return mapRed.osmTag(((TagFilterEquals) filter).getTag());
    } else if (filter instanceof TagFilterEqualsAny) {
      OSHDBTagKey key = ((TagFilterEqualsAny) filter).getTag();
      return mapRed.osmTag(key);
    } else if (filter instanceof TypeFilter) {
      return mapRed.osmTypeInternal(EnumSet.of(((TypeFilter) filter).getType()));
    } else if (filter instanceof AndOperator) {
      return optimizeFilters0(optimizeFilters0(mapRed,
          ((AndOperator) filter).getLeftOperand()),
          ((AndOperator) filter).getRightOperand());
    }
    return mapRed;
  }

  private <O> MapReducer<O> optimizeFilters1(MapReducer<O> mapRed, FilterExpression filter) {
    // more advanced optimizations that rely on analyzing the DNF of a filter expression
    List<List<Filter>> filterNormalized = filter.normalize();
    // collect all OSMTypes in all of the clauses
    EnumSet<OSMType> allTypes = EnumSet.noneOf(OSMType.class);
    for (List<Filter> andSubFilter : filterNormalized) {
      EnumSet<OSMType> subTypes = EnumSet.of(OSMType.NODE, OSMType.WAY, OSMType.RELATION);
      for (Filter subFilter : andSubFilter) {
        if (subFilter instanceof TypeFilter) {
          subTypes.retainAll(EnumSet.of(((TypeFilter) subFilter).getType()));
        } else if (subFilter instanceof GeometryTypeFilter) {
          subTypes.retainAll(((GeometryTypeFilter) subFilter).getOSMTypes());
        }
      }
      allTypes.addAll(subTypes);
    }
    mapRed = mapRed.osmType(allTypes);
    // (todo) intelligently group queried tags
    /*
     * here, we could optimize a few situations further: when a specific tag or key is used in all
     * branches of the filter: run mapRed.osmTag the set of tags which are present in any branches:
     * run mapRed.osmTag(list) (note that for this all branches need to have at least one
     * TagFilterEquals or TagFilterEqualsAny) related: https://github.com/GIScience/oshdb/pull/210
     */
    return mapRed;
  }
}
