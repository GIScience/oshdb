package org.heigit.bigspatialdata.oshdb.api.mapreducer;

import java.util.*;
import java.util.List;

import com.vividsolutions.jts.geom.Polygon;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.generic.*;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.*;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducer_Ignite;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducer_JDBC_multithread;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.MapReducer_JDBC_singlethread;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Ignite;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_JDBC;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamps;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.api.utils.ISODateTimeParser;
import org.heigit.bigspatialdata.oshdb.api.utils.OSHDBTimestampList;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.*;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class of oshdb's "functional programming" API.
 *
 * It accepts a list of filters, transformation `map` functions a produces a result when calling the `reduce` method (or one of its shorthand versions like `sum`, `count`, etc.).
 *
 * You can set a list of filters that are applied on the raw OSM data, for example you can filter:
 * <ul>
 *   <li>geometrically by an area of interest (bbox or polygon)</li>
 *   <li>by osm tags (key only or key/value)</li>
 *   <li>by OSM type</li>
 *   <li>custom filter callback</li>
 * </ul>
 *
 * Depending on the used data "view", the MapReducer produces either "snapshots" or evaluated all modifications ("contributions") of the matching raw OSM data.
 *
 * These data can then be transformed arbitrarily by user defined `map` functions (which take one of these entity snapshots or modifications as input an produce an arbitrary output) or `flatMap` functions (which can return an arbitrary number of results per entity snapshot/contribution).
 * It is possible to chain together any number of transformation functions.
 *
 * Finally, one can either use one of the pre-defined result-generating functions (e.g. `sum`, `count`, `average`, `uniq`), or specify a custom `reduce` procedure.
 *
 * If one wants to get results that are aggregated by timestamp (or some other index), one can use the `aggregateByTimestamp` or `aggregate` functionality that automatically handles the grouping of the output data.
 *
 * For more complex analyses, it is also possible to enable the grouping of the input data by the respective OSM ID. This can be used to view at the whole history of entities at once.
 *
 * @param <X> the type that is returned by the currently set of mapper function. the next added mapper function will be called with a parameter of this type as input
 */
public abstract class MapReducer<X> {
	private static final Logger LOG = LoggerFactory.getLogger(MapReducer.class);

  protected OSHDB _oshdb;
  protected OSHDB_JDBC _oshdbForTags;

  // internal state
  Class _forClass = null;
  private enum Grouping { NONE, BY_ID }
  private Grouping _grouping = Grouping.NONE;

  // utility objects
  private TagTranslator _tagTranslator = null;
  protected TagInterpreter _tagInterpreter = null;

  // settings and filters
  protected OSHDBTimestampList _tstamps = new OSHDBTimestamps("2008-01-01", (new OSHDBTimestamp((new Date()).getTime()/1000)).formatIsoDateTime(), OSHDBTimestamps.Interval.MONTHLY);
  protected BoundingBox _bboxFilter = null;
  protected Polygon _polyFilter = null;
  protected EnumSet<OSMType> _typeFilter = EnumSet.allOf(OSMType.class);
  private final List<SerializablePredicate<OSHEntity>> _preFilters = new ArrayList<>();
  private final List<SerializablePredicate<OSMEntity>> _filters = new ArrayList<>();
  private List<SerializableFunction> _mappers = new LinkedList<>();
  private Set<SerializableFunction> _flatMappers = new HashSet<>();

  // basic constructor
  protected MapReducer(OSHDB oshdb) {
    System.out.println((new OSHDBTimestamp((new Date()).getTime()/1000)).formatIsoDateTime());
    this._oshdb = oshdb;
  }

  /**
   * Factory function that creates the mapReducer object of the appropriate class for the chosen backend (e.g. JDBC based database or Ignite)
   *
   * @param oshdb the database backend (and potentially computation grid) to use for fetching and processing the data
   * @param forClass the class (same as template parameter &lt;T&gt;) to iterate over in the `mapping` function
   * @return
   */
  public static <X> MapReducer<X> using(OSHDB oshdb, Class<?> forClass) {
    if (oshdb instanceof OSHDB_JDBC) {
      MapReducer<X> mapReducer;
      if (((OSHDB_JDBC)oshdb).multithreading())
        mapReducer = new MapReducer_JDBC_multithread<X>(oshdb);
      else
        mapReducer = new MapReducer_JDBC_singlethread<X>(oshdb);
      mapReducer._oshdb = oshdb;
      mapReducer._oshdbForTags = (OSHDB_JDBC)oshdb;
      mapReducer._forClass = forClass;
      return mapReducer;
    } else if (oshdb instanceof OSHDB_Ignite) {
      MapReducer<X> mapReducer = new MapReducer_Ignite<X>(oshdb);
      mapReducer._oshdbForTags = null;
      mapReducer._forClass = forClass;
      return mapReducer;
    } else {
      throw new UnsupportedOperationException("No mapper implemented for your database type");
    }
  }

  // -------------------------------------------------------------------------------------------------------------------
  // "Setting" methods and associated internal helpers
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Sets the keytables database to use in the calculations to resolve strings (osm tags, roles) into internally used identifiers.
   * If this function is never called, the main database (specified during the construction of this object) is used for this.
   *
   * @param oshdb the database to use for resolving strings into internal identifiers
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> keytables(OSHDB_JDBC oshdb) {
    this._oshdbForTags = oshdb;
    return this;
  }

  /**
   * Helper that resolves an OSM tag key string into an internal identifier (using the keytables database).
   *
   * @param key the osm tag key string (e.g. "building")
   * @return the identifier of this tag key
   * @throws Exception
   */
  protected Integer getTagKeyId(String key) throws Exception {
    if (this._tagTranslator == null) this._tagTranslator = new TagTranslator((this._oshdbForTags).getConnection());
    return this._tagTranslator.key2Int(key);
  }

  /**
   * Helper that resolves an OSM tag (key and value) string into an internal identifier (using the keytables database).
   *
   * @param key the osm tag key string (e.g. "highway")
   * @param value the osm tag value string (e.g. "residential")
   * @return the key's and value's identifiers of this tag key and value respectively
   * @throws Exception
   */
  protected Pair<Integer, Integer> getTagValueId(String key, String value) throws Exception {
    if (this._tagTranslator == null) this._tagTranslator = new TagTranslator((this._oshdbForTags).getConnection());
    return this._tagTranslator.tag2Int(new ImmutablePair(key,value));
  }

  /**
   * Sets the tagInterpreter to use in the analysis.
   * The tagInterpreter is used internally to determine the geometry type of osm entities (e.g. an osm way can become either
   * a LineString or a Polygon, depending on its tags).
   * Normally, this is generated automatically for the user. But for example, if  one doesn't want to use the DefaultTagInterpreter,
   * it is possible to use this function to supply their own tagInterpreter.
   *
   * @param tagInterpreter the tagInterpreter object to use in the processing of osm entities
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> tagInterpreter(TagInterpreter tagInterpreter) {
    this._tagInterpreter = tagInterpreter;
    return this;
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Filtering methods
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Set the area of interest to the given bounding box. Deprecated, use `areaOfInterest()` instead (w/ same semantics).
   *
   * @param bbox the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  @Deprecated
  public MapReducer<X> boundingBox(BoundingBox bbox) {
    return this.areaOfInterest(bbox);
  }

  /**
   * Set the area of interest to the given bounding box.
   * Only objects inside or clipped by this bbox will be passed on to the analysis' `mapper` function.
   *
   * @param bboxFilter the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> areaOfInterest(BoundingBox bboxFilter) {
    if (this._polyFilter == null) {
      if (this._bboxFilter == null)
        this._bboxFilter = bboxFilter;
      else
        this._bboxFilter = BoundingBox.intersect(bboxFilter, this._bboxFilter);
    } else {
      this._polyFilter = (Polygon)Geo.clip(this._polyFilter, bboxFilter);
      this._bboxFilter = new BoundingBox(this._polyFilter.getEnvelopeInternal());
    }
    return this;
  }

  /**
   * Set the area of interest to the given polygon.
   * Only objects inside or clipped by this polygon will be passed on to the analysis' `mapper` function.
   *
   * @param polygonFilter the bounding box to query the data in
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> areaOfInterest(Polygon polygonFilter) {
    if (this._polyFilter == null) {
      if (this._bboxFilter == null)
        this._polyFilter = polygonFilter;
      else
        this._polyFilter = (Polygon)Geo.clip(polygonFilter, this._bboxFilter);
    } else {
      this._polyFilter = (Polygon)Geo.clip(polygonFilter, this._polyFilter);
    }
    this._bboxFilter = new BoundingBox(this._polyFilter.getEnvelopeInternal());
    return this;
  }

  /**
   * Set the timestamps for which to perform the analysis.
   *
   * Depending on the *View*, this has slightly different semantics:
   * * For the OSMEntitySnapshotView it will set the time slices at which to take the "snapshots"
   * * For the OSMContributionView it will set the time interval in which to look for osm contributions (only the first and last
   *   timestamp of this list are contributing). Additionally, the timestamps are used in the `aggregateByTimestamps` functionality.
   *
   * @param tstamps an object (implementing the OSHDBTimestampList interface) which provides the timestamps to do the analysis for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> timestamps(OSHDBTimestampList tstamps) {
    this._tstamps = tstamps;
    return this;
  }

  /**
   * Set the timestamps for which to perform the analysis in a regular interval between a start and end date.
   *
   * See {@link #timestamps(OSHDBTimestampList)} for further information.
   *
   * @param isoDateStart an ISO 8601 date string representing the start date of the analysis
   * @param isoDateEnd an ISO 8601 date string representing the end date of the analysis
   * @param isoDateStart an ISO 8601 period string representing the (regular) interval between the timestamps to be used in the analysis; for example "P1M" can be used for a monthly interval
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> timestamps(String isoDateStart, String isoDateEnd, String isoPeriod) {
    this._tstamps = new OSHDBTimestamps(isoDateStart, isoDateEnd, isoPeriod);
    return this;
  }

  /**
   * Set the timestamps for which to perform the analysis in a regular interval between a start and end date.
   *
   * See {@link #timestamps(OSHDBTimestampList)} for further information.
   *
   * @param isoDateStart an ISO 8601 date string representing the start date of the analysis
   * @param isoDateEnd an ISO 8601 date string representing the end date of the analysis
   * @param isoDateStart the interval between the timestamps to be used in the analysis
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> timestamps(String isoDateStart, String isoDateEnd, OSHDBTimestamps.Interval interval) {
    this._tstamps = new OSHDBTimestamps(isoDateStart, isoDateEnd, interval);
    return this;
  }

  /**
   * Sets two timestamps (start and end date) for which to perform the analysis.
   *
   * Useful in combination with the OSMContributionView when not performing further aggregation by timestamp.
   *
   * See {@link #timestamps(OSHDBTimestampList)} for further information.
   *
   * @param isoDateStart an ISO 8601 date string representing the start date of the analysis
   * @param isoDateEnd an ISO 8601 date string representing the end date of the analysis
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> timestamps(String isoDateStart, String isoDateEnd) {
    this._tstamps = new OSHDBTimestamps(isoDateStart, isoDateEnd);
    return this;
  }

  /**
   * Sets multiple arbitrary timestamps for which to perform the analysis.
   *
   * See {@link #timestamps(OSHDBTimestampList)} for further information.
   *
   * @param isoDateFirst an ISO 8601 date string representing the start date of the analysis
   * @param isoDateMore more ISO 8601 date strings representing the remaining timestamps of the analysis
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> timestamps(String isoDateFirst, String... isoDateMore) {
    List<Long> timestamps = new ArrayList<>(2 + isoDateMore.length);
    try {
      timestamps.add(ISODateTimeParser.parseISODateTime(isoDateFirst).toEpochSecond());
      for (String isoDate : isoDateMore) {
        timestamps.add(ISODateTimeParser.parseISODateTime(isoDate).toEpochSecond());
      }
    } catch(Exception e) {
      LOG.error("unable to parse ISO date string: " + e.getMessage());
    }
    Collections.sort(timestamps);
    this._tstamps = () -> timestamps;
    return this;
  }

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param typeFilter the set of osm types to filter (e.g. `EnumSet.of(OSMType.WAY)`)
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> osmTypes(EnumSet<OSMType> typeFilter) {
    this._typeFilter = typeFilter;
    return this;
  }

  /**
   * Limits the analysis to the given osm entity types.
   *
   * @param type1 the set of osm types to filter (e.g. `OSMType.NODE`)
   * @param otherTypes more osm types which should be analyzed
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> osmTypes(OSMType type1, OSMType ...otherTypes) {
    return this.osmTypes(EnumSet.of(type1, otherTypes));
  }

  /**
   * Adds a custom arbitrary filter that gets executed for each osm entity and determines if it should be considered for this analyis or not.
   *
   * @param f the filter function to call for each osm entity
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> where(SerializablePredicate<OSMEntity> f) {
    this._filters.add(f);
    return this;
  }

  /**
   * Adds a custom arbitrary filter that gets executed for each osm entity and determines if it should be considered for this analyis or not.
   *
   * Deprecated, use `where(f)` instead
   *
   * @param f the filter function to call for each osm entity
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @deprecated
   */
  @Deprecated
  public MapReducer<X> filterByOSMEntity(SerializablePredicate<OSMEntity> f) {
    return this.where(f);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key (with an arbitrary value).
   *
   * @param key the tag key to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @throws Exception
   */
  public MapReducer<X> where(String key) throws Exception {
    Integer keyId = this.getTagKeyId(key);
    if (keyId == null) {
      LOG.warn("Tag key \"{}\" not found. No data will match this filter.", key);
      this._preFilters.add(ignored -> false);
      this._filters.add(ignored -> false);
      return this;
    }
    this._preFilters.add(oshEntitiy -> oshEntitiy.hasTagKey(keyId));
    this._filters.add(osmEntity -> osmEntity.hasTagKey(keyId));
    return this;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key (with an arbitrary value).
   *
   * Deprecated, use `where(key)` instead.
   *
   * @param key the tag key to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @throws Exception
   * @deprecated
   */
  @Deprecated
  public MapReducer<X> filterByTag(String key) throws Exception {
    return this.where(key);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key (with an arbitrary value).
   *
   * Deprecated, use `where(key)` instead.
   *
   * @param key the tag key to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @throws Exception
   * @deprecated
   */
  @Deprecated
  public MapReducer<X> filterByTagKey(String key) throws Exception {
    return this.where(key);
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key and value.
   *
   * @param key the tag key to filter the osm entities for
   * @param value the tag value to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @throws Exception
   */
  public MapReducer<X> where(String key, String value) throws Exception {
    Pair<Integer, Integer> keyValueId = this.getTagValueId(key, value);
    if (keyValueId == null) {
      LOG.warn("Tag key \"{}\" not found. No data will match this filter.", key);
      this._preFilters.add(ignored -> false);
      this._filters.add(ignored -> false);
      return this;
    }
    int keyId = keyValueId.getKey();
    int valueId = keyValueId.getValue();
    this._filters.add(osmEntity -> osmEntity.hasTagValue(keyId, valueId));
    return this;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key and value.
   *
   * Deprecated, use `where(key, value)` instead.
   *
   * @param key the tag key to filter the osm entities for
   * @param value the tag value to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @throws Exception
   * @deprecated
   */
  public MapReducer<X> filterByTag(String key, String value) throws Exception {
    Pair<Integer, Integer> keyValueId = this.getTagValueId(key, value);
    if (keyValueId == null) {
      LOG.warn("Tag key \"{}\" not found. No data will match this filter.", key);
      this._preFilters.add(ignored -> false);
      this._filters.add(ignored -> false);
      return this;
    }
    int keyId = keyValueId.getKey();
    int valueId = keyValueId.getValue();
    this._filters.add(osmEntity -> osmEntity.hasTagValue(keyId, valueId));
    return this;
  }

  /**
   * Adds an osm tag filter: The analysis will be restricted to osm entities that have this tag key and value.
   *
   * Deprecated, use `where(key, value)` instead.
   *
   * @param key the tag key to filter the osm entities for
   * @param value the tag value to filter the osm entities for
   * @return `this` mapReducer (can be used to chain multiple commands together)
   * @throws Exception
   * @deprecated
   */
  @Deprecated
  public MapReducer<X> filterByTagValue(String key, String value) throws Exception {
    return this.where(key, value);
  }

  // -------------------------------------------------------------------------------------------------------------------
  // "map", "flatMap" transformation methods
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Set an arbitrary `map` transformation function.
   *
   * @param mapper function that will be applied to each data entry (osm entity snapshot or contribution)
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return the MapReducer object operating on the transformed type (&lt;R&gt;)
   */
  public <R> MapReducer<R> map(SerializableFunction<X, R> mapper) {
    this._mappers.add(mapper);
    return (MapReducer<R>)this;
  }

  /**
   * Set an arbitrary `flatMap` transformation function, which returns list with an arbitrary number of results per input data entry.
   * The results of this function will be "flattened", meaning that they can be for example transformed again by setting additional `map` functions.
   *
   * @param flatMapper function that will be applied to each data entry (osm entity snapshot or contribution) and returns a list of results
   * @param <R> an arbitrary data type which is the return type of the transformation `map` function
   * @return the MapReducer object operating on the transformed type (&lt;R&gt;)
   */
  public <R> MapReducer<R> flatMap(SerializableFunction<X, List<R>> flatMapper) {
    this._mappers.add(flatMapper);
    this._flatMappers.add(flatMapper);
    return (MapReducer<R>)this;
  }

  /**
   * Adds a custom arbitrary filter that gets executed in the current transformation chain.
   *
   * @param f the filter function that determines if the respective data should be passed on (when f returns true) or discarded (when f returns false)
   * @return `this` mapReducer (can be used to chain multiple commands together)
   */
  public MapReducer<X> filter(SerializablePredicate<X> f) {
    return this.flatMap(data -> f.test(data)
        ? Collections.singletonList(data)
        : Collections.emptyList()
    );
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Grouping and Aggregation
  // Sets how the input data is "grouped", or the output data is "aggregated" into separate chunks.
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Groups the input data (osm entity snapshot or contributions) by their respective entity's ids before feeding them into further transformation functions.
   * This can be used to do more complex analysis on the osm data, that requires one to know about the full editing history of individual osm entities.
   *
   * This needs to be called before any `map` or `flatMap` transformation functions have been set. Otherwise a runtime exception will be thrown.
   *
   * @return the MapReducer object which applies its transformations on (by entity id grouped) lists of the input data
   * @throws UnsupportedOperationException if this is called after some map (or flatMap) functions have already been set
   * @throws UnsupportedOperationException if this is called when a grouping has already been activated
   */
  public MapReducer<List<X>> groupById() throws UnsupportedOperationException {
    if (!this._mappers.isEmpty())
      throw new UnsupportedOperationException("groupById() must be called before any `map` or `flatMap` transformation functions have been set");
    if (this._grouping != Grouping.NONE)
      throw new UnsupportedOperationException("A grouping is already active on this MapReducer");
    this._grouping = Grouping.BY_ID;
    return (MapReducer<List<X>>)(this);
  }

  /**
   * Sets a custom aggregation function that is used to group output results into.
   *
   * @param indexer a function that will be called for each input element and returns a value that will be used to group the results by
   * @param <U> the data type of the values used to aggregate the output. has to be a comparable type
   * @return a MapAggregator object with the equivalent state (settings, filters, map function, etc.) of the current MapReducer object
   */
  public <U extends Comparable> MapAggregator<U, X> aggregate(SerializableFunction<X, U> indexer) {
    return new MapAggregator<U, X>(this, indexer);
  }

  /**
   * Sets up aggregation by timestamp.
   *
   * In the OSMEntitySnapshotView, the snapshots' timestamp will be used directly to aggregate results into.
   * In the OSMContributionView, the timestamps of the respective data modifications will be matched to corresponding time intervals (that are defined by the `timestamps` setting here).
   *
   * Cannot be used together with the `groupById()` setting enabled.
   *
   * @return a MapAggregator object with the equivalent state (settings, filters, map function, etc.) of the current MapReducer object
   * @throws UnsupportedOperationException if this is called when the `groupById()` mode has been activated
   */
  public MapAggregator<OSHDBTimestamp, X> aggregateByTimestamp() throws UnsupportedOperationException {
    if (this._grouping != Grouping.NONE)
      throw new UnsupportedOperationException("aggregateByTimestamp cannot be used together with the groupById() functionality");

    final List<OSHDBTimestamp> timestamps = this._tstamps.getOSHDBTimestamps();

    // by timestamp indexing function -> for some data views we need to match the input data to the list
    SerializableFunction<X, OSHDBTimestamp> indexer;
    if (this._forClass.equals(OSMContribution.class)) {
      indexer = data -> {
        int timeBinIndex = Collections.binarySearch(timestamps, ((OSMContribution)data).getTimestamp());
        if (timeBinIndex < 0) { timeBinIndex = -timeBinIndex - 2; }
        return timestamps.get(timeBinIndex);
      };
    } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
      switch (this._grouping) {
        case BY_ID:
          indexer = data -> ((List<OSMEntitySnapshot>)data).get(0).getTimestamp();
          break;
        case NONE:
        default:
          indexer = data -> ((OSMEntitySnapshot)data).getTimestamp();
          break;
      }
    } else throw new UnsupportedOperationException("aggregateByTimestamp only implemented for OSMContribution and OSMEntitySnapshot");

    if (this._mappers.size() > 0) {
      // for convenience we allow one to set this function even after some map functions were set.
      // if some map / flatMap functions were alredy set:
      // "rewind" them first, apply the indexer and then re-apply the map/flatMap functions accordingly
      List<SerializableFunction> mappers = new LinkedList<>(this._mappers);
      Set<SerializableFunction> flatMappers = new HashSet<>(this._flatMappers);
      this._mappers.clear();
      this._flatMappers.clear();
      MapAggregatorByTimestamps<X> mapAggregator = new MapAggregatorByTimestamps<X>(this, indexer);
      mappers.forEach(action -> {
        if (flatMappers.contains(action))
          mapAggregator.flatMap(action);
        else
          mapAggregator.map(action);
      });
      return mapAggregator;
    } else {
      return new MapAggregatorByTimestamps<X>(this, indexer);
    }
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Exposed generic reduce.
  // Can be used by experienced users of the api to implement complex queries.
  // These offer full flexibility, but are potentially a bit tricky to work with (see javadoc).
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Generic map-reduce routine
   *
   * The combination of the used types and identity/reducer functions must make "mathematical" sense:
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function: `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a> interface.
   *
   * @param identitySupplier a factory function that returns a new starting value to reduce results into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;) and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter (mutate) the state of the accumulation value (e.g. directly adding new values to an existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function must be pure (have no side effects), and is not allowed to alter the state of the two input objects it gets!</b>
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the `combiner` function, after all `mapper` results have been aggregated (in the `accumulator` and `combiner` steps)
   * @throws Exception
   */
  public <S> S reduce(SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, X, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    switch (this._grouping) {
      case NONE:
        if (this._flatMappers.size() == 0) {
          final SerializableFunction<Object, X> mapper = this._getMapper();
          if (this._forClass.equals(OSMContribution.class)) {
            return this.mapReduceCellsOSMContribution((SerializableFunction<OSMContribution, X>)mapper::apply, identitySupplier, accumulator, combiner);
          } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
            return this.mapReduceCellsOSMEntitySnapshot((SerializableFunction<OSMEntitySnapshot, X>)mapper::apply, identitySupplier, accumulator, combiner);
          } else throw new UnsupportedOperationException("Unimplemented data view: " + this._forClass.toString());
        } else {
          final SerializableFunction<Object, List<X>> flatMapper = this._getFlatMapper();
          if (this._forClass.equals(OSMContribution.class)) {
            return this.flatMapReduceCellsOSMContributionGroupedById((List<OSMContribution> inputList) -> {
              List<X> outputList = new LinkedList<>();
              inputList.stream().map((SerializableFunction<OSMContribution, List<X>>)flatMapper::apply).forEach(outputList::addAll);
              return outputList;
            }, identitySupplier, accumulator, combiner);
          } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
            return this.flatMapReduceCellsOSMEntitySnapshotGroupedById((List<OSMEntitySnapshot> inputList) -> {
              List<X> outputList = new LinkedList<>();
              inputList.stream().map((SerializableFunction<OSMEntitySnapshot, List<X>>)flatMapper::apply).forEach(outputList::addAll);
              return outputList;
            }, identitySupplier, accumulator, combiner);
          } else throw new UnsupportedOperationException("Unimplemented data view: " + this._forClass.toString());
        }
      case BY_ID:
        final SerializableFunction<Object, List<X>> flatMapper;
        if (this._flatMappers.size() == 0) {
          final SerializableFunction<Object, X> mapper = this._getMapper();
          flatMapper = data -> Collections.singletonList(mapper.apply(data)); // todo: check if this is actually necessary, doesn't getFlatMapper() do the "same" in this case? should we add this as optimization case to getFlatMapper()??
        } else {
          flatMapper = this._getFlatMapper();
        }
        if (this._forClass.equals(OSMContribution.class)) {
          return this.flatMapReduceCellsOSMContributionGroupedById((SerializableFunction<List<OSMContribution>, List<X>>)flatMapper::apply, identitySupplier, accumulator, combiner);
        } else if (this._forClass.equals(OSMEntitySnapshot.class)) {
          return this.flatMapReduceCellsOSMEntitySnapshotGroupedById((SerializableFunction<List<OSMEntitySnapshot>, List<X>>)flatMapper::apply, identitySupplier, accumulator, combiner);
        } else throw new UnsupportedOperationException("Unimplemented data view: " + this._forClass.toString());
      default:
        throw new UnsupportedOperationException("Unsupported grouping: " + this._grouping.toString());
    }
  }

  // -------------------------------------------------------------------------------------------------------------------
  // "Quality of life" helper methods to use the map-reduce functionality more directly and easily for typical queries.
  // Available are: sum, count, average, weightedAverage and uniq.
  // Each one can be used to get results aggregated by timestamp, aggregated by a custom index and not aggregated totals.
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Sums up the results.
   *
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime exception will be thrown.
   *
   * @return the sum of the current data
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   * @throws Exception
   */
  public Number sum() throws Exception {
    return this
        .makeNumeric()
        .reduce(
            () -> 0,
            (SerializableBiFunction<Number, Number, Number>) (x, y) -> NumberUtils.add(x, y),
            (SerializableBinaryOperator<Number>) (x, y) -> NumberUtils.add(x, y)
        );
  }

  /**
   * Sums up the results provided by a given `mapper` function.
   *
   * This is a shorthand for `.map(mapper).sum()`, with the difference that here the numerical return type of the `mapper` is ensured.
   *
   * @param mapper function that returns the numbers to sum up
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the summed up results of the `mapper` function
   * @throws Exception
   */
  public <R extends Number> R sum(SerializableFunction<X, R> mapper) throws Exception {
    return this
        .map(mapper)
        .reduce(
            () -> (R) (Integer) 0,
            (SerializableBiFunction<R, R, R>)(x,y) -> NumberUtils.add(x,y),
            (SerializableBinaryOperator<R>)(x,y) -> NumberUtils.add(x,y)
        );
  }

  /**
   * Counts the number of results.
   *
   * @return the total count of features or modifications, summed up over all timestamps
   * @throws Exception
   */
  public Integer count() throws Exception {
    return this.sum(ignored -> 1);
  }

  /**
   * Gets all unique values of the results.
   *
   * For example, this can be used together with the OSMContributionView to get the total amount of unique users editing specific feature types.
   *
   * @return the set of distinct values
   * @throws Exception
   */
  public Set<X> uniq() throws Exception {
    return this
        .reduce(
            HashSet::new,
            (acc, cur) -> { acc.add(cur); return acc; },
            (a,b) -> { HashSet<X> result = new HashSet<>(a); result.addAll(b); return result; }
        );
  }

  /**
   * Gets all unique values of the results provided by a given mapper function.
   *
   * This is a shorthand for `.map(mapper).uniq()`.
   *
   * @param mapper function that returns some values
   * @param <R> the type that is returned by the `mapper` function
   * @return a set of distinct values returned by the `mapper` function
   * @throws UnsupportedOperationException if the data cannot be cast to numbers
   * @throws Exception
   */
  public <R> Set<R> uniq(SerializableFunction<X, R> mapper) throws Exception {
    return this.map(mapper).uniq();
  }

  /**
   * Calculates the averages of the results.
   *
   * The current data values need to be numeric (castable to "Number" type), otherwise a runtime exception will be thrown.
   *
   * @return the average of the current data
   * @throws Exception
   */
  public Double average() throws Exception {
    return this
        .makeNumeric()
        .average(n -> n);
  }

  /**
   * Calculates the average of the results provided by a given `mapper` function.
   *
   * @param mapper function that returns the numbers to average
   * @param <R> the numeric type that is returned by the `mapper` function
   * @return the average of the numbers returned by the `mapper` function
   * @throws Exception
   */
  public <R extends Number> Double average(SerializableFunction<X, R> mapper) throws Exception {
    PayloadWithWeight<Double> runningSums = this
        .map(mapper)
        .reduce(
            () -> new PayloadWithWeight<Double>(0.0,0.0),
            (acc, cur) -> {
              acc.num = NumberUtils.add(acc.num, cur.doubleValue());
              acc.weight += 1;
              return acc;
            },
            (a, b) -> new PayloadWithWeight<>(NumberUtils.add(a.num, b.num), a.weight+b.weight)
        );
    return runningSums.num / runningSums.weight;
  }

  /**
   * Calculates the weighted average of the results provided by the `mapper` function.
   *
   * The mapper must return an object of the type `WeightedValue` which contains a numeric value associated with a (floating point) weight.
   *
   * @param mapper function that gets called for each entity snapshot or modification, needs to return the value and weight combination of numbers to average
   * @return the weighted average of the numbers returned by the `mapper` function
   * @throws Exception
   */
  public Double weightedAverage(SerializableFunction<X, WeightedValue> mapper) throws Exception {
    PayloadWithWeight<Double> runningSums = this
        .map(mapper)
        .reduce(
            () -> new PayloadWithWeight<>(0.0,0.0),
            (acc, cur) -> {
              acc.num = NumberUtils.add(acc.num, cur.getValue().doubleValue());
              acc.weight += cur.getWeight();
              return acc;
            },
            (a, b) -> new PayloadWithWeight<>(NumberUtils.add(a.num, b.num), a.weight+b.weight)
        );
    return runningSums.num / runningSums.weight;
  }

  // -------------------------------------------------------------------------------------------------------------------
  // "Iterator" like helpers (forEach, collect), mostly intended for testing purposes
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Iterates over each entity snapshot or contribution, and performs a single `action` on each one of them.
   *
   * This method can be handy for testing purposes. But note that since the `action` doesn't produce a return value, it must facilitate its own way of producing output.
   *
   * @param action function that gets called for each transformed data entry
   * @throws Exception
   */
  @Deprecated
  public void forEach(SerializableConsumer<X> action) throws Exception {
    this.map(data -> { action.accept(data); return null; }).reduce(() -> null, (ignored, ignored2) -> null, (ignored, ignored2) -> null);
  }

  /**
   * Collects all results into a List
   *
   * @return a list with all results returned by the `mapper` function
   * @throws Exception
   */
  public List<X> collect() throws Exception {
    return this.reduce(
        LinkedList::new,
        (acc, cur) -> { acc.add(cur); return acc; },
        (list1, list2) -> { LinkedList<X> combinedLists = new LinkedList<>(list1); combinedLists.addAll(list2); return combinedLists; }
    );
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Generic map-reduce functions (internal).
  // These need to be implemented by the actual db/processing backend!
  // -------------------------------------------------------------------------------------------------------------------

  /**
   * Generic map-reduce used by the `OSMContributionView`
   *
   * The combination of the used types and identity/reducer functions must make "mathematical" sense:
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function: `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a> interface.
   *
   * @param mapper a function that's called for each `OSMContribution`
   * @param identitySupplier a factory function that returns a new starting value to reduce results into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;) and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter (mutate) the state of the accumulation value (e.g. directly adding new values to an existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function must be pure (have no side effects), and is not allowed to alter the state of the two input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the `combiner` function, after all `mapper` results have been aggregated (in the `accumulator` and `combiner` steps)
   * @throws Exception
   */
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }

  /**
   * Generic "flat" version of the map-reduce used by the `OSMContributionView`, with by-osm-id grouped input to the `mapper` function
   *
   * Contrary to the "normal" map-reduce, the "flat" version adds the possibility to return any number of results in the `mapper` function. Additionally, this interface provides the `mapper` function with a list of all `OSMContribution`s of a particular OSM entity. This is used to do more complex analyses that require the full edit history of the respective OSM entities as input.
   *
   * The combination of the used types and identity/reducer functions must make "mathematical" sense:
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function: `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a> interface.
   *
   * @param mapper a function that's called for all `OSMContribution`s of a particular OSM entity; returns a list of results (which can have any number of entries).
   * @param identitySupplier a factory function that returns a new starting value to reduce results into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;) and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter (mutate) the state of the accumulation value (e.g. directly adding new values to an existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function must be pure (have no side effects), and is not allowed to alter the state of the two input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the `combiner` function, after all `mapper` results have been aggregated (in the `accumulator` and `combiner` steps)
   * @throws Exception
   */
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(SerializableFunction<List<OSMContribution>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }

  /**
   * Generic map-reduce used by the `OSMEntitySnapshotView`
   *
   * The combination of the used types and identity/reducer functions must make "mathematical" sense:
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function: `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a> interface.
   *
   * @param mapper a function that's called for each `OSMEntitySnapshot`
   * @param identitySupplier a factory function that returns a new starting value to reduce results into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;) and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter (mutate) the state of the accumulation value (e.g. directly adding new values to an existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function must be pure (have no side effects), and is not allowed to alter the state of the two input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the `combiner` function, after all `mapper` results have been aggregated (in the `accumulator` and `combiner` steps)
   * @throws Exception
   */
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }

  /**
   * Generic "flat" version of the map-reduce used by the `OSMEntitySnapshotView`, with by-osm-id grouped input to the `mapper` function
   *
   * Contrary to the "normal" map-reduce, the "flat" version adds the possibility to return any number of results in the `mapper` function. Additionally, this interface provides the `mapper` function with a list of all `OSMContribution`s of a particular OSM entity. This is used to do more complex analyses that require the full list of snapshots of the respective OSM entities as input.
   *
   * The combination of the used types and identity/reducer functions must make "mathematical" sense:
   * <ul>
   *   <li>the accumulator and combiner functions need to be associative,</li>
   *   <li>values generated by the identitySupplier factory must be an identity for the combiner function: `combiner(identitySupplier(),x)` must be equal to `x`,</li>
   *   <li>the combiner function must be compatible with the accumulator function: `combiner(u, accumulator(identitySupplier(), t)) == accumulator.apply(u, t)`</li>
   * </ul>
   *
   * Functionally, this interface is similar to Java8 Stream's <a href="https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-">reduce(identity,accumulator,combiner)</a> interface.
   *
   * @param mapper a function that's called for all `OSMEntitySnapshot`s of a particular OSM entity; returns a list of results (which can have any number of entries)
   * @param identitySupplier a factory function that returns a new starting value to reduce results into (e.g. when summing values, one needs to start at zero)
   * @param accumulator a function that takes a result from the `mapper` function (type &lt;R&gt;) and an accumulation value (type &lt;S&gt;, e.g. the result of `identitySupplier()`) and returns the "sum" of the two; contrary to `combiner`, this function is allowed to alter (mutate) the state of the accumulation value (e.g. directly adding new values to an existing Set object)
   * @param combiner a function that calculates the "sum" of two &lt;S&gt; values; <b>this function must be pure (have no side effects), and is not allowed to alter the state of the two input objects it gets!</b>
   * @param <R> the data type returned by the `mapper` function
   * @param <S> the data type used to contain the "reduced" (intermediate and final) results
   * @return the result of the map-reduce operation, the final result of the last call to the `combiner` function, after all `mapper` results have been aggregated (in the `accumulator` and `combiner` steps)
   * @throws Exception
   */
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    throw new UnsupportedOperationException("Reduce function not yet implemented");
  }

  // -------------------------------------------------------------------------------------------------------------------
  // Some helper methods for internal use in the mapReduce functions
  // -------------------------------------------------------------------------------------------------------------------

  // Helper that chains multiple oshEntity filters together
  protected SerializablePredicate<OSHEntity> _getPreFilter() {
    return (this._preFilters.isEmpty()) ? (oshEntity -> true) : (oshEntity -> {
      for (SerializablePredicate<OSHEntity> filter : this._preFilters)
        if (!filter.test(oshEntity))
          return false;
      return true;
    });
  }

  // Helper that chains multiple osmEntity filters together
  protected SerializablePredicate<OSMEntity> _getFilter() {
    return (this._filters.isEmpty()) ? (osmEntity -> true) : (osmEntity -> {
      for (SerializablePredicate<OSMEntity> filter : this._filters)
        if (!filter.test(osmEntity))
          return false;
      return true;
    });
  }

  // get all cell ids covered by the current area of interest's bounding box
  protected Iterable<CellId> _getCellIds() {
    XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
    if (this._bboxFilter == null || (this._bboxFilter.minLon >= this._bboxFilter.maxLon || this._bboxFilter.minLat >= this._bboxFilter.maxLat)) {
      // return an empty iterable if bbox is not set or empty
      LOG.warn("area of interest not set or empty");
      return Collections.emptyList();
    }
    return grid.bbox2CellIds(this._bboxFilter, true);
  }

  // concatenates all applied `map` functions
  private SerializableFunction<Object, X> _getMapper() {
    // todo: maybe we can somehow optimize this?? at least for special cases like this._mappers.size() == 1
    return (SerializableFunction<Object, X>)(data -> {
      Object result = data;
      for (SerializableFunction mapper: this._mappers) {
        if (this._flatMappers.contains(mapper)) {
          throw new UnsupportedOperationException("cannot flat map this");
        } else {
          result = mapper.apply(result);
        }
      }
      return (X)result;
    });
  }

  // concatenates all applied `flatMap` and `map` functions
  private SerializableFunction<Object, List<X>> _getFlatMapper() {
    // todo: maybe we can somehow optimize this?? at least for special cases like this._mappers.size() == 1
    return (SerializableFunction<Object, List<X>>)(data -> {
      List<Object> results = new LinkedList<>();
      results.add(data);
      for (SerializableFunction mapper: this._mappers) {
        List<Object> newResults = new LinkedList<>();
        if (this._flatMappers.contains(mapper)) {
          results.forEach(result -> newResults.addAll((List<Object>)mapper.apply(result)));
        } else {
          results.forEach(result -> newResults.add(mapper.apply(result)));
        }
        results = newResults;
      }
      return (List<X>)results;
    });
  }

  // casts current results to a numeric type, for summing and averaging
  private MapReducer<Number> makeNumeric() {
    return this.map(x -> {
      if (!Number.class.isInstance(x)) // todo: slow??
        throw new UnsupportedOperationException("Cannot convert to non-numeric values of type: " + x.getClass().toString());
      return (Number)x;
    });
  }
}

// -------------------------------------------------------------------------------------------------------------------
// Auxiliary classes and interfaces
// -------------------------------------------------------------------------------------------------------------------

// mutable version of WeightedValue type (for internal use to do faster aggregation)
class PayloadWithWeight<X> {
  X num;
  double weight;
  PayloadWithWeight(X num, double weight) {
    this.num = num;
    this.weight = weight;
  }
}