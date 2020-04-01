package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Streams;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteRunnable;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.Kernels.CellProcessor;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.OSHDBIgniteMapReduceComputeTask.CancelableIgniteMapReduceJob;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.OSHEntityFilter;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.OSMEntityFilter;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.util.taginterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.update.UpdateDbHelper;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.roaringbitmap.longlong.LongBitmapDataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@inheritDoc}
 *
 * <p>
 * The "ScanQuery" implementation is an implementation of the oshdb mapreducer on Ignite, which
 * always scans over the whole data set when running queries. It might offer better performance for
 * global (or almost global) queries. In other situations it should not be used.
 * </p>
 */
public class MapReducerIgniteScanQuery<X> extends MapReducer<X> {
  public MapReducerIgniteScanQuery(OSHDBDatabase oshdb,
      Class<? extends OSHDBMapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducerIgniteScanQuery(MapReducerIgniteScanQuery obj) {
    super(obj);
  }

  static boolean cellKeyInRange(
      Long cellKey, Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel
  ) {
    CellId cellId = CellId.fromLevelId(cellKey);
    int level = cellId.getZoomLevel();
    long id = cellId.getId();
    if (!cellIdRangesByLevel.containsKey(level)) {
      return false;
    }
    Entry<Long, CellIdRange> cellIdRangeEntry =
        cellIdRangesByLevel.get(level).floorEntry(id);
    if (cellIdRangeEntry == null) {
      return false;
    }
    CellIdRange cellIdRange = cellIdRangeEntry.getValue();
    return cellIdRange.getStart().getId() <= id && cellIdRange.getEnd().getId() >= id;
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducerIgniteScanQuery<X>(this);
  }

  @Override
  public boolean isCancelable() {
    return true;
  }

  // === map-reduce operations ===

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    final Map<OSMType, LongBitmapDataProvider> bitMapIndex;
    S updateResult = identitySupplier.get();
    //implement Timeoout for updates
    long execStart = System.currentTimeMillis();
    if (this.update != null) {
      bitMapIndex = UpdateDbHelper.getBitMap(
          this.update.getBitArrayDb()
      );
      CellProcessor<S> cellProcessor = Kernels.getOSMContributionCellReducer(
          mapper,
          identitySupplier,
          accumulator
      );
      updateResult = this.getResultFromUpdates(
          execStart,
          cellProcessor,
          identitySupplier,
          combiner,
          bitMapIndex);
    } else {
      bitMapIndex = null;
    }

    // get regular result
    S result = this.typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelperMapReduce.mapReduceCellsOSMContribution(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper, identitySupplier, accumulator, combiner,
          bitMapIndex);
    }).reduce(identitySupplier.get(), combiner);

    return combiner.apply(result, updateResult);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    final Map<OSMType, LongBitmapDataProvider> bitMapIndex;
    S updateResult = identitySupplier.get();
    //implement Timeoout for updates
    long execStart = System.currentTimeMillis();

    if (this.update != null) {
      bitMapIndex = UpdateDbHelper.getBitMap(
          this.update.getBitArrayDb()
      );
      CellProcessor<S> cellProcessor = Kernels.getOSMContributionGroupingCellReducer(
          mapper,
          identitySupplier,
          accumulator
      );
      updateResult = this.getResultFromUpdates(
          execStart,
          cellProcessor,
          identitySupplier,
          combiner,
          bitMapIndex);
    } else {
      bitMapIndex = null;
    }

    // get regular result
    S result = this.typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelperMapReduce.flatMapReduceCellsOSMContributionGroupedById(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper, identitySupplier, accumulator, combiner,
          bitMapIndex);
    }).reduce(identitySupplier.get(), combiner);

    return combiner.apply(result, updateResult);
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    final Map<OSMType, LongBitmapDataProvider> bitMapIndex;
    S updateResult = identitySupplier.get();
    if (this.update != null) {
      // implement timeout for updates
      long execStart = System.currentTimeMillis();
      bitMapIndex = UpdateDbHelper.getBitMap(
          this.update.getBitArrayDb()
      );
      CellProcessor<S> cellProcessor = Kernels.getOSMEntitySnapshotCellReducer(
          mapper,
          identitySupplier,
          accumulator
      );
      updateResult = this.getResultFromUpdates(
          execStart,
          cellProcessor,
          identitySupplier,
          combiner,
          bitMapIndex);
    } else {
      bitMapIndex = null;
    }

    // get regular result
    S result = this.typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelperMapReduce.mapReduceCellsOSMEntitySnapshot(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper, identitySupplier, accumulator, combiner,
          bitMapIndex);
    }).reduce(identitySupplier.get(), combiner);

    return combiner.apply(result, updateResult);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    final Map<OSMType, LongBitmapDataProvider> bitMapIndex;
    S updateResult = identitySupplier.get();
    if (this.update != null) {
      // implement timeout for updates
      long execStart = System.currentTimeMillis();
      bitMapIndex = UpdateDbHelper.getBitMap(
          this.update.getBitArrayDb()
      );
      CellProcessor<S> cellProcessor = Kernels.getOSMEntitySnapshotGroupingCellReducer(
          mapper,
          identitySupplier,
          accumulator
      );
      updateResult = this.getResultFromUpdates(
          execStart,
          cellProcessor,
          identitySupplier,
          combiner,
          bitMapIndex);
    } else {
      bitMapIndex = null;
    }

    // get regular result
    S result = this.typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelperMapReduce.flatMapReduceCellsOSMEntitySnapshotGroupedById(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper, identitySupplier, accumulator, combiner,
          bitMapIndex);
    }).reduce(identitySupplier.get(), combiner);

    return combiner.apply(result, updateResult);
  }

  // === stream operations ===

  @Override
  protected Stream<X> mapStreamCellsOSMContribution(
      SerializableFunction<OSMContribution, X> mapper) throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    return this.typeFilter.stream().map((Function<OSMType, Stream<X>> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelperMapStream.mapStreamCellsOSMContributionOnIgniteCache(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper);
    }).flatMap(x -> x);
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper) throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    return this.typeFilter.stream().map((Function<OSMType, Stream<X>> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelperMapStream.flatMapStreamCellsOSMContributionGroupedById(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper);
    }).flatMap(x -> x);
  }

  @Override
  protected Stream<X> mapStreamCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, X> mapper) throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    return this.typeFilter.stream().map((Function<OSMType, Stream<X>> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelperMapStream.mapStreamCellsOSMEntitySnapshot(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper);
    }).flatMap(x -> x);
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper) throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    return this.typeFilter.stream().map((Function<OSMType, Stream<X>> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelperMapStream.flatMapStreamCellsOSMEntitySnapshotGroupedById(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper);
    }).flatMap(x -> x);
  }

  // === helper functions ===

  private Map<Integer, TreeMap<Long, CellIdRange>> getCellIdRangesByLevel() {
    Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel = new HashMap<>();
    for (CellIdRange cellIdRange : this.getCellIdRanges()) {
      int level = cellIdRange.getStart().getZoomLevel();
      if (!cellIdRangesByLevel.containsKey(level)) {
        cellIdRangesByLevel.put(level, new TreeMap<>());
      }
      cellIdRangesByLevel.get(level).put(cellIdRange.getStart().getId(), cellIdRange);
    }
    return cellIdRangesByLevel;
  }

  private <S> S getResultFromUpdates(
      long execStart,
      CellProcessor<S> cellProcessor,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner,
      Map<OSMType, LongBitmapDataProvider> bitMapIndex)
      throws ClassNotFoundException, ParseException, SQLException, IOException {

    S updateResult = identitySupplier.get();
    if (this.update != null) {
      CellIterator updateIterator = new CellIterator(
          this.tstamps.get(),
          this.bboxFilter, this.getPolyFilter(),
          this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
      );
      updateIterator.includeIDsOnly(bitMapIndex);
      updateResult = Streams.stream(this.getUpdates())
          .parallel()
          .filter(ignored -> {
            if (timeout != null && System.currentTimeMillis() - execStart > timeout) {
              throw new OSHDBTimeoutException();
            }
            return true;
          })
          .map(oshCell -> cellProcessor.apply(oshCell, updateIterator))
          .reduce(identitySupplier.get(), combiner);
    }
    return updateResult;
  }

}

class IgniteScanQueryHelperMapReduce {
  /**
   * Compute closure that iterates over every partition owned by a node located in a partition.
   */
  private abstract static class MapReduceCellsOnIgniteCacheComputeJob
      <V, R, M, S, P extends Geometry & Polygonal>
      implements CancelableIgniteMapReduceJob<S> {
    private static final Logger LOG =
        LoggerFactory.getLogger(MapReduceCellsOnIgniteCacheComputeJob.class);

    private boolean notCanceled = true;

    @Override
    public void cancel() {
      LOG.info("compute job canceled");
      this.notCanceled = false;
    }

    @Override
    public boolean isActive() {
      return this.notCanceled;
    }

    Map<UUID, List<Integer>> nodesToPart;

    /* computation settings */
    final String cacheName;
    final Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel;
    final CellIterator cellIterator;
    final SerializableFunction<V, M> mapper;
    final SerializableSupplier<S> identitySupplier;
    final SerializableBiFunction<S, R, S> accumulator;
    final SerializableBinaryOperator<S> combiner;

    MapReduceCellsOnIgniteCacheComputeJob(TagInterpreter tagInterpreter, String cacheName,
        Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<V, M> mapper, SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
      this.cacheName = cacheName;
      this.cellIdRangesByLevel = cellIdRangesByLevel;
      this.cellIterator = new CellIterator(
          tstamps, bbox, poly, tagInterpreter, preFilter, filter, false
      );
      if (bitMapIndex != null) {
        this.cellIterator.excludeIDs(bitMapIndex);
      }
      this.mapper = mapper;
      this.identitySupplier = identitySupplier;
      this.accumulator = accumulator;
      this.combiner = combiner;
    }

    void setNodesToPart(Map<UUID, List<Integer>> nodesToPart) {
      this.nodesToPart = nodesToPart;
    }

    boolean cellKeyInRange(Long cellKey) {
      return MapReducerIgniteScanQuery.cellKeyInRange(cellKey, cellIdRangesByLevel);
    }

    S execute(Ignite node, CellProcessor<S> cellProcessor) {
      IgniteCache<Long, BinaryObject> cache = node.cache(cacheName).withKeepBinary();
      // Getting a list of the partitions owned by this node.
      List<Integer> myPartitions = nodesToPart.get(node.cluster().localNode().id());
      Collections.shuffle(myPartitions);
      // run processing in parallel
      return myPartitions.parallelStream()
          .filter(ignored -> this.isActive())
          .map(part -> {
            try (
                QueryCursor<S> cursor = cache.query(
                    new ScanQuery<Long, Object>((key, cell) ->
                        this.isActive() && this.cellKeyInRange(key)
                    ).setPartition(part), cacheEntry -> {
                      if (!this.isActive()) {
                        return identitySupplier.get();
                      }
                      // iterate over the history of all OSM objects in the current cell
                      Object data = cacheEntry.getValue();
                      GridOSHEntity oshEntityCell;
                      if (data instanceof BinaryObject) {
                        oshEntityCell = ((BinaryObject) data).deserialize();
                      } else {
                        oshEntityCell = (GridOSHEntity) data;
                      }
                      return cellProcessor.apply(oshEntityCell, this.cellIterator);
                    }
                )
            ) {
              S accExternal = identitySupplier.get();
              // reduce the results
              for (S entry : cursor) {
                accExternal = combiner.apply(accExternal, entry);
              }
              return accExternal;
            }
          })
          .reduce(identitySupplier.get(), combiner);
    }
  }

  private static class MapReduceCellsOSMContributionOnIgniteCacheComputeJob
      <R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<OSMContribution, R, R, S, P> {
    MapReduceCellsOSMContributionOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        String cacheName, Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<OSMContribution, R> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
      super(tagInterpreter, cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter,
          mapper, identitySupplier, accumulator, combiner, bitMapIndex);
    }

    @Override
    public S execute(Ignite node) {
      return super.execute(node, Kernels.getOSMContributionCellReducer(
          this.mapper,
          this.identitySupplier,
          this.accumulator,
          this
      ));
    }
  }

  private static class FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob
      <R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<List<OSMContribution>, R, Iterable<R>, S, P> {
    FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        String cacheName, Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
      super(tagInterpreter, cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter,
          mapper, identitySupplier, accumulator, combiner, bitMapIndex);
    }

    @Override
    public S execute(Ignite node) {
      return super.execute(node, Kernels.getOSMContributionGroupingCellReducer(
          this.mapper,
          this.identitySupplier,
          this.accumulator,
          this
      ));
    }
  }

  private static class MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob
      <R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<OSMEntitySnapshot, R, R, S, P> {
    MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        String cacheName, Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<OSMEntitySnapshot, R> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
      super(tagInterpreter, cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter,
          mapper, identitySupplier, accumulator, combiner, bitMapIndex);
    }

    @Override
    public S execute(Ignite node) {
      return super.execute(node, Kernels.getOSMEntitySnapshotCellReducer(
          this.mapper,
          this.identitySupplier,
          this.accumulator,
          this
      ));
    }
  }

  private static class FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob
      <R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<List<OSMEntitySnapshot>, R, Iterable<R>, S, P> {
    FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        String cacheName, Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
      super(tagInterpreter, cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter,
          mapper, identitySupplier, accumulator, combiner, bitMapIndex);
    }

    @Override
    public S execute(Ignite node) {
      return super.execute(node, Kernels.getOSMEntitySnapshotGroupingCellReducer(
          this.mapper,
          this.identitySupplier,
          this.accumulator,
          this
      ));
    }
  }

  /**
   * Executes a compute job on all ignite nodes and further reduces and returns result(s).
   *
   * @throws OSHDBTimeoutException if a timeout was set and the computations took too long.
   */
  private static <V, R, M, S, P extends Geometry & Polygonal> S mapReduceOnIgniteCache(
      OSHDBIgnite oshdb, String cacheName, SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner,
      MapReduceCellsOnIgniteCacheComputeJob<V, R, M, S, P> computeJob) {
    Ignite ignite = oshdb.getIgnite();

    // build mapping from ignite compute nodes to cache partitions
    Affinity affinity = ignite.affinity(cacheName);
    List<Integer> allPartitions = new ArrayList<>(affinity.partitions());
    for (int i = 0; i < affinity.partitions(); i++) {
      allPartitions.add(i);
    }
    Map<Integer, ClusterNode> partPerNodes = affinity.mapPartitionsToNodes(allPartitions);
    Map<UUID, List<Integer>> nodesToPart = new HashMap<>();
    for (Map.Entry<Integer, ClusterNode> entry : partPerNodes.entrySet()) {
      List<Integer> nodeParts =
          nodesToPart.computeIfAbsent(entry.getValue().id(), k -> new ArrayList<>());
      nodeParts.add(entry.getKey());
    }

    // async execute compute job on all ignite nodes and further reduce+return result(s)
    IgniteCompute compute = ignite.compute(ignite.cluster().forNodeIds(nodesToPart.keySet()));
    computeJob.setNodesToPart(nodesToPart);
    IgniteRunnable onClose = oshdb.onClose().orElse(() -> { });
    ComputeTaskFuture<S> result = compute.executeAsync(
        new OSHDBIgniteMapReduceComputeTask<Object, S>(
            computeJob,
            identitySupplier,
            combiner,
            onClose
        ),
        null
    );
    S ret;
    if (!oshdb.timeoutInMilliseconds().isPresent()) {
      ret = result.get();
    } else {
      try {
        ret = result.get(oshdb.timeoutInMilliseconds().getAsLong());
      } catch (IgniteFutureTimeoutException e) {
        result.cancel();
        throw new OSHDBTimeoutException();
      }
    }
    return ret;
  }

  static <R, S, P extends Geometry & Polygonal> S mapReduceCellsOSMContribution(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
    return mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new MapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter, cacheName,
            cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter, mapper, identitySupplier,
            accumulator, combiner, bitMapIndex));
  }

  static <R, S, P extends Geometry & Polygonal>
      S flatMapReduceCellsOSMContributionGroupedById(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
    return mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner, bitMapIndex));
  }

  static <R, S, P extends Geometry & Polygonal> S mapReduceCellsOSMEntitySnapshot(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
    return mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner, bitMapIndex));
  }

  static <R, S, P extends Geometry & Polygonal>
      S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
    return mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner, bitMapIndex));
  }
}

class IgniteScanQueryHelperMapStream {
  private static final int SCAN_QUERY_PAGE_SIZE = 16;

  /**
   * Executes a scanquery resulting in the requested data.
   *
   * @throws OSHDBTimeoutException if a timeout was set and the computations took too long.
   */
  private static <X> Stream<X> mapStreamOnIgniteCache(
      OSHDBIgnite oshdb,
      String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      CellIterator cellIterator,
      CellProcessor<Stream<X>> cellProcessor
  ) {
    QueryCursor<List<X>> cursor = oshdb.getIgnite().cache(cacheName).withKeepBinary().query(
        new ScanQuery<Long, Object>((key, cell) ->
            /*isActive() &&*/ MapReducerIgniteScanQuery.cellKeyInRange(key, cellIdRangesByLevel)
        ).setPageSize(SCAN_QUERY_PAGE_SIZE), cacheEntry -> {
          // iterate over the history of all OSM objects in the current cell
          Object data = cacheEntry.getValue();
          GridOSHEntity oshEntityCell;
          if (data instanceof BinaryObject) {
            oshEntityCell = ((BinaryObject) data).deserialize();
          } else {
            oshEntityCell = (GridOSHEntity) data;
          }
          return cellProcessor.apply(oshEntityCell, cellIterator).collect(Collectors.toList());
        }
    );
    // todo: ignite scan query doesn't support timeouts -> implement ourself?
    return Streams.stream(cursor)
        .onClose(cursor::close)
        .flatMap(Collection::stream);
  }

  static <X, P extends Geometry & Polygonal> Stream<X> mapStreamCellsOSMContributionOnIgniteCache(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMContribution, X> mapper) {
    return mapStreamOnIgniteCache(
        oshdb,
        cacheName,
        cellIdRangesByLevel,
        new CellIterator(tstamps, bbox, poly, tagInterpreter, preFilter, filter, false),
        Kernels.getOSMContributionCellStreamer(mapper)
    );
  }

  static <X, P extends Geometry & Polygonal>
  Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      OSHEntityFilter preFilter, OSMEntityFilter filter,
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper) {
    return mapStreamOnIgniteCache(
        oshdb,
        cacheName,
        cellIdRangesByLevel,
        new CellIterator(tstamps, bbox, poly, tagInterpreter, preFilter, filter, false),
        Kernels.getOSMContributionGroupingCellStreamer(mapper)
    );
  }

  public static <X, P extends Geometry & Polygonal>
  Stream<X> mapStreamCellsOSMEntitySnapshot(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      OSHEntityFilter preFilter, OSMEntityFilter filter,
      SerializableFunction<OSMEntitySnapshot, X> mapper) {
    return mapStreamOnIgniteCache(
        oshdb,
        cacheName,
        cellIdRangesByLevel,
        new CellIterator(tstamps, bbox, poly, tagInterpreter, preFilter, filter, false),
        Kernels.getOSMEntitySnapshotCellStreamer(mapper)
    );
  }

  static <X, P extends Geometry & Polygonal>
  Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      OSHEntityFilter preFilter, OSMEntityFilter filter,
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper) {
    return mapStreamOnIgniteCache(
        oshdb,
        cacheName,
        cellIdRangesByLevel,
        new CellIterator(tstamps, bbox, poly, tagInterpreter, preFilter, filter, false),
        Kernels.getOSMEntitySnapshotGroupingCellStreamer(mapper)
    );
  }
}
