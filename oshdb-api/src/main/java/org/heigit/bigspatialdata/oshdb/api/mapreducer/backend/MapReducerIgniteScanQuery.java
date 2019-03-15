package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import javax.cache.Cache;
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
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@inheritDoc}
 *
 * <p>
 * The "ScanQuery" implementation is the an implementation of the oshdb mapreducer on Ignite, which
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

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducerIgniteScanQuery<X>(this);
  }

  @Override
  public boolean isCancelable() {
    return true;
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    return this.typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelper.mapReduceCellsOSMContributionOnIgniteCache(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper, identitySupplier, accumulator, combiner);
    }).reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    return this.typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelper.flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper, identitySupplier, accumulator, combiner);
    }).reduce(identitySupplier.get(), combiner);
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    return this.typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelper.mapReduceCellsOSMEntitySnapshotOnIgniteCache(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper, identitySupplier, accumulator, combiner);
    }).reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    // load tag interpreter helper which is later used for geometry building
    TagInterpreter tagInterpreter = this.getTagInterpreter();

    return this.typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      return IgniteScanQueryHelper.flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
          (OSHDBIgnite) this.oshdb, tagInterpreter, cacheName, this.getCellIdRangesByLevel(),
          this.tstamps.get(), this.bboxFilter, this.getPolyFilter(),
          this.getPreFilter(), this.getFilter(), mapper, identitySupplier, accumulator, combiner);
    }).reduce(identitySupplier.get(), combiner);
  }

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
}



class IgniteScanQueryHelper {
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
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      this.cacheName = cacheName;
      this.cellIdRangesByLevel = cellIdRangesByLevel;
      this.cellIterator = new CellIterator(
          tstamps, bbox, poly, tagInterpreter, preFilter, filter, false
      );
      this.mapper = mapper;
      this.identitySupplier = identitySupplier;
      this.accumulator = accumulator;
      this.combiner = combiner;
    }

    void setNodesToPart(Map<UUID, List<Integer>> nodesToPart) {
      this.nodesToPart = nodesToPart;
    }

    boolean cellKeyInRange(Long cellKey) {
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

    S execute(Ignite node, CellProcessor<S> cellProcessor) {
      IgniteCache<Long, BinaryObject> cache = node.cache(cacheName).withKeepBinary();
      // Getting a list of the partitions owned by this node.
      List<Integer> myPartitions = nodesToPart.get(node.cluster().localNode().id());
      Collections.shuffle(myPartitions);
      // run processing in parallel
      return myPartitions.parallelStream()
          .filter(ignored -> this.isActive())
          .map(part -> {
            // noinspection unchecked
            try (
                QueryCursor<S> cursor = cache.query(
                    new ScanQuery((key, cell) ->
                        this.isActive() && this.cellKeyInRange((Long)key)
                    ).setPartition(part), cacheEntry -> {
                      if (!this.isActive()) {
                        return identitySupplier.get();
                      }
                      // iterate over the history of all OSM objects in the current cell
                      Object data = ((Cache.Entry<Long, Object>) cacheEntry).getValue();
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
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter,
          mapper, identitySupplier, accumulator, combiner);
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
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter,
          mapper, identitySupplier, accumulator, combiner);
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
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter,
          mapper, identitySupplier, accumulator, combiner);
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
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter,
          mapper, identitySupplier, accumulator, combiner);
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

  static <R, S, P extends Geometry & Polygonal> S mapReduceCellsOSMContributionOnIgniteCache(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new MapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter, cacheName,
            cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter, mapper, identitySupplier,
            accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal>
      S flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal> S mapReduceCellsOSMEntitySnapshotOnIgniteCache(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal>
      S flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName,
      Map<Integer, TreeMap<Long, CellIdRange>> cellIdRangesByLevel,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheName, cellIdRangesByLevel, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }
}
