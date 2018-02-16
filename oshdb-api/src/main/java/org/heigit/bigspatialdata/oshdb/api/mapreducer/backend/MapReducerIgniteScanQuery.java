package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.api.generic.function.*;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampInterval;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.jetbrains.annotations.NotNull;

/**
 * {@inheritDoc}
 *
 *
 * The "ScanQuery" implementation is the an implementation of the oshdb mapreducer on Ignite, which
 * always scans over the whole data set when running queries. It might offer better performance for
 * global (or almost global) queries. In other situations it should not be used.
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
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    TagInterpreter tagInterpreter = this._getTagInterpreter(); // load tag interpreter helper which
                                                               // is later used for geometry
                                                               // building

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this._oshdb.prefix());
      return IgniteScanQueryHelper._mapReduceCellsOSMContributionOnIgniteCache(
          (OSHDBIgnite) this._oshdb, tagInterpreter, cacheName, cellIdsList,
          this._tstamps.get(), this._bboxFilter, this._getPolyFilter(),
          this._getPreFilter(), this._getFilter(), mapper, identitySupplier, accumulator, combiner);
    }).reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    TagInterpreter tagInterpreter = this._getTagInterpreter(); // load tag interpreter helper which
                                                               // is later used for geometry
                                                               // building

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this._oshdb.prefix());
      return IgniteScanQueryHelper._flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
          (OSHDBIgnite) this._oshdb, tagInterpreter, cacheName, cellIdsList,
          this._tstamps.get(), this._bboxFilter, this._getPolyFilter(),
          this._getPreFilter(), this._getFilter(), mapper, identitySupplier, accumulator, combiner);
    }).reduce(identitySupplier.get(), combiner);
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    TagInterpreter tagInterpreter = this._getTagInterpreter(); // load tag interpreter helper which
                                                               // is later used for geometry
                                                               // building

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this._oshdb.prefix());
      return IgniteScanQueryHelper._mapReduceCellsOSMEntitySnapshotOnIgniteCache(
          (OSHDBIgnite) this._oshdb, tagInterpreter, cacheName, cellIdsList,
          this._tstamps.get(), this._bboxFilter, this._getPolyFilter(),
          this._getPreFilter(), this._getFilter(), mapper, identitySupplier, accumulator, combiner);
    }).reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    TagInterpreter tagInterpreter = this._getTagInterpreter(); // load tag interpreter helper which
                                                               // is later used for geometry
                                                               // building

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this._oshdb.prefix());
      return IgniteScanQueryHelper._flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
          (OSHDBIgnite) this._oshdb, tagInterpreter, cacheName, cellIdsList,
          this._tstamps.get(), this._bboxFilter, this._getPolyFilter(),
          this._getPreFilter(), this._getFilter(), mapper, identitySupplier, accumulator, combiner);
    }).reduce(identitySupplier.get(), combiner);
  }
}



class IgniteScanQueryHelper {
  /**
   * Compute closure that iterates over every partition owned by a node located in a partition.
   */
  private static abstract class MapReduceCellsOnIgniteCacheComputeJob<V, R, MR, S, P extends Geometry & Polygonal>
      implements IgniteCallable<S> {
    /** */
    Map<UUID, List<Integer>> nodesToPart;

    /** */
    @IgniteInstanceResource
    Ignite node;

    /** */
    IgniteCache<Long, GridOSHEntity> cache;

    /* computation settings */
    final String cacheName;
    final Set<CellId> cellIdsList;
    final CellIterator cellIterator;
    final SortedSet<OSHDBTimestamp> tstamps;
    final SerializableFunction<V, MR> mapper;
    final SerializableSupplier<S> identitySupplier;
    final SerializableBiFunction<S, R, S> accumulator;
    final SerializableBinaryOperator<S> combiner;

    MapReduceCellsOnIgniteCacheComputeJob(TagInterpreter tagInterpreter, String cacheName,
        Set<CellId> cellIdsList, SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter,
        SerializableFunction<V, MR> mapper, SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      this.cacheName = cacheName;
      this.cellIdsList = cellIdsList;
      this.cellIterator = new CellIterator(bbox, poly, tagInterpreter, preFilter, filter, false);
      this.tstamps = tstamps;
      this.mapper = mapper;
      this.identitySupplier = identitySupplier;
      this.accumulator = accumulator;
      this.combiner = combiner;
    }

    void setNodesToPart(Map<UUID, List<Integer>> nodesToPart) {
      this.nodesToPart = nodesToPart;
    }
  }

  private static class MapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<OSMContribution, R, R, S, P> {
    MapReduceCellsOSMContributionOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        String cacheName, Set<CellId> cellIdsList, SortedSet<OSHDBTimestamp> tstamps,
        OSHDBBoundingBox bbox, P poly,
        SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter,
        SerializableFunction<OSMContribution, R> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheName, cellIdsList, tstamps, bbox, poly, preFilter, filter, mapper,
          identitySupplier, accumulator, combiner);
    }

    @Override
    public S call() throws Exception {
      cache = node.cache(cacheName);
      // Getting a list of the partitions owned by this node.
      List<Integer> myPartitions = nodesToPart.get(node.cluster().localNode().id());
      Collections.shuffle(myPartitions); // todo: check why this gives 2x speedup (regarding
                                         // "uptime") on cluster!!??
      // run processing in parallel
      return myPartitions.parallelStream().map(part -> {
        // noinspection unchecked
        try (QueryCursor<S> cursor = cache.query((new ScanQuery((key, cell) ->
            cellIdsList.contains(new CellId(
                ((GridOSHEntity) cell).getLevel(),
                ((GridOSHEntity) cell).getId()
            ))
        )).setPartition(part), cacheEntry -> {
          // iterate over the history of all OSM objects in the current cell
          GridOSHEntity oshEntityCell = ((Cache.Entry<Long, GridOSHEntity>) cacheEntry).getValue();
          AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
          cellIterator.iterateByContribution(oshEntityCell, new OSHDBTimestampInterval(tstamps))
              .forEach(contribution -> {
                OSMContribution osmContribution =
                    new OSMContribution(contribution);
                accInternal
                    .set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
              });
          return accInternal.get();
        })) {
          S accExternal = identitySupplier.get();
          // reduce the results
          for (S entry : cursor) {
            accExternal = combiner.apply(accExternal, entry);
          }
          return accExternal;
        }
      }).reduce(identitySupplier.get(), combiner);
    }
  }

  private static class FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<List<OSMContribution>, R, List<R>, S, P> {
    FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        String cacheName, Set<CellId> cellIdsList, SortedSet<OSHDBTimestamp> tstamps,
        OSHDBBoundingBox bbox, P poly,
        SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter,
        SerializableFunction<List<OSMContribution>, List<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheName, cellIdsList, tstamps, bbox, poly, preFilter, filter, mapper,
          identitySupplier, accumulator, combiner);
    }

    @Override
    public S call() throws Exception {
      cache = node.cache(cacheName);
      // Getting a list of the partitions owned by this node.
      List<Integer> myPartitions = nodesToPart.get(node.cluster().localNode().id());
      Collections.shuffle(myPartitions); // todo: check why this gives 2x speedup (regarding
                                         // "uptime") on cluster!!??
      // run processing in parallel
      return myPartitions.parallelStream().map(part -> {
        // noinspection unchecked
        try (QueryCursor<S> cursor = cache.query((new ScanQuery((key, cell) ->
            cellIdsList.contains(new CellId(
                ((GridOSHEntity) cell).getLevel(),
                ((GridOSHEntity) cell).getId()
            ))
        )).setPartition(part), cacheEntry -> {
          // iterate over the history of all OSM objects in the current cell
          GridOSHEntity oshEntityCell = ((Cache.Entry<Long, GridOSHEntity>) cacheEntry).getValue();
          AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
          List<OSMContribution> contributions = new ArrayList<>();
          cellIterator.iterateByContribution(oshEntityCell, new OSHDBTimestampInterval(tstamps))
              .forEach(contribution -> {
                OSMContribution thisContribution =
                    new OSMContribution(contribution);
                if (contributions.size() > 0
                    && thisContribution.getEntityAfter().getId() != contributions
                        .get(contributions.size() - 1).getEntityAfter().getId()) {
                  // immediately fold the results
                  for (R r : mapper.apply(contributions)) {
                    accInternal.set(accumulator.apply(accInternal.get(), r));
                  }
                  contributions.clear();
                }
                contributions.add(thisContribution);
              });
          // apply mapper and fold results one more time for last entity in current cell
          if (contributions.size() > 0) {
            for (R r : mapper.apply(contributions)) {
              accInternal.set(accumulator.apply(accInternal.get(), r));
            }
          }
          return accInternal.get();
        })) {
          S accExternal = identitySupplier.get();
          // reduce the results
          for (S entry : cursor) {
            accExternal = combiner.apply(accExternal, entry);
          }
          return accExternal;
        }
      }).reduce(identitySupplier.get(), combiner);
    }
  }

  private static class MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<OSMEntitySnapshot, R, R, S, P> {
    MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        String cacheName, Set<CellId> cellIdsList, SortedSet<OSHDBTimestamp> tstamps,
        OSHDBBoundingBox bbox, P poly,
        SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter,
        SerializableFunction<OSMEntitySnapshot, R> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheName, cellIdsList, tstamps, bbox, poly, preFilter, filter, mapper,
          identitySupplier, accumulator, combiner);
    }

    @Override
    public S call() throws Exception {
      cache = node.cache(cacheName);
      // Getting a list of the partitions owned by this node.
      List<Integer> myPartitions = nodesToPart.get(node.cluster().localNode().id());
      Collections.shuffle(myPartitions); // todo: check why this gives 2x speedup (regarding
                                         // "uptime") on cluster!!??
      // run processing in parallel
      return myPartitions.parallelStream().map(part -> {
        // noinspection unchecked
        try (QueryCursor<S> cursor = cache.query((new ScanQuery((key, cell) ->
            cellIdsList.contains(new CellId(
                ((GridOSHEntity) cell).getLevel(),
                ((GridOSHEntity) cell).getId()
            ))
        )).setPartition(part), cacheEntry -> {
          GridOSHEntity oshEntityCell = ((Cache.Entry<Long, GridOSHEntity>) cacheEntry).getValue();
          AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
          cellIterator.iterateByTimestamps(oshEntityCell, tstamps).forEach(data -> {
            OSMEntitySnapshot snapshot = new OSMEntitySnapshot(data);
            // immediately fold the result
            accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
          });
          return accInternal.get();
        })) {
          S accExternal = identitySupplier.get();
          // reduce the results
          for (S entry : cursor) {
            accExternal = combiner.apply(accExternal, entry);
          }
          return accExternal;
        }
      }).reduce(identitySupplier.get(), combiner);
    }
  }

  private static class FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<List<OSMEntitySnapshot>, R, List<R>, S, P> {
    FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        String cacheName, Set<CellId> cellIdsList, SortedSet<OSHDBTimestamp> tstamps,
        OSHDBBoundingBox bbox, P poly,
        SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter,
        SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheName, cellIdsList, tstamps, bbox, poly, preFilter, filter, mapper,
          identitySupplier, accumulator, combiner);
    }

    @Override
    public S call() throws Exception {
      cache = node.cache(cacheName);
      // Getting a list of the partitions owned by this node.
      List<Integer> myPartitions = nodesToPart.get(node.cluster().localNode().id());
      Collections.shuffle(myPartitions); // todo: check why this gives 2x speedup (regarding
                                         // "uptime") on cluster!!??
      // run processing in parallel
      return myPartitions.parallelStream().map(part -> {
        // noinspection unchecked
        try (QueryCursor<S> cursor = cache.query((new ScanQuery((key, cell) ->
            cellIdsList.contains(new CellId(
                ((GridOSHEntity) cell).getLevel(),
                ((GridOSHEntity) cell).getId()
            ))
        )).setPartition(part), cacheEntry -> {
          GridOSHEntity oshEntityCell = ((Cache.Entry<Long, GridOSHEntity>) cacheEntry).getValue();
          AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
          List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>();
          cellIterator.iterateByTimestamps(oshEntityCell, tstamps).forEach(data -> {
            OSMEntitySnapshot thisSnapshot = new OSMEntitySnapshot(data);
            if (osmEntitySnapshots.size() > 0
                && thisSnapshot.getEntity().getId() != osmEntitySnapshots
                .get(osmEntitySnapshots.size() - 1).getEntity().getId()) {
              // immediately fold the results
              for (R r : mapper.apply(osmEntitySnapshots)) {
                accInternal.set(accumulator.apply(accInternal.get(), r));
              }
              osmEntitySnapshots.clear();
            }
            osmEntitySnapshots.add(thisSnapshot);
          });
          // apply mapper and fold results one more time for last entity in current cell
          if (osmEntitySnapshots.size() > 0) {
            for (R r : mapper.apply(osmEntitySnapshots)) {
              accInternal.set(accumulator.apply(accInternal.get(), r));
            }
          }
          return accInternal.get();
        })) {
          S accExternal = identitySupplier.get();
          // reduce the results
          for (S entry : cursor) {
            accExternal = combiner.apply(accExternal, entry);
          }
          return accExternal;
        }
      }).reduce(identitySupplier.get(), combiner);
    }
  }

  private static <V, R, MR, S, P extends Geometry & Polygonal> S _mapReduceOnIgniteCache(
      OSHDBIgnite oshdb, String cacheName, SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner,
      MapReduceCellsOnIgniteCacheComputeJob<V, R, MR, S, P> computeJob) {
    Ignite ignite = oshdb.getIgnite();

    // build mapping from ignite compute nodes to cache partitions
    Affinity affinity = ignite.affinity(cacheName);
    List<Integer> allPartitions = new ArrayList<>(affinity.partitions());
    for (int i = 0; i < affinity.partitions(); i++)
      allPartitions.add(i);
    Map<Integer, ClusterNode> partPerNodes = affinity.mapPartitionsToNodes(allPartitions);
    Map<UUID, List<Integer>> nodesToPart = new HashMap<>();
    for (Map.Entry<Integer, ClusterNode> entry : partPerNodes.entrySet()) {
      List<Integer> nodeParts =
          nodesToPart.computeIfAbsent(entry.getValue().id(), k -> new ArrayList<>());
      nodeParts.add(entry.getKey());
    }
    // execute compute job on all ignite nodes and further reduce+return result(s)
    IgniteCompute compute = ignite.compute(ignite.cluster().forNodeIds(nodesToPart.keySet()));
    computeJob.setNodesToPart(nodesToPart);
    Collection<S> nodeResults = compute.broadcast(computeJob);
    return nodeResults.stream().reduce(identitySupplier.get(), combiner);
  }

  static <R, S, P extends Geometry & Polygonal> S _mapReduceCellsOSMContributionOnIgniteCache(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName, Set<CellId> cellIdsList,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter,
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return _mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new MapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter, cacheName,
            cellIdsList, tstamps, bbox, poly, preFilter, filter, mapper, identitySupplier,
            accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal> S _flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName, Set<CellId> cellIdsList,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter,
      SerializableFunction<List<OSMContribution>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return _mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheName, cellIdsList, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal> S _mapReduceCellsOSMEntitySnapshotOnIgniteCache(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName, Set<CellId> cellIdsList,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter,
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return _mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheName, cellIdsList, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal> S _flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
      OSHDBIgnite oshdb, TagInterpreter tagInterpreter, String cacheName, Set<CellId> cellIdsList,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter,
      SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return _mapReduceOnIgniteCache(oshdb, cacheName, identitySupplier, combiner,
        new FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheName, cellIdsList, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }
}
