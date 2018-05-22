package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.api.generic.function.*;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@inheritDoc}
 *
 *
 * The "LocalPeek" implementation is the a very versatile implementation of the oshdb mapreducer on
 * Ignite: It offers high performance, scalability and cancelable queries. It should be used in most
 * situations when running oshdb- analyses on ignite.
 */
public class MapReducerIgniteLocalPeek<X> extends MapReducer<X> {
  private static final Logger LOG = LoggerFactory.getLogger(MapReducerIgniteLocalPeek.class);

  public MapReducerIgniteLocalPeek(OSHDBDatabase oshdb,
      Class<? extends OSHDBMapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducerIgniteLocalPeek(MapReducerIgniteLocalPeek obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducerIgniteLocalPeek<X>(this);
  }

  private List<String> cacheNames(String prefix) {
    return this._typeFilter.stream().map(TableNames::forOSMType).filter(Optional::isPresent)
        .map(Optional::get).map(tn -> tn.toString(prefix)).collect(Collectors.toList());
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return IgniteLocalPeekHelper._mapReduceCellsOSMContributionOnIgniteCache(
        (OSHDBIgnite) this._oshdb, this.cacheNames(this._oshdb.prefix()), this._getCellIdRanges(),
        this._getTagInterpreter(), this._tstamps.get(), this._bboxFilter,
        this._getPolyFilter(), this._getPreFilter(), this._getFilter(), mapper, identitySupplier,
        accumulator, combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return IgniteLocalPeekHelper._flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
        (OSHDBIgnite) this._oshdb, this.cacheNames(this._oshdb.prefix()), this._getCellIdRanges(),
        this._getTagInterpreter(), this._tstamps.get(), this._bboxFilter,
        this._getPolyFilter(), this._getPreFilter(), this._getFilter(), mapper, identitySupplier,
        accumulator, combiner);
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    return IgniteLocalPeekHelper._mapReduceCellsOSMEntitySnapshotOnIgniteCache(
        (OSHDBIgnite) this._oshdb, this.cacheNames(this._oshdb.prefix()), this._getCellIdRanges(),
        this._getTagInterpreter(), this._tstamps.get(), this._bboxFilter,
        this._getPolyFilter(), this._getPreFilter(), this._getFilter(), mapper, identitySupplier,
        accumulator, combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return IgniteLocalPeekHelper._flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
        (OSHDBIgnite) this._oshdb, this.cacheNames(this._oshdb.prefix()), this._getCellIdRanges(),
        this._getTagInterpreter(), this._tstamps.get(), this._bboxFilter,
        this._getPolyFilter(), this._getPreFilter(), this._getFilter(), mapper, identitySupplier,
        accumulator, combiner);
  }
}


class IgniteLocalPeekHelper {
  /**
   * @param <T> Type of the task argument.
   * @param <R> Type of the task result returning from {@link ComputeTask#reduce(List)} method.
   */
  @org.apache.ignite.compute.ComputeTaskNoResultCache
  static class CancelableBroadcastTask<T, R> extends ComputeTaskAdapter<T, R>
      implements Serializable {
    private final MapReduceCellsOnIgniteCacheComputeJob job;
    private final SerializableBinaryOperator<R> combiner;

    private R resultAccumulator;

    public CancelableBroadcastTask(MapReduceCellsOnIgniteCacheComputeJob job,
        SerializableSupplier<R> identitySupplier, SerializableBinaryOperator<R> combiner) {
      this.job = job;
      this.combiner = combiner;
      this.resultAccumulator = identitySupplier.get();
    }

    @Override
    public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, T arg)
        throws IgniteException {
      Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());
      subgrid.forEach(node -> map.put(new ComputeJob() {
        @IgniteInstanceResource
        private Ignite ignite;

        @Override
        public void cancel() {
          job.cancel();
        }

        @Override
        public Object execute() throws IgniteException {
          return job.execute(ignite);
        }
      }, node));
      return map;
    }

    @Override
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd)
        throws IgniteException {
      R data = res.getData();
      resultAccumulator = combiner.apply(resultAccumulator, data);
      return ComputeJobResultPolicy.WAIT;
    }

    @Override
    public R reduce(List<ComputeJobResult> results) throws IgniteException {
      return resultAccumulator;
    }
  }

  /**
   * Compute closure that iterates over every partition owned by a node located in a partition.
   */
  private static abstract class MapReduceCellsOnIgniteCacheComputeJob<V, R, MR, S, P extends Geometry & Polygonal>
      implements Serializable {
    private static final Logger LOG =
        LoggerFactory.getLogger(MapReduceCellsOnIgniteCacheComputeJob.class);
    boolean canceled = false;

    /* computation settings */
    final List<String> cacheNames;
    final Iterable<Pair<CellId, CellId>> cellIdRanges;
    final OSHDBBoundingBox bbox;
    final CellIterator cellIterator;
    final SerializableFunction<V, MR> mapper;
    final SerializableSupplier<S> identitySupplier;
    final SerializableBiFunction<S, R, S> accumulator;
    final SerializableBinaryOperator<S> combiner;

    MapReduceCellsOnIgniteCacheComputeJob(TagInterpreter tagInterpreter, List<String> cacheNames,
        Iterable<Pair<CellId, CellId>> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<V, MR> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      this.cacheNames = cacheNames;
      this.cellIdRanges = cellIdRanges;
      this.bbox = bbox;
      this.cellIterator = new CellIterator(
          tstamps, bbox, poly, tagInterpreter, preFilter, filter, false
      );
      this.mapper = mapper;
      this.identitySupplier = identitySupplier;
      this.accumulator = accumulator;
      this.combiner = combiner;
    }

    void cancel() {
      LOG.info("compute job canceled");
      this.canceled = true;
    }

    public abstract S execute(Ignite node);

    List<Pair<IgniteCache<Long, GridOSHEntity>, Long>> localKeys(Ignite node) {
      // calculate all cache keys we have to investigate
      List<Pair<IgniteCache<Long, GridOSHEntity>, Long>> localKeys = new ArrayList<>();
      this.cacheNames.forEach(cacheName -> {
        IgniteCache<Long, GridOSHEntity> cache = node.cache(cacheName);
        List<Long> cellIdRangeIds = new ArrayList<>();
        this.cellIdRanges.forEach(cellIdRange -> {
          cellIdRangeIds.clear();
          int level = cellIdRange.getLeft().getZoomLevel();
          long from = CellId.getLevelId(level, cellIdRange.getLeft().getId());
          long to = CellId.getLevelId(level, cellIdRange.getRight().getId());
          for (long key = from; key <= to; key++) {
            cellIdRangeIds.add(key);
          }
          // Map keys to ignite nodes and remember the local ones
          node.<Long>affinity(cache.getName())
              .mapKeysToNodes(cellIdRangeIds)
              .getOrDefault(node.cluster().localNode(), Collections.emptyList())
              .forEach(key -> localKeys.add(new ImmutablePair<>(cache, key)));
        });
      });
      return localKeys;
    }

    interface Callback<S> extends Serializable {
      S apply (GridOSHEntity oshEntityCell);
    }

    S execute(Ignite node, Callback<S> callback) {
      return this.localKeys(node).parallelStream()
          .map(cacheKey -> cacheKey.getLeft().localPeek(cacheKey.getRight()))
          .filter(Objects::nonNull) // filter out cache misses === empty oshdb cells
          .filter(ignored -> !this.canceled)
          .map(callback::apply)
          .reduce(identitySupplier.get(), combiner);
    }
  }

  private static class MapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<OSMContribution, R, R, S, P> {
    MapReduceCellsOSMContributionOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, cellIdRanges,tstamps, bbox, poly, preFilter, filter, mapper,
          identitySupplier, accumulator, combiner);
    }

    @Override
    public S execute(Ignite node) {
      return super.execute(node, oshEntityCell -> {
        // iterate over the history of all OSM objects in the current cell
        AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
        cellIterator.iterateByContribution(oshEntityCell)
            .forEach(contribution -> {
              if (this.canceled)
                return;
              OSMContribution osmContribution =
                  new OSMContribution(contribution);
              accInternal
                  .set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
            });
        return accInternal.get();
      });
    }
  }

  private static class FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<List<OSMContribution>, R, Iterable<R>, S, P> {
    FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, cellIdRanges,tstamps, bbox, poly, preFilter, filter, mapper,
          identitySupplier, accumulator, combiner);
    }

    @Override
    public S execute(Ignite node) {
      return super.execute(node, oshEntityCell -> {
        // iterate over the history of all OSM objects in the current cell
        AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
        List<OSMContribution> contributions = new ArrayList<>();
        cellIterator.iterateByContribution(oshEntityCell)
            .forEach(contribution -> {
              if (this.canceled)
                return;
              OSMContribution thisContribution = new OSMContribution(contribution);
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
      });
    }
  }

  private static class MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<OSMEntitySnapshot, R, R, S, P> {
    MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
          identitySupplier, accumulator, combiner);
    }

    @Override
    public S execute(Ignite node) {
      return super.execute(node, oshEntityCell -> {
        // iterate over the history of all OSM objects in the current cell
        AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
        cellIterator.iterateByTimestamps(oshEntityCell).forEach(data -> {
          if (this.canceled) return;
          OSMEntitySnapshot snapshot = new OSMEntitySnapshot(data);
          // immediately fold the result
          accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
        });
        return accInternal.get();
      });
    }
  }

  private static class FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal>
      extends MapReduceCellsOnIgniteCacheComputeJob<List<OSMEntitySnapshot>, R, Iterable<R>, S, P> {
    FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob(TagInterpreter tagInterpreter,
        List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
          identitySupplier, accumulator, combiner);
    }

    @Override
    public S execute(Ignite node) {
      return super.execute(node, oshEntityCell -> {
        // iterate over the history of all OSM objects in the current cell
        AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
        List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>();
        cellIterator.iterateByTimestamps(oshEntityCell).forEach(data -> {
          if (this.canceled)
            return;
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
      });
    }
  }

  private static <V, R, MR, S, P extends Geometry & Polygonal> S _mapReduceOnIgniteCache(
      OSHDBIgnite oshdb, SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner,
      MapReduceCellsOnIgniteCacheComputeJob<V, R, MR, S, P> computeJob) {
    // execute compute job on all ignite nodes and further reduce+return result(s)
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();

    ComputeTaskFuture<S> result = compute.executeAsync(
        new CancelableBroadcastTask<Object, S>(computeJob, identitySupplier, combiner), null);

    if (!oshdb.timeoutInMilliseconds().isPresent()) {
      return result.get();
    } else {
      try {
        return result.get(oshdb.timeoutInMilliseconds().getAsLong());
      } catch (IgniteFutureTimeoutException e) {
        result.cancel();
        throw new OSHDBTimeoutException();
      }
    }
  }

  static <R, S, P extends Geometry & Polygonal> S _mapReduceCellsOSMContributionOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return _mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new MapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal> S _flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return _mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal> S _mapReduceCellsOSMEntitySnapshotOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return _mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal> S _flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return _mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }
}
