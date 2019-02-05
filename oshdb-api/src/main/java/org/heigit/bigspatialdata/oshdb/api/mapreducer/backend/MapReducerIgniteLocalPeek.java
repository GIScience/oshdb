package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.Spliterators;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.Kernels.CancelableProcessStatus;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.Kernels.CellProcessor;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@inheritDoc}
 *
 * <p>
 * The "LocalPeek" implementation is the a very versatile implementation of the oshdb mapreducer on
 * Ignite: It offers high performance, scalability and cancelable queries. It should be used in most
 * situations when running oshdb- analyses on ignite.
 * </p>
 */
public class MapReducerIgniteLocalPeek<X> extends MapReducer<X> {
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
    return this.typeFilter.stream().map(TableNames::forOSMType).filter(Optional::isPresent)
        .map(Optional::get).map(tn -> tn.toString(prefix)).collect(Collectors.toList());
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return IgniteLocalPeekHelper.mapReduceCellsOSMContributionOnIgniteCache(
        (OSHDBIgnite) this.oshdb, this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
        this.getTagInterpreter(), this.tstamps.get(), this.bboxFilter,
        this.getPolyFilter(), this.getPreFilter(), this.getFilter(), mapper, identitySupplier,
        accumulator, combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return IgniteLocalPeekHelper.flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
        (OSHDBIgnite) this.oshdb, this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
        this.getTagInterpreter(), this.tstamps.get(), this.bboxFilter,
        this.getPolyFilter(), this.getPreFilter(), this.getFilter(), mapper, identitySupplier,
        accumulator, combiner);
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    return IgniteLocalPeekHelper.mapReduceCellsOSMEntitySnapshotOnIgniteCache(
        (OSHDBIgnite) this.oshdb, this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
        this.getTagInterpreter(), this.tstamps.get(), this.bboxFilter,
        this.getPolyFilter(), this.getPreFilter(), this.getFilter(), mapper, identitySupplier,
        accumulator, combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return IgniteLocalPeekHelper.flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
        (OSHDBIgnite) this.oshdb, this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
        this.getTagInterpreter(), this.tstamps.get(), this.bboxFilter,
        this.getPolyFilter(), this.getPreFilter(), this.getFilter(), mapper, identitySupplier,
        accumulator, combiner);
  }
}


class IgniteLocalPeekHelper {
  /**
   * A cancelable ignite broadcast task.
   *
   * @param <T> Type of the task argument.
   * @param <R> Type of the task result returning from {@link ComputeTask#reduce(List)} method.
   */
  @ComputeTaskNoResultCache
  static class CancelableBroadcastTask<T, R> extends ComputeTaskAdapter<T, R>
      implements Serializable {
    private final MapReduceCellsOnIgniteCacheComputeJob job;
    private final SerializableBinaryOperator<R> combiner;
    private final IgniteRunnable onClose;

    private R resultAccumulator;

    public CancelableBroadcastTask(
        MapReduceCellsOnIgniteCacheComputeJob job,
        SerializableSupplier<R> identitySupplier,
        SerializableBinaryOperator<R> combiner,
        IgniteRunnable onClose
    ) {
      this.job = job;
      this.combiner = combiner;
      this.resultAccumulator = identitySupplier.get();
      this.onClose = onClose;
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
          Object result = job.execute(ignite);
          onClose.run();
          return result;
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
  private abstract static class MapReduceCellsOnIgniteCacheComputeJob
      <V, R, M, S, P extends Geometry & Polygonal>
      implements Serializable, CancelableProcessStatus {
    private static final Logger LOG =
        LoggerFactory.getLogger(MapReduceCellsOnIgniteCacheComputeJob.class);
    private boolean notCanceled = true;

    /* computation settings */
    final List<String> cacheNames;
    final Iterable<Pair<CellId, CellId>> cellIdRanges;
    final OSHDBBoundingBox bbox;
    final CellIterator cellIterator;
    final SerializableFunction<V, M> mapper;
    final SerializableSupplier<S> identitySupplier;
    final SerializableBiFunction<S, R, S> accumulator;
    final SerializableBinaryOperator<S> combiner;

    MapReduceCellsOnIgniteCacheComputeJob(TagInterpreter tagInterpreter, List<String> cacheNames,
        Iterable<Pair<CellId, CellId>> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<V, M> mapper,
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
      this.notCanceled = false;
    }

    @Override
    public boolean isActive() {
      return this.notCanceled;
    }

    private class CellKeysIterator implements Iterator<Long> {
      private final Iterator<Long> cellIds;
      ArrayList<Long> buffer;

      // a buffer of about ~1M interleaved cellIds
      final int bufferSize = 102400 * ForkJoinPool.commonPool().getParallelism();

      CellKeysIterator(Iterable<Pair<CellId, CellId>> cellIdRanges) {
        this.cellIds = StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(cellIdRanges.iterator(), 0),
            true
        )
        .filter(ignored -> isActive())
        .flatMap(cellIdRange -> {
          int level = cellIdRange.getLeft().getZoomLevel();
          long fromId = cellIdRange.getLeft().getId();
          long toId = cellIdRange.getRight().getId();
          return LongStream.rangeClosed(fromId, toId)
              .map(id -> CellId.getLevelId(level, id))
              .boxed();
        }).iterator();
        buffer = new ArrayList<>(bufferSize);
      }

      private void fillBuffer() {
        buffer.clear();
        while (buffer.size() < bufferSize) {
          if (!cellIds.hasNext()) {
            break;
          }
          buffer.add(cellIds.next());
        }
        Collections.shuffle(buffer);
      }

      @Override
      public boolean hasNext() {
        if (buffer.size() > 0) {
          return true;
        }
        if (isActive() && cellIds.hasNext()) {
          fillBuffer();
          return true;
        }
        return false;
      }

      @Override
      public Long next() {
        return buffer.remove(buffer.size() - 1);
      }
    }

    public abstract S execute(Ignite node);

    S execute(Ignite node, CellProcessor<S> cellProcessor) {
      Iterator<Long> cellKeysIterator = new CellKeysIterator(cellIdRanges);

      Set<IgniteCache<Long, GridOSHEntity>> caches = this.cacheNames.stream()
          .map(node::<Long, GridOSHEntity>cache)
          .collect(Collectors.toSet());

      return StreamSupport.stream(
              Spliterators.spliteratorUnknownSize(cellKeysIterator, 0),
              true
          )
          .filter(ignored -> this.isActive())
          .flatMap(cellKey ->
              // get local data from all requested caches
              caches.stream()
                  .filter(ignored -> this.isActive())
                  .map(cache -> cache.localPeek(cellKey))
          )
          // filter out cache misses === empty oshdb cells or not "local" data
          .filter(Objects::nonNull)
          .filter(ignored -> this.isActive())
          .map(cell -> cellProcessor.apply(cell, this.cellIterator))
          .reduce(identitySupplier.get(), combiner);
    }
  }

  private static class MapReduceCellsOSMContributionOnIgniteCacheComputeJob
      <R, S, P extends Geometry & Polygonal>
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
        List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter,
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
        List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter,
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

  private static <V, R, M, S, P extends Geometry & Polygonal> S mapReduceOnIgniteCache(
      OSHDBIgnite oshdb, SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner,
      MapReduceCellsOnIgniteCacheComputeJob<V, R, M, S, P> computeJob) {
    // execute compute job on all ignite nodes and further reduce+return result(s)
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();

    ComputeTaskFuture<S> asyncResult = compute.executeAsync(
        new CancelableBroadcastTask<Object, S>(
            computeJob,
            identitySupplier,
            combiner,
            oshdb.onClose().orElse(() -> { })
        ),
        null
    );

    if (!oshdb.timeoutInMilliseconds().isPresent()) {
      return asyncResult.get();
    } else {
      try {
        return asyncResult.get(oshdb.timeoutInMilliseconds().getAsLong());
      } catch (ComputeTaskTimeoutException | IgniteFutureTimeoutException e) {
        asyncResult.cancel();
        throw new OSHDBTimeoutException();
      }
    }
  }

  static <R, S, P extends Geometry & Polygonal> S mapReduceCellsOSMContributionOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new MapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal>
      S flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) {
    return mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal> S mapReduceCellsOSMEntitySnapshotOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }

  static <R, S, P extends Geometry & Polygonal>
      S flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<Pair<CellId, CellId>> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner));
  }
}
