package org.heigit.ohsome.oshdb.api.mapreducer.backend;

import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.heigit.ohsome.oshdb.OSHDBBoundingBox;
import org.heigit.ohsome.oshdb.OSHDBTimestamp;
import org.heigit.ohsome.oshdb.api.db.OSHDBDatabase;
import org.heigit.ohsome.oshdb.api.db.OSHDBIgnite;
import org.heigit.ohsome.oshdb.api.mapreducer.MapReducerBase;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.Kernels.CellProcessor;
import org.heigit.ohsome.oshdb.api.mapreducer.backend.OSHDBIgniteMapReduceComputeTask.CancelableIgniteMapReduceJob;
import org.heigit.ohsome.oshdb.grid.GridOSHEntity;
import org.heigit.ohsome.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.ohsome.oshdb.util.CellId;
import org.heigit.ohsome.oshdb.util.TableNames;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.ohsome.oshdb.util.function.OSHEntityFilter;
import org.heigit.ohsome.oshdb.util.function.OSMEntityFilter;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableBinaryOperator;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.heigit.ohsome.oshdb.util.mappable.OSHDBMapReducible;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.heigit.ohsome.oshdb.util.taginterpreter.TagInterpreter;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
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
public class MapReducerIgniteLocalPeek<X> extends MapReducerBase<X> {
  public MapReducerIgniteLocalPeek(OSHDBDatabase oshdb,
      Class<? extends OSHDBMapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducerIgniteLocalPeek(MapReducerIgniteLocalPeek<?> obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducerBase<X> copy() {
    return new MapReducerIgniteLocalPeek<>(this);
  }

  @Override
  protected Stream<X> mapStreamCellsOSMContribution(
      SerializableFunction<OSMContribution, X> mapper
  ) {
    throw new UnsupportedOperationException("Stream function not yet implemented");
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper
  ) {
    throw new UnsupportedOperationException("Stream function not yet implemented");
  }

  @Override
  protected Stream<X> mapStreamCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, X> mapper
  ) {
    throw new UnsupportedOperationException("Stream function not yet implemented");
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper
  ) {
    throw new UnsupportedOperationException("Stream function not yet implemented");
  }

  private List<String> cacheNames(String prefix) {
    return this.typeFilter.stream().map(TableNames::forOSMType).filter(Optional::isPresent)
        .map(Optional::get).map(tn -> tn.toString(prefix)).collect(Collectors.toList());
  }

  @Override
  public boolean isCancelable() {
    return true;
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache((OSHDBIgnite) this.oshdb, identitySupplier, combiner,
        new MapReduceCellsOSMContributionOnIgniteCacheComputeJob<>(
            this.getTagInterpreter(), this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
            this.tstamps.get(), this.bboxFilter, this.getPolyFilter(), this.getPreFilter(),
            this.getFilter(), mapper, identitySupplier, accumulator, combiner));
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache((OSHDBIgnite) this.oshdb, identitySupplier, combiner,
        new FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<>(
            this.getTagInterpreter(), this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
            this.tstamps.get(), this.bboxFilter, this.getPolyFilter(), this.getPreFilter(),
            this.getFilter(), mapper, identitySupplier, accumulator, combiner));
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache((OSHDBIgnite) this.oshdb, identitySupplier, combiner,
        new MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<>(
            this.getTagInterpreter(), this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
            this.tstamps.get(), this.bboxFilter, this.getPolyFilter(), this.getPreFilter(),
            this.getFilter(), mapper, identitySupplier, accumulator, combiner));
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) {
    return mapReduceOnIgniteCache((OSHDBIgnite) this.oshdb, identitySupplier, combiner,
        new FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<>(
            this.getTagInterpreter(), this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
            this.tstamps.get(), this.bboxFilter, this.getPolyFilter(), this.getPreFilter(),
            this.getFilter(), mapper, identitySupplier, accumulator, combiner));
  }

  /**
   * Compute closure that iterates over every partition owned by a node located in a partition.
   */
  private abstract static class MapReduceCellsOnIgniteCacheComputeJob
      <V, R, M, S, P extends Geometry & Polygonal>
      implements CancelableIgniteMapReduceJob<S> {
    private static final Logger LOG =
        LoggerFactory.getLogger(MapReduceCellsOnIgniteCacheComputeJob.class);
    private boolean notCanceled = true;

    /* computation settings */
    private final List<String> cacheNames;
    private final Iterable<CellIdRange> cellIdRanges;
    private final CellIterator cellIterator;
    protected final SerializableFunction<V, M> mapper;
    protected final SerializableSupplier<S> identitySupplier;
    protected final SerializableBiFunction<S, R, S> accumulator;
    protected final SerializableBinaryOperator<S> combiner;

    MapReduceCellsOnIgniteCacheComputeJob(TagInterpreter tagInterpreter, List<String> cacheNames,
        Iterable<CellIdRange> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        OSHEntityFilter preFilter, OSMEntityFilter filter,
        SerializableFunction<V, M> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      this.cacheNames = cacheNames;
      this.cellIdRanges = cellIdRanges;
      this.cellIterator = new CellIterator(
          tstamps, bbox, poly, tagInterpreter, preFilter, filter, false
      );
      this.mapper = mapper;
      this.identitySupplier = identitySupplier;
      this.accumulator = accumulator;
      this.combiner = combiner;
    }

    @Override
    public void cancel() {
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

      CellKeysIterator(Iterable<CellIdRange> cellIdRanges) {
        this.cellIds = Streams.stream(cellIdRanges)
        .filter(ignored -> isActive())
        .flatMap(cellIdRange -> {
          int level = cellIdRange.getStart().getZoomLevel();
          long fromId = cellIdRange.getStart().getId();
          long toId = cellIdRange.getEnd().getId();
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
        if (!buffer.isEmpty()) {
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
        if (buffer.isEmpty()) {
          throw new NoSuchElementException();
        }
        return buffer.remove(buffer.size() - 1);
      }
    }

    S execute(Ignite node, CellProcessor<S> cellProcessor) {
      Set<IgniteCache<Long, GridOSHEntity>> caches = this.cacheNames.stream()
          .map(node::<Long, GridOSHEntity>cache)
          .collect(Collectors.toSet());

      return Streams.stream(new CellKeysIterator(cellIdRanges))
          .parallel()
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
        List<String> cacheNames, Iterable<CellIdRange> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        OSHEntityFilter preFilter, OSMEntityFilter filter,
        SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter,
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
        List<String> cacheNames, Iterable<CellIdRange> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        OSHEntityFilter preFilter, OSMEntityFilter filter,
        SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter,
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
        List<String> cacheNames, Iterable<CellIdRange> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        OSHEntityFilter preFilter, OSMEntityFilter filter,
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
        List<String> cacheNames, Iterable<CellIdRange> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        OSHEntityFilter preFilter, OSMEntityFilter filter,
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

  /**
   * Executes a compute job on all ignite nodes and further reduces and returns result(s).
   *
   * @throws OSHDBTimeoutException if a timeout was set and the computations took too long.
   */
  private static <V, R, M, S, P extends Geometry & Polygonal> S mapReduceOnIgniteCache(
      OSHDBIgnite oshdb, SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner,
      MapReduceCellsOnIgniteCacheComputeJob<V, R, M, S, P> computeJob) {
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();

    ComputeTaskFuture<S> asyncResult = compute.executeAsync(
        new OSHDBIgniteMapReduceComputeTask<>(
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
}
