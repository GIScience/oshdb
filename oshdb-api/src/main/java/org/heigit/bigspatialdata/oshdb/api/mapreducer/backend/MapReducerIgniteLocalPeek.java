package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
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
import org.heigit.bigspatialdata.oshdb.util.dbhandler.update.UpdateDatabaseHandler;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.util.taginterpreter.TagInterpreter;
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
 * The "LocalPeek" implementation is a very versatile implementation of the oshdb mapreducer on
 * Ignite: It offers high performance, scalability and cancelable queries. It should be used in most
 * situations when running oshdb-analyses on ignite.
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
  public boolean isCancelable() {
    return true;
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {

    Map<OSMType, LongBitmapDataProvider> bitMapIndex = null;
    if (this.update != null) {
      bitMapIndex = UpdateDatabaseHandler.getBitMap(
          this.update.getBitArrayDb()
      );
    }
    //implement Timeoout for updates
    long execStart = System.currentTimeMillis();

    //get regular result
    S resultA = IgniteLocalPeekHelper.mapReduceCellsOSMContributionOnIgniteCache(
        (OSHDBIgnite) this.oshdb, this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
        this.getTagInterpreter(), this.tstamps.get(), this.bboxFilter,
        this.getPolyFilter(), this.getPreFilter(), this.getFilter(), mapper, identitySupplier,
        accumulator, combiner, bitMapIndex);

    S resultB = identitySupplier.get();
    if (this.update != null) {
      CellProcessor<S> cellProcessor = Kernels.getOSMContributionCellReducer(
          mapper,
          identitySupplier,
          accumulator
      );
      resultB = this.getResultFromUpdates(
          execStart,
          cellProcessor,
          identitySupplier,
          combiner,
          bitMapIndex);
    }

    return combiner.apply(resultA, resultB);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {

    Map<OSMType, LongBitmapDataProvider> bitMapIndex = null;
    if (this.update != null) {
      bitMapIndex = UpdateDatabaseHandler.getBitMap(
          this.update.getBitArrayDb()
      );
    }

    //implement Timeoout for updates
    long execStart = System.currentTimeMillis();

    //get regular result
    S resultA = IgniteLocalPeekHelper.flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
        (OSHDBIgnite) this.oshdb, this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
        this.getTagInterpreter(), this.tstamps.get(), this.bboxFilter,
        this.getPolyFilter(), this.getPreFilter(), this.getFilter(), mapper, identitySupplier,
        accumulator, combiner, bitMapIndex);

    S resultB = identitySupplier.get();
    if (this.update != null) {
      CellProcessor<S> cellProcessor = Kernels.getOSMContributionGroupingCellReducer(
          mapper,
          identitySupplier,
          accumulator
      );
      resultB = this.getResultFromUpdates(
          execStart,
          cellProcessor,
          identitySupplier,
          combiner,
          bitMapIndex);
    }

    return combiner.apply(resultA, resultB);
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {

    Map<OSMType, LongBitmapDataProvider> bitMapIndex = null;
    if (this.update != null) {
      bitMapIndex = UpdateDatabaseHandler.getBitMap(
          this.update.getBitArrayDb()
      );
    }

    //implement Timeoout for updates
    long execStart = System.currentTimeMillis();

    //get regular result
    S resultA = IgniteLocalPeekHelper.mapReduceCellsOSMEntitySnapshotOnIgniteCache(
        (OSHDBIgnite) this.oshdb, this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
        this.getTagInterpreter(), this.tstamps.get(), this.bboxFilter,
        this.getPolyFilter(), this.getPreFilter(), this.getFilter(), mapper, identitySupplier,
        accumulator, combiner, bitMapIndex);

    S resultB = identitySupplier.get();
    if (this.update != null) {
      CellProcessor<S> cellProcessor = Kernels.getOSMEntitySnapshotCellReducer(
          mapper,
          identitySupplier,
          accumulator
      );
      resultB = this.getResultFromUpdates(
          execStart,
          cellProcessor,
          identitySupplier,
          combiner,
          bitMapIndex);
    }

    return combiner.apply(resultA, resultB);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {

    Map<OSMType, LongBitmapDataProvider> bitMapIndex = null;
    if (this.update != null) {
      bitMapIndex = UpdateDatabaseHandler.getBitMap(
          this.update.getBitArrayDb()
      );
    }

    //implement Timeoout for updates
    long execStart = System.currentTimeMillis();

    //get regular result
    S resultA = IgniteLocalPeekHelper.flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
        (OSHDBIgnite) this.oshdb, this.cacheNames(this.oshdb.prefix()), this.getCellIdRanges(),
        this.getTagInterpreter(), this.tstamps.get(), this.bboxFilter,
        this.getPolyFilter(), this.getPreFilter(), this.getFilter(), mapper, identitySupplier,
        accumulator, combiner, bitMapIndex);

    S resultB = identitySupplier.get();
    if (this.update != null) {
      CellProcessor<S> cellProcessor = Kernels.getOSMEntitySnapshotGroupingCellReducer(
          mapper,
          identitySupplier,
          accumulator
      );
      resultB = this.getResultFromUpdates(
          execStart,
          cellProcessor,
          identitySupplier,
          combiner,
          bitMapIndex);
    }

    return combiner.apply(resultA, resultB);
  }

  private <R, S> S getResultFromUpdates(
      long execStart,
      CellProcessor<S> cellProcessor,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner,
      Map<OSMType, LongBitmapDataProvider> bitMapIndex)
      throws ClassNotFoundException, ParseException, SQLException, IOException {

    CellIterator updateIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    S resultB = identitySupplier.get();
    if (this.update != null) {
      updateIterator.includeIDsOnly(bitMapIndex);
      resultB = this.getUpdates().parallelStream()
          .filter(ignored -> {
            if (timeout != null && System.currentTimeMillis() - execStart > timeout) {
              throw new OSHDBTimeoutException();
            }
            return true;
          })
          .map(oshCell -> cellProcessor.apply(oshCell, updateIterator))
          .reduce(identitySupplier.get(), combiner);
    }
    return resultB;
  }

}


class IgniteLocalPeekHelper {
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
    final List<String> cacheNames;
    final Iterable<CellIdRange> cellIdRanges;
    final OSHDBBoundingBox bbox;
    final CellIterator cellIterator;
    final SerializableFunction<V, M> mapper;
    final SerializableSupplier<S> identitySupplier;
    final SerializableBiFunction<S, R, S> accumulator;
    final SerializableBinaryOperator<S> combiner;

    MapReduceCellsOnIgniteCacheComputeJob(TagInterpreter tagInterpreter, List<String> cacheNames,
        Iterable<CellIdRange> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<V, M> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
      this.cacheNames = cacheNames;
      this.cellIdRanges = cellIdRanges;
      this.bbox = bbox;
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
        this.cellIds = StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(cellIdRanges.iterator(), 0),
            true
        )
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
        List<String> cacheNames, Iterable<CellIdRange> cellIdRanges,
        SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
      super(tagInterpreter, cacheNames, cellIdRanges,tstamps, bbox, poly, preFilter, filter, mapper,
          identitySupplier, accumulator, combiner,bitMapIndex);
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
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
      super(tagInterpreter, cacheNames, cellIdRanges,tstamps, bbox, poly, preFilter, filter, mapper,
          identitySupplier, accumulator, combiner,bitMapIndex);
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
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
      super(tagInterpreter, cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter,
          mapper, identitySupplier, accumulator, combiner,bitMapIndex);
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
        CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
        SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
        SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner,
        Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
      super(tagInterpreter, cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter,
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
      OSHDBIgnite oshdb, SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner,
      MapReduceCellsOnIgniteCacheComputeJob<V, R, M, S, P> computeJob) {
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();

    ComputeTaskFuture<S> asyncResult = compute.executeAsync(
        new OSHDBIgniteMapReduceComputeTask<Object, S>(
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
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<CellIdRange> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner,
      Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
    return mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new MapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner, bitMapIndex));
  }

  static <R, S, P extends Geometry & Polygonal>
      S flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<CellIdRange> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner,
      Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
    return mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner, bitMapIndex));
  }

  static <R, S, P extends Geometry & Polygonal> S mapReduceCellsOSMEntitySnapshotOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<CellIdRange> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner,
      Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
    return mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner, bitMapIndex));
  }

  static <R, S, P extends Geometry & Polygonal>
      S flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
      OSHDBIgnite oshdb, List<String> cacheNames, Iterable<CellIdRange> cellIdRanges,
      TagInterpreter tagInterpreter,
      SortedSet<OSHDBTimestamp> tstamps, OSHDBBoundingBox bbox, P poly,
      CellIterator.OSHEntityFilter preFilter, CellIterator.OSMEntityFilter filter,
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner,
      Map<OSMType, LongBitmapDataProvider> bitMapIndex) {
    return mapReduceOnIgniteCache(oshdb, identitySupplier, combiner,
        new FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(tagInterpreter,
            cacheNames, cellIdRanges, tstamps, bbox, poly, preFilter, filter, mapper,
            identitySupplier, accumulator, combiner, bitMapIndex));
  }
}
