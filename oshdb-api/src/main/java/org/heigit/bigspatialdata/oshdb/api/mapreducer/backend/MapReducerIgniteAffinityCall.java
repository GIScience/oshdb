package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Streams;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
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
import org.heigit.bigspatialdata.oshdb.index.XYGridTree.CellIdRange;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.exceptions.OSHDBTimeoutException;
import org.jetbrains.annotations.NotNull;
import org.json.simple.parser.ParseException;

/**
 * {@inheritDoc}
 *
 * <p>
 * The "AffinityCall" implementation is a very simple, but less efficient implementation of the
 * oshdb mapreducer: It's just sending separate affinityCalls() to the cluster for each data cell
 * and reduces all results locally on the client.
 * </p>
 *
 * <p>
 * It's good for testing purposes and maybe a viable option for special circumstances where one
 * knows beforehand that only few cells have to be iterated over (e.g. queries in a small area of
 * interest), where the (~constant) overhead associated with the other methods might be larger than
 * the (~linear) inefficiency with this implementation.
 * </p>
 */
public class MapReducerIgniteAffinityCall<X> extends MapReducer<X>
    implements CancelableProcessStatus {

  /**
   * Stores the start time of reduce/stream operation as returned by
   * {@link System#currentTimeMillis()}. Used to determine query timeouts.
   */
  private long executionStartTimeMillis;

  public MapReducerIgniteAffinityCall(OSHDBDatabase oshdb,
      Class<? extends OSHDBMapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducerIgniteAffinityCall(MapReducerIgniteAffinityCall obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducerIgniteAffinityCall<X>(this);
  }

  @Override
  public boolean isCancelable() {
    return true;
  }

  @Override
  public boolean isActive() {
    if (timeout != null && System.currentTimeMillis() - executionStartTimeMillis > timeout) {
      throw new OSHDBTimeoutException();
    }
    return true;
  }

  @Nonnull
  private static SerializableFunction<CellIdRange, LongStream> cellIdRangeToCellIds() {
    return cellIdRange -> {
      int level = cellIdRange.getStart().getZoomLevel();
      long from = CellId.getLevelId(level, cellIdRange.getStart().getId());
      long to = CellId.getLevelId(level, cellIdRange.getEnd().getId());
      return LongStream.rangeClosed(from, to);
    };
  }

  private static <T> T asyncGetHandleTimeouts(IgniteFuture<T> async) {
    try {
      return async.get();
    } catch (IgniteException e) {
      if (e.getCause().getCause() instanceof OSHDBTimeoutException) {
        throw (OSHDBTimeoutException) e.getCause().getCause();
      } else {
        throw e;
      }
    }
  }

  /**
   * Implements a generic reduce operation.
   *
   * @throws OSHDBTimeoutException if a timeout was set and the computations took too long.
   */
  private <S> S reduce(
      CellProcessor<S> cellProcessor,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) throws ParseException, SQLException, IOException {
    this.executionStartTimeMillis = System.currentTimeMillis();

    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    final Iterable<CellIdRange> cellIdRanges = this.getCellIdRanges();

    OSHDBIgnite oshdb = (OSHDBIgnite) this.oshdb;
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();
    IgniteRunnable onClose = oshdb.onClose().orElse(() -> { });

    return this.typeFilter.stream().map((SerializableFunction<OSMType, S>) osmType -> {
      assert TableNames.forOSMType(osmType).isPresent();
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      IgniteCache<Long, GridOSHEntity> cache = ignite.cache(cacheName);

      return Streams.stream(cellIdRanges)
          .flatMapToLong(cellIdRangeToCellIds())
          .parallel()
          .filter(ignored -> this.isActive())
          .mapToObj(cellLongId -> asyncGetHandleTimeouts(
              compute.affinityCallAsync(cacheName, cellLongId, () -> {
                @SuppressWarnings("SerializableStoresNonSerializable")
                GridOSHEntity oshEntityCell = cache.localPeek(cellLongId);
                S ret;
                if (oshEntityCell == null) {
                  ret = identitySupplier.get();

                } else {
                  ret = cellProcessor.apply(oshEntityCell, cellIterator);
                }
                onClose.run();
                return ret;
              })
          ))
          .reduce(identitySupplier.get(), combiner);
    }).reduce(identitySupplier.get(), combiner);
  }

  /**
   * Implements a generic stream operation.
   *
   * @throws OSHDBTimeoutException if a timeout was set and the computations took too long.
   */
  private Stream<X> stream(
      CellProcessor<Stream<X>> cellProcessor
  ) throws ParseException, SQLException, IOException {
    this.executionStartTimeMillis = System.currentTimeMillis();

    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    final Iterable<CellIdRange> cellIdRanges = this.getCellIdRanges();

    OSHDBIgnite oshdb = (OSHDBIgnite) this.oshdb;
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();
    IgniteRunnable onClose = oshdb.onClose().orElse(() -> { });

    Stream<X> result = Stream.empty();
    for (OSMType osmType : typeFilter) {
      assert TableNames.forOSMType(osmType).isPresent();
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      IgniteCache<Long, GridOSHEntity> cache = ignite.cache(cacheName);

      Stream<X> resultForType = compute.broadcastAsync(new GetMatchingKeysPreflight(
              cacheName, cellIdRangeToCellIds(), cellIdRanges, cellProcessor, cellIterator
          )).get(this.timeout)
          .stream()
          .flatMap(Collection::stream).parallel()
          .filter(ignored -> this.isActive())
          .map(cellLongId -> asyncGetHandleTimeouts(
              compute.affinityCallAsync(cacheName, cellLongId, () -> {
                GridOSHEntity oshEntityCell = cache.localPeek(cellLongId);
                Collection<X> ret;
                if (oshEntityCell == null) {
                  ret = Collections.<X>emptyList();
                } else {
                  ret = cellProcessor.apply(oshEntityCell, cellIterator)
                      .collect(Collectors.toList());
                }
                onClose.run();
                return ret;
              })
          ))
          .flatMap(Collection::stream);
      result = Stream.concat(result, resultForType);
    }
    return result;
  }

  // === map-reduce operations ===

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return reduce(
        Kernels.getOSMContributionCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return reduce(
        Kernels.getOSMContributionGroupingCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return reduce(
        Kernels.getOSMEntitySnapshotCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return reduce(
        Kernels.getOSMEntitySnapshotGroupingCellReducer(
            mapper,
            identitySupplier,
            accumulator,
            this
        ),
        identitySupplier,
        combiner
    );
  }

  // === stream operations ===

  @Override
  protected Stream<X> mapStreamCellsOSMContribution(
      SerializableFunction<OSMContribution, X> mapper) throws Exception {
    return stream(Kernels.getOSMContributionCellStreamer(mapper, this));
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper) throws Exception {
    return stream(Kernels.getOSMContributionGroupingCellStreamer(mapper, this));
  }

  @Override
  protected Stream<X> mapStreamCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, X> mapper) throws Exception {
    return stream(Kernels.getOSMEntitySnapshotCellStreamer(mapper, this));
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper) throws Exception {
    return stream(Kernels.getOSMEntitySnapshotGroupingCellStreamer(mapper, this));
  }

  private static class GetMatchingKeysPreflight implements IgniteCallable<Collection<Long>> {
    @IgniteInstanceResource
    private Ignite ignite;

    private final String cacheName;
    private final Function<CellIdRange, LongStream> cellIdRangeToCellIds;
    private final Iterable<CellIdRange> cellIdRanges;
    private final CellProcessor<? extends Stream<?>> cellProcessor;
    private final CellIterator cellIterator;

    public GetMatchingKeysPreflight(
        String cacheName,
        Function<CellIdRange, LongStream> cellIdRangeToCellIds,
        Iterable<CellIdRange> cellIdRanges,
        CellProcessor<? extends Stream<?>> cellProcessor,
        CellIterator cellIterator
    ) {
      this.cacheName = cacheName;
      this.cellIdRangeToCellIds = cellIdRangeToCellIds;
      this.cellIdRanges = cellIdRanges;
      this.cellProcessor = cellProcessor;
      this.cellIterator = cellIterator;
    }

    @Override
    public Collection<Long> call() throws Exception {
      IgniteCache<Long, GridOSHEntity> localCache = ignite.cache(cacheName);
      return Streams.stream(cellIdRanges)
          .flatMapToLong(cellIdRangeToCellIds)
          .parallel()
          .filter(cellLongId -> {
            // test if cell exists and contains any relevant data
            GridOSHEntity cell = localCache.localPeek(cellLongId);
            return cell != null && cellProcessor.apply(cell, cellIterator).findAny().isPresent();
          })
          .boxed()
          .collect(Collectors.toList());
    }
  }
}
