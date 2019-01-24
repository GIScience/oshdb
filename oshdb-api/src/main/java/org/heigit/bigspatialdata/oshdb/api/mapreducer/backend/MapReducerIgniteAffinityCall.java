package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Streams;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBIgnite;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.backend.Kernels.CellProcessor;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
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
public class MapReducerIgniteAffinityCall<X> extends MapReducer<X> {
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

  @Nonnull
  private static Function<Pair<CellId, CellId>, LongStream> cellIdRangeToCellIds() {
    return cellIdRange -> {
      int level = cellIdRange.getLeft().getZoomLevel();
      long from = CellId.getLevelId(level, cellIdRange.getLeft().getId());
      long to = CellId.getLevelId(level, cellIdRange.getRight().getId());
      return LongStream.rangeClosed(from, to);
    };
  }

  private <S> S reduce(
      CellProcessor<S> cellProcessor,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) throws ParseException, SQLException, IOException {
    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    final Iterable<Pair<CellId, CellId>> cellIdRanges = this.getCellIdRanges();

    OSHDBIgnite oshdb = ((OSHDBIgnite) this.oshdb);
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();

    return this.typeFilter.stream().map((SerializableFunction<OSMType, S>) osmType -> {
      assert TableNames.forOSMType(osmType).isPresent();
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      IgniteCache<Long, GridOSHEntity> cache = ignite.cache(cacheName);

      return Streams.stream(cellIdRanges)
          .flatMapToLong(cellIdRangeToCellIds())
          .parallel()
          .mapToObj(cellLongId -> compute.affinityCall(cacheName, cellLongId, () -> {
            @SuppressWarnings("SerializableStoresNonSerializable")
            GridOSHEntity oshEntityCell = cache.localPeek(cellLongId);
            S ret;
            if (oshEntityCell == null) {
              ret = identitySupplier.get();
            } else {
              ret = cellProcessor.apply(oshEntityCell, cellIterator);
            }
            oshdb.onClose().ifPresent(Runnable::run);
            return ret;
          })).reduce(identitySupplier.get(), combiner);
    }).reduce(identitySupplier.get(), combiner);
  }

  private Stream<X> stream(
      CellProcessor<Collection<X>> processor
  ) throws ParseException, SQLException, IOException {
    CellIterator cellIterator = new CellIterator(
        this.tstamps.get(),
        this.bboxFilter, this.getPolyFilter(),
        this.getTagInterpreter(), this.getPreFilter(), this.getFilter(), false
    );

    final Iterable<Pair<CellId, CellId>> cellIdRanges = this.getCellIdRanges();

    OSHDBIgnite oshdb = ((OSHDBIgnite) this.oshdb);
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();

    return typeFilter.stream().map((SerializableFunction<OSMType, Stream<X>>) osmType -> {
      assert TableNames.forOSMType(osmType).isPresent();
      String cacheName = TableNames.forOSMType(osmType).get().toString(this.oshdb.prefix());
      IgniteCache<Long, GridOSHEntity> cache = ignite.cache(cacheName);

      return Streams.stream(cellIdRanges)
          .flatMapToLong(cellIdRangeToCellIds())
          .parallel()
          .mapToObj(cellLongId -> compute.affinityCall(cacheName, cellLongId, () -> {
            @SuppressWarnings("SerializableStoresNonSerializable")
            GridOSHEntity oshEntityCell = cache.localPeek(cellLongId);
            Collection<X> ret;
            if (oshEntityCell == null) {
              ret = Collections.<X>emptyList();
            } else {
              ret = processor.apply(oshEntityCell, cellIterator);
            }
            oshdb.onClose().ifPresent(Runnable::run);
            return ret;
          }))
          .flatMap(Collection::stream);
    }).flatMap(x -> x);
  }

  // === map-reduce operations ===

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    return this.reduce(
        Kernels.getOSMContributionCellReducer(mapper, identitySupplier, accumulator),
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
        Kernels.getOSMContributionGroupingCellReducer(mapper, identitySupplier, accumulator),
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
        Kernels.getOSMEntitySnapshotCellReducer(mapper, identitySupplier, accumulator),
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
        Kernels.getOSMEntitySnapshotGroupingCellReducer(mapper, identitySupplier, accumulator),
        identitySupplier,
        combiner
    );
  }

  // === stream operations ===

  @Override
  protected Stream<X> mapStreamCellsOSMContribution(
      SerializableFunction<OSMContribution, X> mapper) throws Exception {
    return stream(Kernels.getOSMContributionCellStreamer(mapper));
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, Iterable<X>> mapper) throws Exception {
    return stream(Kernels.getOSMContributionGroupingCellStreamer(mapper));
  }

  @Override
  protected Stream<X> mapStreamCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, X> mapper) throws Exception {
    return stream(Kernels.getOSMEntitySnapshotCellStreamer(mapper));
  }

  @Override
  protected Stream<X> flatMapStreamCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<X>> mapper) throws Exception {
    return stream(Kernels.getOSMEntitySnapshotGroupingCellStreamer(mapper));
  }
}
