package org.heigit.ohsome.oshdb.api.mapreducer.backend;

import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.api.object.OSMContributionImpl;
import org.heigit.ohsome.oshdb.api.object.OSMEntitySnapshotImpl;
import org.heigit.ohsome.oshdb.util.celliterator.CellIterator;
import org.heigit.ohsome.oshdb.util.celliterator.OSHEntitySource;
import org.heigit.ohsome.oshdb.util.function.SerializableBiFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableFunction;
import org.heigit.ohsome.oshdb.util.function.SerializableSupplier;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;

class Kernels implements Serializable {
  interface CellProcessor<S> extends SerializableBiFunction<OSHEntitySource, CellIterator, S> {}

  interface CancelableProcessStatus {
    default <T> boolean isActive(T ignored) {
      return isActive();
    }

    boolean isActive();
  }

  private static class NonCancelableProcessStatus
      implements CancelableProcessStatus, Serializable {
    @Override
    public boolean isActive() {
      return true;
    }
  }

  private static final CancelableProcessStatus NC = new NonCancelableProcessStatus();

  // === map-reduce processors ===

  @Nonnull
  static <R, S> CellProcessor<S> getOSMContributionCellReducer(
      SerializableFunction<OSMContribution, Optional<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator
  ) {
    return getOSMContributionCellReducer(mapper, identitySupplier, accumulator, NC);
  }

  @Nonnull
  static <R, S> CellProcessor<S> getOSMContributionCellReducer(
      SerializableFunction<OSMContribution, Optional<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      CancelableProcessStatus process
  ) {
    return (source, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByContribution(source)
          .takeWhile(process::isActive)
          .forEach(contribution -> {
            OSMContribution osmContribution = new OSMContributionImpl(contribution);
            mapper.apply(osmContribution).ifPresent(mapped ->
                accInternal.set(accumulator.apply(accInternal.get(), mapped)));
          });
      return accInternal.get();
    };
  }

  @Nonnull
  static <R, S> CellProcessor<S> getOSMContributionGroupingCellReducer(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator
  ) {
    return getOSMContributionGroupingCellReducer(mapper, identitySupplier, accumulator, NC);
  }

  @Nonnull
  static <R, S> CellProcessor<S> getOSMContributionGroupingCellReducer(
      SerializableFunction<List<OSMContribution>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      CancelableProcessStatus process
  ) {
    return (source, cellIterator) -> {
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      // iterate over the history of all OSM objects in the current cell
      List<OSMContribution> contributions = new ArrayList<>();
      cellIterator.iterateByContribution(source)
          .takeWhile(process::isActive)
          .forEach(contribution -> {
            OSMContribution thisContribution = new OSMContributionImpl(contribution);
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
    };
  }

  @Nonnull
  static <R, S> CellProcessor<S> getOSMEntitySnapshotCellReducer(
      SerializableFunction<OSMEntitySnapshot, Optional<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator
  ) {
    return getOSMEntitySnapshotCellReducer(mapper, identitySupplier, accumulator, NC);
  }

  @Nonnull
  static <R, S> CellProcessor<S> getOSMEntitySnapshotCellReducer(
      SerializableFunction<OSMEntitySnapshot, Optional<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      CancelableProcessStatus process
  ) {
    return (source, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByTimestamps(source)
          .takeWhile(process::isActive)
          .forEach(data -> {
            OSMEntitySnapshot snapshot = new OSMEntitySnapshotImpl(data);
            // immediately fold the result
            mapper.apply(snapshot).ifPresent(mapped ->
                accInternal.set(accumulator.apply(accInternal.get(), mapped)));
          });
      return accInternal.get();
    };
  }

  @Nonnull
  static <R, S> CellProcessor<S> getOSMEntitySnapshotGroupingCellReducer(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator
  ) {
    return getOSMEntitySnapshotGroupingCellReducer(mapper, identitySupplier, accumulator, NC);
  }

  @Nonnull
  static <R, S> CellProcessor<S> getOSMEntitySnapshotGroupingCellReducer(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      CancelableProcessStatus process
  ) {
    return (source, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>();
      cellIterator.iterateByTimestamps(source)
          .takeWhile(process::isActive)
          .forEach(data -> {
            OSMEntitySnapshot thisSnapshot = new OSMEntitySnapshotImpl(data);
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
    };
  }

  // === stream processors ===

  @Nonnull
  static <S> CellProcessor<Stream<S>> getOSMContributionCellStreamer(
      SerializableFunction<OSMContribution, Optional<S>> mapper
  ) {
    return getOSMContributionCellStreamer(mapper, NC);
  }

  @Nonnull
  static <S> CellProcessor<Stream<S>> getOSMContributionCellStreamer(
      SerializableFunction<OSMContribution, Optional<S>> mapper,
      CancelableProcessStatus process
  ) {
    // iterate over the history of all OSM objects in the current cell
    return (source, cellIterator) -> cellIterator.iterateByContribution(source)
        .takeWhile(process::isActive)
        .map(OSMContributionImpl::new)
        .map(mapper)
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  @Nonnull
  static <S> CellProcessor<Stream<S>> getOSMContributionGroupingCellStreamer(
      SerializableFunction<List<OSMContribution>, Iterable<S>> mapper
  ) {
    return getOSMContributionGroupingCellStreamer(mapper, NC);
  }

  @Nonnull
  static <S> CellProcessor<Stream<S>> getOSMContributionGroupingCellStreamer(
      SerializableFunction<List<OSMContribution>, Iterable<S>> mapper,
      CancelableProcessStatus process
  ) {
    return (source, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      List<OSMContribution> contributions = new ArrayList<>();
      List<S> result = new LinkedList<>();
      cellIterator.iterateByContribution(source)
          .takeWhile(process::isActive)
          .map(OSMContributionImpl::new)
          .forEach(contribution -> {
            if (contributions.size() > 0 && contribution.getEntityAfter().getId()
                != contributions.get(contributions.size() - 1).getEntityAfter().getId()) {
              // immediately flatten the results
              Iterables.addAll(result, mapper.apply(contributions));
              contributions.clear();
            }
            contributions.add(contribution);
          });
      // apply mapper and fold results one more time for last entity in current cell
      if (contributions.size() > 0) {
        Iterables.addAll(result, mapper.apply(contributions));
      }
      return result.stream();
    };
  }

  @Nonnull
  static <S> CellProcessor<Stream<S>> getOSMEntitySnapshotCellStreamer(
      SerializableFunction<OSMEntitySnapshot, Optional<S>> mapper
  ) {
    return getOSMEntitySnapshotCellStreamer(mapper, NC);
  }

  @Nonnull
  static <S> CellProcessor<Stream<S>> getOSMEntitySnapshotCellStreamer(
      SerializableFunction<OSMEntitySnapshot, Optional<S>> mapper,
      CancelableProcessStatus process
  ) {
    // iterate over the history of all OSM objects in the current cell
    return (source, cellIterator) -> cellIterator.iterateByTimestamps(source)
        .takeWhile(process::isActive)
        .map(OSMEntitySnapshotImpl::new)
        .map(mapper)
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  @Nonnull
  static <S> CellProcessor<Stream<S>> getOSMEntitySnapshotGroupingCellStreamer(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<S>> mapper
  ) {
    return getOSMEntitySnapshotGroupingCellStreamer(mapper, NC);
  }

  @Nonnull
  static <S> CellProcessor<Stream<S>> getOSMEntitySnapshotGroupingCellStreamer(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<S>> mapper,
      CancelableProcessStatus process
  ) {
    return (source, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      List<OSMEntitySnapshot> snapshots = new ArrayList<>();
      List<S> result = new LinkedList<>();
      cellIterator.iterateByTimestamps(source)
          .takeWhile(process::isActive)
          .map(OSMEntitySnapshotImpl::new)
          .forEach(contribution -> {
            if (snapshots.size() > 0 && contribution.getEntity().getId()
                != snapshots.get(snapshots.size() - 1).getEntity().getId()) {
              // immediately flatten the results
              Iterables.addAll(result, mapper.apply(snapshots));
              snapshots.clear();
            }
            snapshots.add(contribution);
          });
      // apply mapper and fold results one more time for last entity in current cell
      if (snapshots.size() > 0) {
        Iterables.addAll(result, mapper.apply(snapshots));
      }
      return result.stream();
    };
  }
}
