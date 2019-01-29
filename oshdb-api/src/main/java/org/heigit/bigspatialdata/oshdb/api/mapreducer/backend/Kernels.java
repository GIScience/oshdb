package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;

class Kernels implements Serializable {
  interface CellProcessor<S> extends SerializableBiFunction<GridOSHEntity, CellIterator, S> {}

  interface CancelableProcessStatus {
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
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator
  ) {
    return getOSMContributionCellReducer(mapper, identitySupplier, accumulator, NC);
  }

  @Nonnull
  static <R, S> CellProcessor<S> getOSMContributionCellReducer(
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      CancelableProcessStatus aborter
  ) {
    return (oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByContribution(oshEntityCell)
          .filter(ignored -> aborter.isActive())
          .forEach(contribution -> {
            OSMContribution osmContribution = new OSMContribution(contribution);
            accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
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
      CancelableProcessStatus aborter
  ) {
    return (oshEntityCell, cellIterator) -> {
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      // iterate over the history of all OSM objects in the current cell
      List<OSMContribution> contributions = new ArrayList<>();
      cellIterator.iterateByContribution(oshEntityCell)
          .filter(ignored -> aborter.isActive())
          .forEach(contribution -> {
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
    };
  }

  @Nonnull
  static <R, S> CellProcessor<S> getOSMEntitySnapshotCellReducer(
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator
  ) {
    return getOSMEntitySnapshotCellReducer(mapper, identitySupplier, accumulator, NC);
  }

  @Nonnull
  static <R, S> CellProcessor<S> getOSMEntitySnapshotCellReducer(
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      CancelableProcessStatus aborter
  ) {
    return (oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByTimestamps(oshEntityCell)
          .filter(ignored -> aborter.isActive())
          .forEach(data -> {
            OSMEntitySnapshot snapshot = new OSMEntitySnapshot(data);
            // immediately fold the result
            accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
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
      CancelableProcessStatus aborter
  ) {
    return (oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>();
      cellIterator.iterateByTimestamps(oshEntityCell)
          .filter(ignored -> aborter.isActive())
          .forEach(data -> {
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
    };
  }

  // === stream processors ===

  @Nonnull
  static <S> CellProcessor<Collection<S>> getOSMContributionCellStreamer(
      SerializableFunction<OSMContribution, S> mapper
  ) {
    return (oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      return cellIterator.iterateByContribution(oshEntityCell)
          .map(OSMContribution::new)
          .map(mapper)
          .collect(Collectors.toList());
    };
  }

  @Nonnull
  static <S> CellProcessor<Collection<S>> getOSMContributionGroupingCellStreamer(
      SerializableFunction<List<OSMContribution>, Iterable<S>> mapper
  ) {
    return (oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      List<OSMContribution> contributions = new ArrayList<>();
      List<S> result = new LinkedList<>();
      cellIterator.iterateByContribution(oshEntityCell)
          .map(OSMContribution::new)
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
      return result;
    };
  }

  @Nonnull
  static <S> CellProcessor<Collection<S>> getOSMEntitySnapshotCellStreamer(
      SerializableFunction<OSMEntitySnapshot, S> mapper
  ) {
    return (oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      return cellIterator.iterateByTimestamps(oshEntityCell)
          .map(OSMEntitySnapshot::new)
          .map(mapper)
          .collect(Collectors.toList());
    };
  }

  @Nonnull
  static <S> CellProcessor<Collection<S>> getOSMEntitySnapshotGroupingCellStreamer(
      SerializableFunction<List<OSMEntitySnapshot>, Iterable<S>> mapper
  ) {
    return (oshEntityCell, cellIterator) -> {
      // iterate over the history of all OSM objects in the current cell
      List<OSMEntitySnapshot> snapshots = new ArrayList<>();
      List<S> result = new LinkedList<>();
      cellIterator.iterateByTimestamps(oshEntityCell)
          .map(OSMEntitySnapshot::new)
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
      return result;
    };
  }
}
