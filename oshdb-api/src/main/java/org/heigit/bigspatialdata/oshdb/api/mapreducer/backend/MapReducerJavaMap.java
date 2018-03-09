package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDBDatabase;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBiFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableBinaryOperator;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableFunction;
import org.heigit.bigspatialdata.oshdb.api.generic.function.SerializableSupplier;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDBMapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.time.OSHDBTimestampInterval;
import org.json.simple.parser.ParseException;

public class MapReducerJavaMap<X> extends MapReducer<X>  {
  
  /**
   * 
   */
  private static final long serialVersionUID = -558543033850509774L;
  
  final Map<OSMType,Map<Long,GridOSHEntity>> database;
  public MapReducerJavaMap(OSHDBDatabase oshdb, Class<? extends OSHDBMapReducible> forClass, Map<OSMType,Map<Long,GridOSHEntity>> database) {
    super(oshdb, forClass);
    this.database = database;
  }

  protected MapReducerJavaMap(MapReducerJavaMap<?> obj) {
    super(obj);
    this.database = obj.database;
   
  }

  @Override
  protected MapReducer<X> copy() {
    return new MapReducerJavaMap<>(this);
  }
  
  private interface Callback<S> extends Serializable {
    S apply (
        GridOSHEntity oshEntityCell,
        CellIterator cellIterator,
        OSHDBTimestampInterval timestampInterval,
        SortedSet<OSHDBTimestamp> timestamps
    );
  }
  
  
  
  private <S> S run(
      Callback<S> callback,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner
  ) throws ParseException, SQLException, IOException, ClassNotFoundException {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );

    SortedSet<OSHDBTimestamp> timestamps = this._tstamps.get();
    OSHDBTimestampInterval timestampInterval = new OSHDBTimestampInterval(timestamps);
    
    final List<Pair<CellId, CellId>> cellIdRanges = new ArrayList<>();
    this._getCellIdRanges().forEach(cellIdRanges::add);
    
    return this._typeFilter.stream()
    .map(type -> database.get(type))
    .flatMap(grids -> cellIdRanges.stream()
          .flatMap(r -> LongStream.rangeClosed(r.getLeft().getId(), r.getRight().getId()).mapToObj(cell -> grids.get(cell)))
    )
    .filter(oshCell-> oshCell != null)
    .map(oshCell -> (S)callback.apply(oshCell, cellIterator, timestampInterval, timestamps))
    .reduce(identitySupplier.get(), combiner);
  }
  
  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return this.run((oshEntityCell, cellIterator, timestampInterval, ignored) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByContribution(oshEntityCell, timestampInterval).forEach(contribution -> {
        OSMContribution osmContribution = new OSMContribution(contribution);
        accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
      });
      return accInternal.get();
    }, identitySupplier, combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return this.run((oshEntityCell, cellIterator, timestampInterval, ignored) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      List<OSMContribution> contributions = new ArrayList<>();
      cellIterator.iterateByContribution(oshEntityCell, timestampInterval).forEach(contribution -> {
        OSMContribution thisContribution = new OSMContribution(contribution);
        if (contributions.size() > 0 && thisContribution.getEntityAfter()
            .getId() != contributions.get(contributions.size() - 1).getEntityAfter().getId()) {
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
    }, identitySupplier, combiner);
  }

  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    return this.run((oshEntityCell, cellIterator, ignored, timestamps) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      cellIterator.iterateByTimestamps(oshEntityCell, timestamps).forEach(data -> {
        OSMEntitySnapshot snapshot = new OSMEntitySnapshot(data);
        // immediately fold the result
        accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
      });
      return accInternal.get();
    }, identitySupplier, combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    return this.run((oshEntityCell, cellIterator, ignored, timestamps) -> {
      // iterate over the history of all OSM objects in the current cell
      AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
      List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>();
      cellIterator.iterateByTimestamps(oshEntityCell, timestamps).forEach(data -> {
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
    }, identitySupplier, combiner);
  }
}
