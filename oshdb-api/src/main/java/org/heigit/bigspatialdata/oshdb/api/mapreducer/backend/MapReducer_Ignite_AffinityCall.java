package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Geometry;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Database;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Ignite;
import org.heigit.bigspatialdata.oshdb.api.generic.function.*;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.object.OSHDB_MapReducible;
import org.heigit.bigspatialdata.oshdb.api.object.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.object.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.celliterator.CellIterator.TimestampInterval;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.CellId;
import org.heigit.bigspatialdata.oshdb.TableNames;
import org.jetbrains.annotations.NotNull;

/**
 * {@inheritDoc}
 *
 *
 * The "AffinityCall" implementation is a very simple, but less efficient implementation of the
 * oshdb mapreducer: It's just sending separate affinityCalls() to the cluster for each data cell
 * and reduces all results locally on the client.
 *
 * It's good for testing purposes and maybe a viable option for special circumstances where one
 * knows beforehand that only few cells have to be iterated over (e.g. queries in a small area of
 * interest), where the (~constant) overhead associated with the other methods might be larger than
 * the (~linear) inefficiency with this implementation.
 */
public class MapReducer_Ignite_AffinityCall<X> extends MapReducer<X> {
  public MapReducer_Ignite_AffinityCall(OSHDB_Database oshdb,
      Class<? extends OSHDB_MapReducible> forClass) {
    super(oshdb, forClass);
  }

  // copy constructor
  private MapReducer_Ignite_AffinityCall(MapReducer_Ignite_AffinityCall obj) {
    super(obj);
  }

  @NotNull
  @Override
  protected MapReducer<X> copy() {
    return new MapReducer_Ignite_AffinityCall<X>(this);
  }

  @Override
  protected <R, S> S mapReduceCellsOSMContribution(SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    TimestampInterval timestampInterval = new CellIterator.TimestampInterval(
        this._tstamps.get().get(0),
        this._tstamps.get().get(this._tstamps.get().size() - 1)
    );

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    Ignite ignite = ((OSHDB_Ignite) this._oshdb).getIgnite();
    IgniteCompute compute = ignite.compute();

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this._oshdb.prefix());
      IgniteCache<Long, GridOSHEntity> cache = ignite.cache(cacheName);

      return cellIdsList.stream().map(cell -> ZGrid.addZoomToId(cell.getId(), cell.getZoomLevel()))
          .map(cellLongId -> compute.affinityCall(cacheName, cellLongId, () -> {
            GridOSHEntity oshEntityCell = cache.localPeek(cellLongId);
            if (oshEntityCell == null)
              return identitySupplier.get();
            // iterate over the history of all OSM objects in the current cell
            AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
            cellIterator.iterateAll(oshEntityCell, timestampInterval)
                .forEach(contribution -> {
                  OSMContribution osmContribution =
                      new OSMContribution(contribution.timestamp,
                          contribution.nextTimestamp,
                          contribution.previousGeometry, contribution.geometry,
                          contribution.previousOsmEntity, contribution.osmEntity,
                          contribution.activities);
                  accInternal
                      .set(accumulator.apply(accInternal.get(), mapper.apply(osmContribution)));
                });
            return accInternal.get();
          })).reduce(identitySupplier.get(), combiner);
    }).reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    TimestampInterval timestampInterval = new CellIterator.TimestampInterval(
        this._tstamps.get().get(0),
        this._tstamps.get().get(this._tstamps.get().size() - 1)
    );

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    Ignite ignite = ((OSHDB_Ignite) this._oshdb).getIgnite();
    IgniteCompute compute = ignite.compute();

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this._oshdb.prefix());
      IgniteCache<Long, GridOSHEntity> cache = ignite.cache(cacheName);

      return cellIdsList.stream().map(cell -> ZGrid.addZoomToId(cell.getId(), cell.getZoomLevel()))
          .map(cellLongId -> compute.affinityCall(cacheName, cellLongId, () -> {
            GridOSHEntity oshEntityCell = cache.localPeek(cellLongId);
            if (oshEntityCell == null)
              return identitySupplier.get();
            // iterate over the history of all OSM objects in the current cell
            AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
            List<OSMContribution> contributions = new ArrayList<>();
            List<OSHDBTimestamp> tstamps = this._tstamps.get();
            cellIterator.iterateAll(oshEntityCell, timestampInterval)
                .forEach(contribution -> {
                  OSMContribution thisContribution =
                      new OSMContribution(contribution.timestamp,
                          contribution.nextTimestamp,
                          contribution.previousGeometry, contribution.geometry,
                          contribution.previousOsmEntity, contribution.osmEntity,
                          contribution.activities);
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
          })).reduce(identitySupplier.get(), combiner);
    }).reduce(identitySupplier.get(), combiner);
  }


  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner)
      throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    List<OSHDBTimestamp> timestamps = this._tstamps.get();

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    Ignite ignite = ((OSHDB_Ignite) this._oshdb).getIgnite();
    IgniteCompute compute = ignite.compute();

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this._oshdb.prefix());
      IgniteCache<Long, GridOSHEntity> cache = ignite.cache(cacheName);

      return cellIdsList.stream().map(cell -> ZGrid.addZoomToId(cell.getId(), cell.getZoomLevel()))
          .map(cellLongId -> compute.affinityCall(cacheName, cellLongId, () -> {
            GridOSHEntity oshEntityCell = cache.localPeek(cellLongId);
            if (oshEntityCell == null)
              return identitySupplier.get();
            // iterate over the history of all OSM objects in the current cell
            AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
            cellIterator.iterateByTimestamps(oshEntityCell, timestamps)
                .forEach(result -> {
                  result.forEach((timestamp, entityGeometry) -> {
                    Geometry geometry = entityGeometry.getRight();
                    OSMEntity entity = entityGeometry.getLeft();
                    OSMEntitySnapshot snapshot = new OSMEntitySnapshot(timestamp, geometry, entity);
                    // immediately fold the result
                    accInternal.set(accumulator.apply(accInternal.get(), mapper.apply(snapshot)));
                  });
                });
            return accInternal.get();
          })).reduce(identitySupplier.get(), combiner);
    }).reduce(identitySupplier.get(), combiner);
  }

  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner) throws Exception {
    CellIterator cellIterator = new CellIterator(
        this._bboxFilter, this._getPolyFilter(),
        this._getTagInterpreter(), this._getPreFilter(), this._getFilter(), false
    );
    List<OSHDBTimestamp> timestamps = this._tstamps.get();

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    Ignite ignite = ((OSHDB_Ignite) this._oshdb).getIgnite();
    IgniteCompute compute = ignite.compute();

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable) osmType -> {
      String cacheName = TableNames.forOSMType(osmType).get().toString(this._oshdb.prefix());
      IgniteCache<Long, GridOSHEntity> cache = ignite.cache(cacheName);

      return cellIdsList.stream().map(cell -> ZGrid.addZoomToId(cell.getId(), cell.getZoomLevel()))
          .map(cellLongId -> compute.affinityCall(cacheName, cellLongId, () -> {
            GridOSHEntity oshEntityCell = cache.localPeek(cellLongId);
            if (oshEntityCell == null)
              return identitySupplier.get();
            // iterate over the history of all OSM objects in the current cell
            AtomicReference<S> accInternal = new AtomicReference<>(identitySupplier.get());
            cellIterator.iterateByTimestamps(oshEntityCell, timestamps).forEach(snapshots -> {
                  List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>(snapshots.size());
                  snapshots.forEach((timestamp, value) -> {
                    Geometry geometry = value.getRight();
                    OSMEntity entity = value.getLeft();
                    osmEntitySnapshots.add(new OSMEntitySnapshot(timestamp, geometry, entity));
                  });
                  // immediately fold the results
                  for (R r : mapper.apply(osmEntitySnapshots)) {
                    accInternal.set(accumulator.apply(accInternal.get(), r));
                  }
                });
            return accInternal.get();
          })).reduce(identitySupplier.get(), combiner);
    }).reduce(identitySupplier.get(), combiner);
  }
}
