package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Ignite;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.*;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.util.*;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.*;

public class MapReducer_Ignite<X> extends MapReducer<X> {
  private static final Logger LOG = LoggerFactory.getLogger(MapReducer_Ignite.class);

  public MapReducer_Ignite(OSHDB oshdb) {
    super(oshdb);
  }

  private<R, S> S _mapReduceCellsOSMContributionOnIgniteCache(
      String cacheName,
      Set<CellId> cellIdsList,
      List<Long> tstamps,
      BoundingBox bbox,
      Polygon poly,
      SerializablePredicate<OSHEntity> preFilter,
      SerializablePredicate<OSMEntity> filter,
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) {
    Ignite ignite = ((OSHDB_Ignite) this._oshdb).getIgnite();
    CacheConfiguration<Long, GridOSHEntity> cacheCfg = new CacheConfiguration<>(cacheName);
    cacheCfg.setStatisticsEnabled(true);
    cacheCfg.setBackups(0);
    cacheCfg.setCacheMode(CacheMode.PARTITIONED);
    IgniteCache<Long, GridOSHEntity> cache = ignite.getOrCreateCache(cacheCfg);

    //noinspection unchecked
    try (QueryCursor<S> cursor = cache.query(
        new ScanQuery((key, cell) -> {
          try {
            return cellIdsList.contains(new CellId(((GridOSHEntity) cell).getLevel(), ((GridOSHEntity) cell).getId()));
          } catch (CellId.cellIdExeption cellIdExeption) {
            cellIdExeption.printStackTrace();
          }
          return false;
        }),
        cacheEntry -> {
          // iterate over the history of all OSM objects in the current cell
          GridOSHEntity oshEntityCell = ((Cache.Entry<Long, GridOSHEntity>)cacheEntry).getValue();
          List<R> rs = new ArrayList<>();
          CellIterator.iterateAll(
              oshEntityCell,
              bbox,
              poly,
              new CellIterator.TimestampInterval(tstamps.get(0), tstamps.get(tstamps.size()-1)),
              this._tagInterpreter,
              preFilter,
              filter,
              false
          ).forEach(contribution -> rs.add(
              mapper.apply(
                  new OSMContribution(
                      new OSHDBTimestamp(contribution.timestamp),
                      contribution.nextTimestamp != null ? new OSHDBTimestamp(contribution.nextTimestamp) : null,
                      contribution.previousGeometry,
                      contribution.geometry,
                      contribution.previousOsmEntity,
                      contribution.osmEntity,
                      contribution.activities
                  )
              )
          ));

          // todo: replace this with `rs.stream().reduce(identitySupplier, accumulator, combiner);` (needs accumulator to be non-interfering and stateless, see http://download.java.net/java/jdk9/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-)
          S accInternal = identitySupplier.get();
          // fold the results
          for (R r : rs) {
            accInternal = accumulator.apply(accInternal, r);
          }
          return accInternal;
        }
    )) {
      S accExternal = identitySupplier.get();
      // reduce the results
      for (S entry : cursor) {
        accExternal = combiner.apply(accExternal, entry);
      }
      return accExternal;
    }
  }
  @Override
  protected <R, S> S mapReduceCellsOSMContribution(
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromJDBC(((OSHDB_H2) this._oshdbForTags).getConnection());

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable)osmType -> {
      String cacheName;
      switch(osmType) {
        case NODE:
          cacheName = "grid_nodes";
          break;
        case WAY:
          cacheName = "grid_ways";
          break;
        case RELATION:
          cacheName = "gid_relations";
          break;
        default:
          LOG.warn("unhandled osm type: " + osmType.toString());
          return identitySupplier.get();
      }
      return this._mapReduceCellsOSMContributionOnIgniteCache(
          cacheName,
          cellIdsList,
          this._tstamps.getTimestamps(),
          this._bboxFilter,
          this._polyFilter,
          this._getPreFilter(),
          this._getFilter(),
          mapper,
          identitySupplier,
          accumulator,
          combiner
      );
    }).reduce(identitySupplier.get(), combiner);
  }

  private<R, S> S _flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
      String cacheName,
      Set<CellId> cellIdsList,
      List<Long> tstamps,
      BoundingBox bbox,
      Polygon poly,
      SerializablePredicate<OSHEntity> preFilter,
      SerializablePredicate<OSMEntity> filter,
      SerializableFunction<List<OSMContribution>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) {
    Ignite ignite = ((OSHDB_Ignite) this._oshdb).getIgnite();
    CacheConfiguration<Long, GridOSHEntity> cacheCfg = new CacheConfiguration<>(cacheName);
    cacheCfg.setStatisticsEnabled(true);
    cacheCfg.setBackups(0);
    cacheCfg.setCacheMode(CacheMode.PARTITIONED);
    IgniteCache<Long, GridOSHEntity> cache = ignite.getOrCreateCache(cacheCfg);

    //noinspection unchecked
    try (QueryCursor<S> cursor = cache.query(
        new ScanQuery((key, cell) -> {
          try {
            return cellIdsList.contains(new CellId(((GridOSHEntity) cell).getLevel(), ((GridOSHEntity) cell).getId()));
          } catch (CellId.cellIdExeption cellIdExeption) {
            cellIdExeption.printStackTrace();
          }
          return false;
        }),
        cacheEntry -> {
          // iterate over the history of all OSM objects in the current cell
          GridOSHEntity oshEntityCell = ((Cache.Entry<Long, GridOSHEntity>)cacheEntry).getValue();
          List<R> rs = new ArrayList<>();
          List<OSMContribution> contributions = new ArrayList<>();
          CellIterator.iterateAll(
              oshEntityCell,
              bbox,
              poly,
              new CellIterator.TimestampInterval(tstamps.get(0), tstamps.get(tstamps.size()-1)),
              this._tagInterpreter,
              preFilter,
              filter,
              false
          ).forEach(contribution -> {
            OSMContribution thisContribution = new OSMContribution(
                new OSHDBTimestamp(contribution.timestamp),
                contribution.nextTimestamp != null ? new OSHDBTimestamp(contribution.nextTimestamp) : null,
                contribution.previousGeometry,
                contribution.geometry,
                contribution.previousOsmEntity,
                contribution.osmEntity,
                contribution.activities
            );
            if (contributions.size() > 0 && thisContribution.getEntityAfter().getId() != contributions.get(contributions.size()-1).getEntityAfter().getId()) {
              rs.addAll(mapper.apply(contributions));
              contributions.clear();
            }
            contributions.add(thisContribution);
          });
          // apply mapper one more time for last entity in current cell
          if (contributions.size() > 0)
            rs.addAll(mapper.apply(contributions));

          // todo: replace this with `rs.stream().reduce(identitySupplier, accumulator, combiner);` (needs accumulator to be non-interfering and stateless, see http://download.java.net/java/jdk9/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-)
          S accInternal = identitySupplier.get();
          // fold the results
          for (R r : rs) {
            accInternal = accumulator.apply(accInternal, r);
          }
          return accInternal;
        }
    )) {
      S accExternal = identitySupplier.get();
      // reduce the results
      for (S entry : cursor) {
        accExternal = combiner.apply(accExternal, entry);
      }
      return accExternal;
    }
  }
  @Override
  protected <R, S> S flatMapReduceCellsOSMContributionGroupedById(
      SerializableFunction<List<OSMContribution>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromJDBC(((OSHDB_H2) this._oshdbForTags).getConnection());

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable)osmType -> {
      String cacheName;
      switch(osmType) {
        case NODE:
          cacheName = "grid_nodes";
          break;
        case WAY:
          cacheName = "grid_ways";
          break;
        case RELATION:
          cacheName = "gid_relations";
          break;
        default:
          LOG.warn("unhandled osm type: " + osmType.toString());
          return identitySupplier.get();
      }
      return this._flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
          cacheName,
          cellIdsList,
          this._tstamps.getTimestamps(),
          this._bboxFilter,
          this._polyFilter,
          this._getPreFilter(),
          this._getFilter(),
          mapper,
          identitySupplier,
          accumulator,
          combiner
      );
    }).reduce(identitySupplier.get(), combiner);
  }


  private <R, S> S _mapReduceCellsOSMEntitySnapshotOnIgniteCache(
      String cacheName,
      Set<CellId> cellIdsList,
      List<Long> tstamps,
      BoundingBox bbox,
      Polygon poly,
      SerializablePredicate<OSHEntity> preFilter,
      SerializablePredicate<OSMEntity> filter,
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) {
    Ignite ignite = ((OSHDB_Ignite) this._oshdb).getIgnite();
    CacheConfiguration<Long, GridOSHEntity> cacheCfg = new CacheConfiguration<>(cacheName);
    cacheCfg.setStatisticsEnabled(true);
    cacheCfg.setBackups(0);
    cacheCfg.setCacheMode(CacheMode.PARTITIONED);
    IgniteCache<Long, GridOSHEntity> cache = ignite.getOrCreateCache(cacheCfg);

    //noinspection unchecked
    try (QueryCursor<S> cursor = cache.query(
        new ScanQuery((key, cell) -> {
          try {
            return cellIdsList.contains(new CellId(((GridOSHEntity) cell).getLevel(), ((GridOSHEntity) cell).getId()));
          } catch (CellId.cellIdExeption cellIdExeption) {
            cellIdExeption.printStackTrace();
          }
          return false;
        }),
        cacheEntry -> {
          GridOSHEntity oshEntityCell = ((Cache.Entry<Long, GridOSHEntity>)cacheEntry).getValue();
          List<R> rs = new ArrayList<>();
          CellIterator.iterateByTimestamps(
              oshEntityCell,
              bbox,
              poly,
              tstamps,
              this._tagInterpreter,
              preFilter,
              filter,
              false
          ).forEach(result -> result.entrySet().forEach(resultEntry -> {
            OSHDBTimestamp tstamp = new OSHDBTimestamp(resultEntry.getKey());
            Geometry geometry = resultEntry.getValue().getRight();
            OSMEntity entity = resultEntry.getValue().getLeft();
            rs.add(mapper.apply(new OSMEntitySnapshot(tstamp, geometry, entity)));
          }));

          // todo: replace this with `rs.stream().reduce(identitySupplier, accumulator, combiner);` (needs accumulator to be non-interfering and stateless, see http://download.java.net/java/jdk9/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-)
          S accInternal = identitySupplier.get();
          // fold the results
          for (R r : rs) {
            accInternal = accumulator.apply(accInternal, r);
          }
          return accInternal;
        }
    )) {
      S accExternal = identitySupplier.get();
      // reduce the results
      for (S entry : cursor) {
        accExternal = combiner.apply(accExternal, entry);
      }
      return accExternal;
    }
  }
  @Override
  protected <R, S> S mapReduceCellsOSMEntitySnapshot(
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromJDBC(((OSHDB_H2) this._oshdbForTags).getConnection());

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable)osmType -> {
      String cacheName;
      switch(osmType) {
        case NODE:
          cacheName = "grid_nodes";
          break;
        case WAY:
          cacheName = "grid_ways";
          break;
        case RELATION:
          cacheName = "gid_relations";
          break;
        default:
          LOG.warn("unhandled osm type: " + osmType.toString());
          return identitySupplier.get();
      }
      return this._mapReduceCellsOSMEntitySnapshotOnIgniteCache(
          cacheName,
          cellIdsList,
          this._tstamps.getTimestamps(),
          this._bboxFilter,
          this._polyFilter,
          this._getPreFilter(),
          this._getFilter(),
          mapper,
          identitySupplier,
          accumulator,
          combiner
      );
    }).reduce(identitySupplier.get(), combiner);
  }

  private <R, S> S _flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
      String cacheName,
      Set<CellId> cellIdsList,
      List<Long> tstamps, BoundingBox bbox,
      Polygon poly,
      SerializablePredicate<OSHEntity> preFilter,
      SerializablePredicate<OSMEntity> filter,
      SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) {
    Ignite ignite = ((OSHDB_Ignite) this._oshdb).getIgnite();
    CacheConfiguration<Long, GridOSHEntity> cacheCfg = new CacheConfiguration<>(cacheName);
    cacheCfg.setStatisticsEnabled(true);
    cacheCfg.setBackups(0);
    cacheCfg.setCacheMode(CacheMode.PARTITIONED);
    IgniteCache<Long, GridOSHEntity> cache = ignite.getOrCreateCache(cacheCfg);

    //noinspection unchecked
    try (QueryCursor<S> cursor = cache.query(
        new ScanQuery((key, cell) -> {
          try {
            return cellIdsList.contains(new CellId(((GridOSHEntity) cell).getLevel(), ((GridOSHEntity) cell).getId()));
          } catch (CellId.cellIdExeption cellIdExeption) {
            cellIdExeption.printStackTrace();
          }
          return false;
        }),
        cacheEntry -> {
          GridOSHEntity oshEntityCell = ((Cache.Entry<Long, GridOSHEntity>)cacheEntry).getValue();
          List<R> rs = new ArrayList<>();
          CellIterator.iterateByTimestamps(
              oshEntityCell,
              bbox,
              poly,
              tstamps,
              this._tagInterpreter,
              preFilter,
              filter,
              false
          ).forEach(snapshots -> {
            List<OSMEntitySnapshot> osmEntitySnapshots = new ArrayList<>(snapshots.size());
            snapshots.entrySet().forEach(entry -> {
              OSHDBTimestamp tstamp = new OSHDBTimestamp(entry.getKey());
              Geometry geometry = entry.getValue().getRight();
              OSMEntity entity = entry.getValue().getLeft();
              osmEntitySnapshots.add(new OSMEntitySnapshot(tstamp, geometry, entity));
            });
            rs.addAll(mapper.apply(osmEntitySnapshots));
          });

          // todo: replace this with `rs.stream().reduce(identitySupplier, accumulator, combiner);` (needs accumulator to be non-interfering and stateless, see http://download.java.net/java/jdk9/docs/api/java/util/stream/Stream.html#reduce-U-java.util.function.BiFunction-java.util.function.BinaryOperator-)
          S accInternal = identitySupplier.get();
          // fold the results
          for (R r : rs) {
            accInternal = accumulator.apply(accInternal, r);
          }
          return accInternal;
        }
    )) {
      S accExternal = identitySupplier.get();
      // reduce the results
      for (S entry : cursor) {
        accExternal = combiner.apply(accExternal, entry);
      }
      return accExternal;
    }
  }
  @Override
  protected <R, S> S flatMapReduceCellsOSMEntitySnapshotGroupedById(
      SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromJDBC(((OSHDB_H2) this._oshdbForTags).getConnection());

    final Set<CellId> cellIdsList = Sets.newHashSet(this._getCellIds());

    return this._typeFilter.stream().map((Function<OSMType, S> & Serializable)osmType -> {
      String cacheName;
      switch(osmType) {
        case NODE:
          cacheName = "grid_nodes";
          break;
        case WAY:
          cacheName = "grid_ways";
          break;
        case RELATION:
          cacheName = "gid_relations";
          break;
        default:
          LOG.warn("unhandled osm type: " + osmType.toString());
          return identitySupplier.get();
      }
      return this._flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
          cacheName,
          cellIdsList,
          this._tstamps.getTimestamps(),
          this._bboxFilter,
          this._polyFilter,
          this._getPreFilter(),
          this._getFilter(),
          mapper,
          identitySupplier,
          accumulator,
          combiner
      );
    }).reduce(identitySupplier.get(), combiner);
  }
}
