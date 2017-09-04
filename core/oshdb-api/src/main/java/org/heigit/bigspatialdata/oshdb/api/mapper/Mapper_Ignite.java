package org.heigit.bigspatialdata.oshdb.api.mapper;

import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.vividsolutions.jts.geom.Geometry;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Ignite;
import org.heigit.bigspatialdata.oshdb.api.objects.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.*;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;

import javax.cache.Cache;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.*;
import java.util.stream.Stream;

public class Mapper_Ignite<T> extends Mapper<T> {
  private TagTranslator _tagTranslator = null;

  protected Mapper_Ignite(OSHDB oshdb) {
    super(oshdb);
  }
  
  protected Integer getTagKeyId(String key) throws Exception {
    if (this._tagTranslator == null) this._tagTranslator = new TagTranslator(((OSHDB_H2) this._oshdbForTags).getConnection());
    return this._tagTranslator.key2Int(key);
  }
  
  protected Pair<Integer, Integer> getTagValueId(String key, String value) throws Exception {
    if (this._tagTranslator == null) this._tagTranslator = new TagTranslator(((OSHDB_H2) this._oshdbForTags).getConnection());
    return this._tagTranslator.tag2Int(new ImmutablePair(key,value));
  }

  private<R, S> S _reduceCellsOSMContributionByIgniteCache(String cacheName, Set<CellId> cellIdsList, List<Long> tstamps, BoundingBox bbox, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
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
        entry -> {
          GridOSHEntity oshEntityCell = ((Cache.Entry<Long, GridOSHEntity>)entry).getValue();
          List<R> rs = new ArrayList<>();
          CellIterator.iterateAll(
              oshEntityCell,
              bbox,
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
  protected <R, S> S reduceCellsOSMContribution(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromH2(((OSHDB_H2) this._oshdbForTags).getConnection());

    final Set<CellId> cellIdsList = Sets.newHashSet(cellIds);

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
          System.err.println("unhandled osm type: " + osmType.toString());
          return identitySupplier.get();
      }
      return this._reduceCellsOSMContributionByIgniteCache(
          cacheName,
          cellIdsList,
          tstamps,
          bbox,
          preFilter,
          filter,
          mapper,
          identitySupplier,
          accumulator,
          combiner
      );
    }).reduce(identitySupplier.get(), combiner);
  }


  private <R, S> S _reduceCellsOSMEntitySnapshotByIgniteCache(String cacheName, Set<CellId> cellIdsList, List<Long> tstamps, BoundingBox bbox, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
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
        entry -> {
          GridOSHEntity oshEntityCell = ((Cache.Entry<Long, GridOSHEntity>)entry).getValue();
          List<R> rs = new ArrayList<>();
          CellIterator.iterateByTimestamps(
              oshEntityCell,
              bbox,
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
  protected <R, S> S reduceCellsOSMEntitySnapshot(Iterable<CellId> cellIds, List<Long> tstamps, BoundingBox bbox, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) throws Exception {
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromH2(((OSHDB_H2) this._oshdbForTags).getConnection());

    final Set<CellId> cellIdsList = Sets.newHashSet(cellIds);

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
          System.err.println("unhandled osm type: " + osmType.toString());
          return identitySupplier.get();
      }
      return this._reduceCellsOSMEntitySnapshotByIgniteCache(
          cacheName,
          cellIdsList,
          tstamps,
          bbox,
          preFilter,
          filter,
          mapper,
          identitySupplier,
          accumulator,
          combiner
      );
    }).reduce(identitySupplier.get(), combiner);
  }
}
