package org.heigit.bigspatialdata.oshdb.api.mapreducer.backend;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygonal;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.heigit.bigspatialdata.oshdb.OSHDB;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_H2;
import org.heigit.bigspatialdata.oshdb.api.db.OSHDB_Ignite;
import org.heigit.bigspatialdata.oshdb.api.exceptions.OSHDBTimeoutException;
import org.heigit.bigspatialdata.oshdb.api.generic.lambdas.*;
import org.heigit.bigspatialdata.oshdb.api.mapreducer.MapReducer;
import org.heigit.bigspatialdata.oshdb.api.utils.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMContribution;
import org.heigit.bigspatialdata.oshdb.api.objects.OSMEntitySnapshot;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.index.XYGridTree;
import org.heigit.bigspatialdata.oshdb.index.zfc.ZGrid;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.util.BoundingBox;
import org.heigit.bigspatialdata.oshdb.util.CellIterator;
import org.heigit.bigspatialdata.oshdb.util.TableNames;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.DefaultTagInterpreter;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class MapReducer_Ignite_LocalPeek<X> extends MapReducer<X> {
  public MapReducer_Ignite_LocalPeek(OSHDB oshdb) {
    super(oshdb);
  }

  // copy constructor
  public MapReducer_Ignite_LocalPeek(MapReducer_Ignite_LocalPeek obj) {
    super(obj);
  }

  private List<String> cacheNames() {
    return this._typeFilter.stream()
    .map(TableNames::forOSMType)
    .filter(Optional::isPresent)
    .map(Optional::get)
    .map(TableNames::toString)
    .collect(Collectors.toList());
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

    return Ignite_LocalPeek_Helper._mapReduceCellsOSMContributionOnIgniteCache(
        (OSHDB_Ignite)this._oshdb,
        this.cacheNames(),
        this._tagInterpreter,
        this._tstamps.getTimestamps(),
        this._bboxFilter,
        this._getPolyFilter(),
        this._getPreFilter(),
        this._getFilter(),
        mapper,
        identitySupplier,
        accumulator,
        combiner
    );
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

    return Ignite_LocalPeek_Helper._flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
        (OSHDB_Ignite)this._oshdb,
        this.cacheNames(),
        this._tagInterpreter,
        this._tstamps.getTimestamps(),
        this._bboxFilter,
        this._getPolyFilter(),
        this._getPreFilter(),
        this._getFilter(),
        mapper,
        identitySupplier,
        accumulator,
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
    //load tag interpreter helper which is later used for geometry building
    if (this._tagInterpreter == null) this._tagInterpreter = DefaultTagInterpreter.fromJDBC(((OSHDB_H2) this._oshdbForTags).getConnection());

    return Ignite_LocalPeek_Helper._mapReduceCellsOSMEntitySnapshotOnIgniteCache(
        (OSHDB_Ignite)this._oshdb,
        this.cacheNames(),
        this._tagInterpreter,
        this._tstamps.getTimestamps(),
        this._bboxFilter,
        this._getPolyFilter(),
        this._getPreFilter(),
        this._getFilter(),
        mapper,
        identitySupplier,
        accumulator,
        combiner
    );
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

    return Ignite_LocalPeek_Helper._flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
        (OSHDB_Ignite)this._oshdb,
        this.cacheNames(),
        this._tagInterpreter,
        this._tstamps.getTimestamps(),
        this._bboxFilter,
        this._getPolyFilter(),
        this._getPreFilter(),
        this._getFilter(),
        mapper,
        identitySupplier,
        accumulator,
        combiner
    );
  }
}

class Ignite_LocalPeek_Helper {
  /**
   * @param <T> Type of the task argument.
   * @param <R> Type of the task result returning from {@link ComputeTask#reduce(List)} method.
   */
  @org.apache.ignite.compute.ComputeTaskNoResultCache
  static class CancelableBroadcastTask<T, R> extends ComputeTaskAdapter<T, R> implements Serializable {
    private final MapReduceCellsOnIgniteCacheComputeJob job;
    private final SerializableBinaryOperator<R> combiner;

    private R resultAccumulator;

    public CancelableBroadcastTask(MapReduceCellsOnIgniteCacheComputeJob job, SerializableSupplier<R> identitySupplier, SerializableBinaryOperator<R> combiner) {
      this.job = job;
      this.combiner = combiner;
      this.resultAccumulator = identitySupplier.get();
    }

    @Override
    public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, T arg) throws IgniteException {
      Map<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());
      subgrid.forEach(node -> map.put(new ComputeJob() {
        @IgniteInstanceResource
        private Ignite ignite;
        @Override
        public void cancel() {
          job.cancel();
        }
        @Override
        public Object execute() throws IgniteException {
          return job.execute(ignite);
        }
      }, node));
      return map;
    }

    @Override
    public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
      R data = res.getData();
      resultAccumulator = combiner.apply(resultAccumulator, data);
      return ComputeJobResultPolicy.WAIT;
    }

    @Override
    public R reduce(List<ComputeJobResult> results) throws IgniteException {
      return resultAccumulator;
    }
  }

  /**
   * Compute closure that iterates over every partition owned by a node
   * located in a partition.
   */
  private static abstract class MapReduceCellsOnIgniteCacheComputeJob<V, R, MR, S, P extends Geometry & Polygonal> implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(MapReduceCellsOnIgniteCacheComputeJob.class);
    boolean canceled = false;

    /* computation settings */
    final TagInterpreter tagInterpreter;
    final List<String> cacheNames;
    final List<Long> tstamps;
    final BoundingBox bbox;
    final P poly;
    final SerializablePredicate<OSHEntity> preFilter;
    final SerializablePredicate<OSMEntity> filter;
    final SerializableFunction<V, MR> mapper;
    final SerializableSupplier<S> identitySupplier;
    final SerializableBiFunction<S, R, S> accumulator;
    final SerializableBinaryOperator<S> combiner;

    MapReduceCellsOnIgniteCacheComputeJob(
        TagInterpreter tagInterpreter,
        List<String> cacheNames,
        List<Long> tstamps,
        BoundingBox bbox,
        P poly,
        SerializablePredicate<OSHEntity> preFilter,
        SerializablePredicate<OSMEntity> filter,
        SerializableFunction<V, MR> mapper,
        SerializableSupplier<S> identitySupplier,
        SerializableBiFunction<S, R, S> accumulator,
        SerializableBinaryOperator<S> combiner) {
      this.tagInterpreter = tagInterpreter;
      this.cacheNames = cacheNames;
      this.tstamps = tstamps;
      this.bbox = bbox;
      this.poly = poly;
      this.preFilter = preFilter;
      this.filter = filter;
      this.mapper = mapper;
      this.identitySupplier = identitySupplier;
      this.accumulator = accumulator;
      this.combiner = combiner;
    }

    void cancel() {
      LOG.info("compute job canceled");
      this.canceled = true;
    }

    public abstract S execute(Ignite node);

    List<Pair<IgniteCache<Long, GridOSHEntity>, Long>> localKeys(Ignite node) {
      // calculate all cache keys we have to investigate
      XYGridTree grid = new XYGridTree(OSHDB.MAXZOOM);
      Collection<Long> keys = new LinkedList<>();
      grid.bbox2CellIds(bbox, true).forEach(cell -> keys.add(ZGrid.addZoomToId(cell.getId(), cell.getZoomLevel())));
      List<Pair<IgniteCache<Long, GridOSHEntity>, Long>> localKeys = new ArrayList<>(keys.size());
      this.cacheNames.forEach(cacheName -> {
        IgniteCache<Long, GridOSHEntity> cache = node.cache(cacheName);
        // Map all keys to ignite nodes
        Map<ClusterNode, Collection<Long>> mappings = node.<Long>affinity(cache.getName()).mapKeysToNodes(keys);
        Collection<Long> cacheLocalKeys = mappings.get(node.cluster().localNode());
        cacheLocalKeys.forEach(key ->
            localKeys.add(new ImmutablePair<>(cache, key))
        );
      });
      return localKeys;
    }
  }

  private static class MapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal> extends MapReduceCellsOnIgniteCacheComputeJob<OSMContribution, R, R, S, P> {
    MapReduceCellsOSMContributionOnIgniteCacheComputeJob(TagInterpreter tagInterpreter, List<String> cacheNames, List<Long> tstamps, BoundingBox bbox, P poly, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMContribution, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, tstamps, bbox, poly, preFilter, filter, mapper, identitySupplier, accumulator, combiner);
    }

    @Override
    public S execute(Ignite node) {
      return this.localKeys(node).parallelStream()
      .map(cacheKey -> cacheKey.getLeft().localPeek(cacheKey.getRight()))
      .filter(Objects::nonNull) // filter out cache misses === empty oshdb cells
      .map(oshEntityCell -> {
        if (this.canceled) return identitySupplier.get();
        // iterate over the history of all OSM objects in the current cell
        List<R> rs = new ArrayList<>();
        CellIterator.iterateAll(
            oshEntityCell,
            bbox,
            poly,
            new CellIterator.TimestampInterval(tstamps.get(0), tstamps.get(tstamps.size() - 1)),
            tagInterpreter,
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
        S accInternal = identitySupplier.get();
        // fold the results
        for (R r : rs) {
          accInternal = accumulator.apply(accInternal, r);
        }
        return accInternal;
      }).reduce(identitySupplier.get(), combiner);
    }
  }

  private static class FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal> extends MapReduceCellsOnIgniteCacheComputeJob<List<OSMContribution>, R, List<R>, S, P> {
    FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob(TagInterpreter tagInterpreter, List<String> cacheNames, List<Long> tstamps, BoundingBox bbox, P poly, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<List<OSMContribution>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, tstamps, bbox, poly, preFilter, filter, mapper, identitySupplier, accumulator, combiner);
    }

    @Override
    public S execute(Ignite node) {
      return this.localKeys(node).parallelStream()
      .map(cacheKey -> cacheKey.getLeft().localPeek(cacheKey.getRight()))
      .filter(Objects::nonNull) // filter out cache misses === empty oshdb cells
      .map(oshEntityCell -> {
        if (this.canceled) return identitySupplier.get();
        // iterate over the history of all OSM objects in the current cell
        List<R> rs = new ArrayList<>();
        List<OSMContribution> contributions = new ArrayList<>();
        CellIterator.iterateAll(
            oshEntityCell,
            bbox,
            poly,
            new CellIterator.TimestampInterval(tstamps.get(0), tstamps.get(tstamps.size()-1)),
            tagInterpreter,
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

        S accInternal = identitySupplier.get();
        // fold the results
        for (R r : rs) {
          accInternal = accumulator.apply(accInternal, r);
        }
        return accInternal;
      }).reduce(identitySupplier.get(), combiner);
    }
  }

  private static class MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal> extends MapReduceCellsOnIgniteCacheComputeJob<OSMEntitySnapshot, R, R, S, P> {
    MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob(TagInterpreter tagInterpreter, List<String> cacheNames, List<Long> tstamps, BoundingBox bbox, P poly, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<OSMEntitySnapshot, R> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, tstamps, bbox, poly, preFilter, filter, mapper, identitySupplier, accumulator, combiner);
    }

    @Override
    public S execute(Ignite node) {
      return this.localKeys(node).parallelStream()
      .map(cacheKey -> cacheKey.getLeft().localPeek(cacheKey.getRight()))
      .filter(Objects::nonNull) // filter out cache misses === empty oshdb cells
      .map(oshEntityCell -> {
        if (this.canceled) return identitySupplier.get();
        // iterate over the history of all OSM objects in the current cell
        List<R> rs = new ArrayList<>();
        CellIterator.iterateByTimestamps(
            oshEntityCell,
            bbox,
            poly,
            tstamps,
            tagInterpreter,
            preFilter,
            filter,
            false
        ).forEach(result -> result.forEach((timestamp, entityGeometry) -> {
          OSHDBTimestamp tstamp = new OSHDBTimestamp(timestamp);
          Geometry geometry = entityGeometry.getRight();
          OSMEntity entity = entityGeometry.getLeft();
          OSMEntitySnapshot foo = new OSMEntitySnapshot(tstamp, geometry, entity);
          R bar = mapper.apply(foo);
          rs.add(bar);
        }));
        S accInternal = identitySupplier.get();
        // fold the results
        for (R r : rs) {
          accInternal = accumulator.apply(accInternal, r);
        }
        return accInternal;
      }).reduce(identitySupplier.get(), combiner);
    }
  }

  private static class FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P extends Geometry & Polygonal> extends MapReduceCellsOnIgniteCacheComputeJob<List<OSMEntitySnapshot>, R, List<R>, S, P> {
    FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob(TagInterpreter tagInterpreter, List<String> cacheNames, List<Long> tstamps, BoundingBox bbox, P poly, SerializablePredicate<OSHEntity> preFilter, SerializablePredicate<OSMEntity> filter, SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper, SerializableSupplier<S> identitySupplier, SerializableBiFunction<S, R, S> accumulator, SerializableBinaryOperator<S> combiner) {
      super(tagInterpreter, cacheNames, tstamps, bbox, poly, preFilter, filter, mapper, identitySupplier, accumulator, combiner);
    }

    @Override
    public S execute(Ignite node) {
      return this.localKeys(node).parallelStream()
      .map(cacheKey -> cacheKey.getLeft().localPeek(cacheKey.getRight()))
      .filter(Objects::nonNull) // filter out cache misses === empty oshdb cells
      .map(oshEntityCell -> {
        if (this.canceled) return identitySupplier.get();
        // iterate over the history of all OSM objects in the current cell
        List<R> rs = new ArrayList<>();
        CellIterator.iterateByTimestamps(
            oshEntityCell,
            bbox,
            poly,
            tstamps,
            tagInterpreter,
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
        S accInternal = identitySupplier.get();
        // fold the results
        for (R r : rs) {
          accInternal = accumulator.apply(accInternal, r);
        }
        return accInternal;
      }).reduce(identitySupplier.get(), combiner);
    }
  }

  private static <V, R, MR, S, P extends Geometry & Polygonal> S _mapReduceOnIgniteCache(
      OSHDB_Ignite oshdb,
      SerializableSupplier<S> identitySupplier,
      SerializableBinaryOperator<S> combiner,
      MapReduceCellsOnIgniteCacheComputeJob<V, R, MR, S, P> computeJob
  ) {
    // execute compute job on all ignite nodes and further reduce+return result(s)
    Ignite ignite = oshdb.getIgnite();
    IgniteCompute compute = ignite.compute();

    ComputeTaskFuture<S> result = compute.executeAsync(new CancelableBroadcastTask<Object, S>(computeJob, identitySupplier, combiner), null);

    if (!oshdb.timeoutInMilliseconds().isPresent()) {
      return result.get();
    } else {
      try {
        return result.get(oshdb.timeoutInMilliseconds().getAsLong());
      } catch (IgniteFutureTimeoutException e) {
        result.cancel();
        throw new OSHDBTimeoutException();
      }
    }
  }

  static <R, S, P extends Geometry & Polygonal> S _mapReduceCellsOSMContributionOnIgniteCache(
      OSHDB_Ignite oshdb,
      List<String> cacheNames,
      TagInterpreter tagInterpreter,
      List<Long> tstamps,
      BoundingBox bbox,
      P poly,
      SerializablePredicate<OSHEntity> preFilter,
      SerializablePredicate<OSMEntity> filter,
      SerializableFunction<OSMContribution, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) {
    return _mapReduceOnIgniteCache(
        oshdb,
        identitySupplier,
        combiner,
        new MapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(
            tagInterpreter,
            cacheNames,
            tstamps,
            bbox,
            poly,
            preFilter,
            filter,
            mapper,
            identitySupplier,
            accumulator,
            combiner
        )
    );
  }

  static <R, S, P extends Geometry & Polygonal> S _flatMapReduceCellsOSMContributionGroupedByIdOnIgniteCache(
      OSHDB_Ignite oshdb,
      List<String> cacheNames,
      TagInterpreter tagInterpreter,
      List<Long> tstamps,
      BoundingBox bbox,
      P poly,
      SerializablePredicate<OSHEntity> preFilter,
      SerializablePredicate<OSMEntity> filter,
      SerializableFunction<List<OSMContribution>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) {
    return _mapReduceOnIgniteCache(
        oshdb,
        identitySupplier,
        combiner,
        new FlatMapReduceCellsOSMContributionOnIgniteCacheComputeJob<R, S, P>(
            tagInterpreter,
            cacheNames,
            tstamps,
            bbox,
            poly,
            preFilter,
            filter,
            mapper,
            identitySupplier,
            accumulator,
            combiner
        )
    );
  }

  static <R, S, P extends Geometry & Polygonal> S _mapReduceCellsOSMEntitySnapshotOnIgniteCache(
      OSHDB_Ignite oshdb,
      List<String> cacheNames,
      TagInterpreter tagInterpreter,
      List<Long> tstamps,
      BoundingBox bbox,
      P poly,
      SerializablePredicate<OSHEntity> preFilter,
      SerializablePredicate<OSMEntity> filter,
      SerializableFunction<OSMEntitySnapshot, R> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) {
    return _mapReduceOnIgniteCache(
        oshdb,
        identitySupplier,
        combiner,
        new MapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(
            tagInterpreter,
            cacheNames,
            tstamps,
            bbox,
            poly,
            preFilter,
            filter,
            mapper,
            identitySupplier,
            accumulator,
            combiner
        )
    );
  }

  static <R, S, P extends Geometry & Polygonal> S _flatMapReduceCellsOSMEntitySnapshotGroupedByIdOnIgniteCache(
      OSHDB_Ignite oshdb,
      List<String> cacheNames,
      TagInterpreter tagInterpreter,
      List<Long> tstamps, BoundingBox bbox,
      P poly,
      SerializablePredicate<OSHEntity> preFilter,
      SerializablePredicate<OSMEntity> filter,
      SerializableFunction<List<OSMEntitySnapshot>, List<R>> mapper,
      SerializableSupplier<S> identitySupplier,
      SerializableBiFunction<S, R, S> accumulator,
      SerializableBinaryOperator<S> combiner
  ) {
    return _mapReduceOnIgniteCache(
        oshdb,
        identitySupplier,
        combiner,
        new FlatMapReduceCellsOSMEntitySnapshotOnIgniteCacheComputeJob<R, S, P>(
            tagInterpreter,
            cacheNames,
            tstamps,
            bbox,
            poly,
            preFilter,
            filter,
            mapper,
            identitySupplier,
            accumulator,
            combiner
        )
    );
  }
}