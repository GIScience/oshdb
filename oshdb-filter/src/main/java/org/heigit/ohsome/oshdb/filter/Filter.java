package org.heigit.ohsome.oshdb.filter;

import java.util.function.Supplier;
import org.heigit.ohsome.oshdb.filter.NegatableFilter.FilterInternal;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.function.OSHEntityFilter;
import org.heigit.ohsome.oshdb.util.function.OSMEntityFilter;
import org.heigit.ohsome.oshdb.util.function.SerializableBiPredicate;
import org.locationtech.jts.geom.Geometry;

/**
 * A filter condition which can be applied to an OSM entity.
 */
public interface Filter extends FilterExpression {

  /**
   * Constructs a simple filter based on a predicate on OSH entities.
   *
   * <p>The callback is called once for each OSH entity and decides whether it should be kept
   * (by returning true) or discarded (by returning false).</p>
   *
   * <p>Example: `Filter.byOSHEntity(entity -&gt; entity.getId() == 42);`</p>
   *
   * @param oshCallback predicate which tests osh entities
   * @return a filter object which filters using the given predicate
   */
  static Filter byOSHEntity(OSHEntityFilter oshCallback) {
    return by(oshCallback, ignored -> true);
  }

  /**
   * Constructs a simple filter based on a predicate on OSM entities.
   *
   * <p>The callback is called once for each OSM entity and decides whether it should be kept
   * (by returning true) or discarded (by returning false).</p>
   *
   * <p>Example: `Filter.byOSMEntity(entity -&gt; entity.getVersion() == 1);`</p>
   *
   * @param osmCallback predicate which tests osm entities
   * @return a filter object which filters using the given predicate
   */
  static Filter byOSMEntity(OSMEntityFilter osmCallback) {
    return new NegatableFilter(osmCallback::test);
  }

  /**
   * Constructs a simple filter based on two predicates.
   *
   * <p>The callbacks are called for each OSH/OSM entity and decide whether the OSH/OSM object
   * should be kept (by returning true) or discarded (by returning false).</p>
   *
   * <p>Example: `Filter.by(osh -&gt; osh.getId() == 42, osm -&gt; osm.getVersion() == 1);`</p>
   *
   * @param oshCallback predicate which tests osh entities
   * @param osmCallback predicate which tests osm entities
   * @return a filter object which filters using the given predicates
   */
  static Filter by(
      OSHEntityFilter oshCallback,
      OSMEntityFilter osmCallback) {
    return by(oshCallback, osmCallback, (ignored, ignored2) -> true);
  }

  /**
   * Constructs a simple filter based on two predicates and a geometry test.
   *
   * <p>The callbacks are called for each OSM feature and decide whether the feature
   * should be kept (by returning true) or discarded (by returning false).</p>
   *
   * <p>Example: `Filter.by(osh -&gt; osh.getId() == 42, osm -&gt; osm.getVersion() == 1,
   * (osm, geometrySupplier) -&gt; geometrySupplier.get() instanceOf Polygon);`</p>
   *
   * @param oshCallback predicate which tests osh entities
   * @param osmCallback predicate which tests osm entities
   * @param geomCallback predicate which tests osm geometries, alongside the geometry (given as a
   *                     supplier method), the entity itself is given also to be able to perform
   *                     filtering on the whole "OSM Feature" (metadata + tags + geometry).
   * @return a filter object which filters using the given predicates
   */
  static Filter by(
      OSHEntityFilter oshCallback,
      OSMEntityFilter osmCallback,
      SerializableBiPredicate<OSMEntity, Supplier<Geometry>> geomCallback
  ) {
    return new NegatableFilter(new FilterInternal() {
      @Override
      public boolean applyOSM(OSMEntity entity) {
        return osmCallback.test(entity);
      }

      @Override
      public boolean applyOSH(OSHEntity entity) {
        return oshCallback.test(entity);
      }

      @Override
      public boolean applyOSMGeometry(OSMEntity entity, Supplier<Geometry> geometrySupplier) {
        return geomCallback.test(entity, geometrySupplier);
      }
    });
  }
}
