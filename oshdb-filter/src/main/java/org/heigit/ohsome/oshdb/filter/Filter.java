package org.heigit.ohsome.oshdb.filter;

import org.heigit.ohsome.oshdb.filter.NegatableFilter.FilterInternal;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.function.OSHEntityFilter;
import org.heigit.ohsome.oshdb.util.function.OSMEntityFilter;

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
    return new NegatableFilter(new FilterInternal() {
      @Override
      public boolean applyOSM(OSMEntity entity) {
        return true;
      }

      @Override
      public boolean applyOSH(OSHEntity entity) {
        return oshCallback.test(entity);
      }

      @Override
      boolean applyOSHNegated(OSHEntity entity) {
        return !oshCallback.test(entity);
      }
    });
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
    return new NegatableFilter(new FilterInternal() {
      @Override
      public boolean applyOSM(OSMEntity entity) {
        return osmCallback.test(entity);
      }

      @Override
      boolean applyOSMNegated(OSMEntity entity) {
        return !osmCallback.test(entity);
      }
    });
  }

}
