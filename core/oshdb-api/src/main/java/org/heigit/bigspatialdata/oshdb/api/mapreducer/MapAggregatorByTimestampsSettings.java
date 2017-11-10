package org.heigit.bigspatialdata.oshdb.api.mapreducer;

/**
 * Interface defining the common setting methods found on MapAggregatorByTimestamps and MapBiAggregatorByTimestamps objects
 *
 * @param <M> the class returned by all setting methods
 */
interface MapAggregatorByTimestampsSettings<M> {
  /**
   * Enables/Disables the zero-filling feature of otherwise empty timestamp entries in the result.
   *
   * This feature is enabled by default, and can be disabled by calling this function with a value of `false`.
   *
   * @param zerofill the enabled/disabled state of the zero-filling feature
   * @return this mapAggregator object
   */
  M zerofill(boolean zerofill);
}
