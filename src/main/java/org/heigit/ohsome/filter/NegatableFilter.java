package org.heigit.ohsome.filter;

import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.locationtech.jts.geom.Geometry;

/**
 * A filter which implements the negate method using a boolean flag.
 */
class NegatableFilter implements Filter {
  interface Filter extends org.heigit.ohsome.filter.Filter {
    @Override
    default FilterExpression negate() {
      throw new IllegalStateException("Invalid call of inner negate() on a negatable filter");
    }
  }

  private final boolean negated;
  final Filter filter;

  private NegatableFilter(@Nonnull Filter filter, boolean negated) {
    this.filter = filter;
    this.negated = negated;
  }

  protected NegatableFilter(@Nonnull Filter filter) {
    this(filter, false);
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return this.filter.applyOSH(entity) ^ this.negated;
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return this.filter.applyOSM(entity) ^ this.negated;
  }

  @Override
  public boolean applyOSMGeometry(OSMEntity entity, Geometry geometry) {
    return this.filter.applyOSMGeometry(entity, geometry) ^ this.negated;
  }

  @Override
  public NegatableFilter negate() {
    return new NegatableFilter(this.filter, !this.negated);
  }

  @Override
  public String toString() {
    return (this.negated ? "not " : "") + this.filter.toString();
  }
}