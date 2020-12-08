package org.heigit.ohsome.filter;

import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.locationtech.jts.geom.Geometry;

/**
 * A filter which implements the negate method using a boolean flag.
 */
class NegatableFilter implements Filter {
  interface FilterInternal extends Filter {
    @Override
    default FilterExpression negate() {
      throw new IllegalStateException("Invalid call of inner negate() on a negatable filter");
    }
  }

  private final boolean negated;
  final FilterInternal filter;

  private NegatableFilter(@Nonnull FilterInternal filter, boolean negated) {
    this.filter = filter;
    this.negated = negated;
  }

  protected NegatableFilter(@Nonnull FilterInternal filter) {
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
  public boolean applyOSMGeometry(OSMEntity entity, Supplier<Geometry> geometrySupplier) {
    return this.filter.applyOSMGeometry(entity, geometrySupplier) ^ this.negated;
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