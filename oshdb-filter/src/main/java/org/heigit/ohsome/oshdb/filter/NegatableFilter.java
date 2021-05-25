package org.heigit.ohsome.oshdb.filter;

import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.locationtech.jts.geom.Geometry;

/**
 * A filter which implements the negate method using a boolean flag.
 */
class NegatableFilter implements FilterExpression {

  private final boolean negated;
  final FilterExpression filter;

  private NegatableFilter(@Nonnull FilterExpression filter, boolean negated) {
    this.filter = filter;
    this.negated = negated;
  }

  protected NegatableFilter(@Nonnull FilterExpression filter) {
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
  public boolean applyOSMEntitySnapshot(OSMEntitySnapshot snapshot) {
    return this.filter.applyOSMEntitySnapshot(snapshot) ^ this.negated;
  }

  @Override
  public boolean applyOSMContribution(OSMContribution contribution) {
    return this.filter.applyOSMContribution(contribution) ^ this.negated;
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