package org.heigit.ohsome.oshdb.filter;

import java.util.function.Supplier;
import org.heigit.ohsome.oshdb.osh.OSHEntity;
import org.heigit.ohsome.oshdb.osm.OSMEntity;
import org.heigit.ohsome.oshdb.util.mappable.OSMContribution;
import org.heigit.ohsome.oshdb.util.mappable.OSMEntitySnapshot;
import org.locationtech.jts.geom.Geometry;

/**
 * A boolean "and" of two sub-expressions.
 */
public class AndOperator extends BinaryOperator {
  AndOperator(FilterExpression op1, FilterExpression op2) {
    super(op1, op2);
  }

  @Override
  public boolean applyOSM(OSMEntity entity) {
    return op1.applyOSM(entity) && op2.applyOSM(entity);
  }

  @Override
  public boolean applyOSH(OSHEntity entity) {
    return op1.applyOSH(entity) && op2.applyOSH(entity);
  }

  @Override
  public boolean applyOSMGeometry(OSMEntity entity, Supplier<Geometry> geometrySupplier) {
    return op1.applyOSMGeometry(entity, geometrySupplier)
        && op2.applyOSMGeometry(entity, geometrySupplier);
  }

  @Override
  public boolean applyOSMEntitySnapshot(OSMEntitySnapshot snapshot) {
    return op1.applyOSMEntitySnapshot(snapshot) && op2.applyOSMEntitySnapshot(snapshot);
  }

  @Override
  public boolean applyOSMContribution(OSMContribution contribution) {
    return op1.applyOSMContribution(contribution) && op2.applyOSMContribution(contribution);
  }

  @Override
  public FilterExpression negate() {
    return new OrOperator(op1.negate(), op2.negate());
  }

  @Override
  public String toString() {
    String op1String = op1 instanceof OrOperator ? "(" + op1.toString() + ")" : op1.toString();
    String op2String = op2 instanceof OrOperator ? "(" + op2.toString() + ")" : op2.toString();
    return op1String + " and " + op2String;
  }
}
