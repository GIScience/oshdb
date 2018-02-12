package org.heigit.bigspatialdata.oshdb.util.celliterator;

import java.util.EnumSet;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class LazyEvaluatedContributionTypes implements Supplier<EnumSet<ContributionType>> {
  private EnumSet<ContributionType> values = EnumSet.noneOf(ContributionType.class);
  private EnumSet<ContributionType> evaluated = EnumSet.noneOf(ContributionType.class);
  private Predicate<ContributionType> evaluator;

  public LazyEvaluatedContributionTypes(Predicate<ContributionType> evaluator) {
    this.evaluator = evaluator;
  }

  public LazyEvaluatedContributionTypes(EnumSet<ContributionType> values) {
    this.values = values;
    this.evaluated = EnumSet.allOf(ContributionType.class);
  }

  public boolean contains(ContributionType t) {
    if (!this.evaluated.contains(t)) {
      boolean value = this.evaluator.test(t);
      this.evaluated.add(t);
      if (value) {
        this.values.add(t);
      }
      return value;
    }
    return this.values.contains(t);
  }

  @Override
  public EnumSet<ContributionType> get() {
    EnumSet<ContributionType> unevaluated = EnumSet.allOf(ContributionType.class);
    unevaluated.removeAll(this.evaluated);
    for (ContributionType contributionType : unevaluated) {
      this.contains(contributionType);
    }
    return this.values;
  }
}
