# Multiple Maps and Result Filters

The previous two steps, [maps](map.md) and [result filters](result-filters.md),
may be applied multiple times. That is, after mapping and filtering you can
apply another map and another result filter and so on.

```
public static void main (...) throws [...] {

  [...]

  // -- REPEAT --
  MapAggregator<OSHDBTimestamp, Integer> mapAggregatorTwoResultFilter = mapAggregatorWithMap.map(new MapperII());
  mapAggregatorTwoResultFilter.filter(new ResultFilterII());
}

private static class MapperII implements SerializableFunction<Double, Integer> {
  @Override
  public Integer apply(Double t) {
    return t.intValue();
  }
}

private static class ResultFilterII implements SerializablePredicate<Integer> {
  @Override
  public boolean test(Integer t) {
    return (t % 2) == 0;
  }
}
```

The next step is to [reduce](reduce.md) the results obtained so far.