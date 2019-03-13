# Reduce

The reduce phase is the second of the two main phases of the MapReduce
programming model. It permits one to aggregate the results of the map
phase across the whole compute cluster. In the oshdb-API, specifying
a reduce function also triggers the whole computation.
The oshdb-API provides a number of standard aggregation functions.

## Average

Calculate the average of the given data.

```
// -- REDUCE --
// average
SortedMap<OSHDBTimestamp, Double> average = mapAggregatorTwoResultFilter.average();
```

## Sum

Calculate the sum of the given data.

```
//sum
SortedMap<OSHDBTimestamp,Number> sum = mapAggregatorWithMap.sum();
```

## Count

Count the number of given data items.
```
//count
SortedMap<OSHDBTimestamp,Integer> count = mapAggregatorWithMap.count();
```

## Unique

Return a set of unique values.
```
//unique
SortedMap<OSHDBTimestamp, Set<Double>> uniq = mapAggregatorWithMap.uniq();
```

## Custom Reduce

You may also implement a custom reduce function. In this tutorial we
illustrate how to do this, by re-implementing the sum function.

```
public static void main(...) throws [...] {

  [...]

  //traditional reduce
  SortedMap<OSHDBTimestamp, Integer> result = mapAggregatorTwoResultFilter.reduce(new IdentitySupplier(), new Accumulator(), new Combiner());
}

private static class IdentitySupplier implements SerializableSupplier<Integer> {
  @Override
  public Integer get() {
    return 0;
  }
}

private static class Accumulator implements SerializableBiFunction<Integer, Integer, Integer> {
  @Override
  public Integer apply(Integer t, Integer u) {
    return t + u;
  }
}

private static class Combiner implements SerializableBinaryOperator<Integer> {
  @Override
  public Integer apply(Integer t, Integer u) {
    return t + u;
  }
}
```

## Summary

In this step we explained how to specify a reduce function.

Finally you probably wish to [output your results](result-handling.md).
