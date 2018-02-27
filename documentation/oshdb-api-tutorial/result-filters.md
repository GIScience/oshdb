# Result Filters

The scope of interest may also be restricted based on the
results of the previous mapping step by specifying result filters.
In particular, using result filters also reduces the amount of
data transfer within the compute cluster. In this example we
filter for ways that have a length of more than 100m.

```
public static void main(...) throws [...] {

  [...]

  // -- RESULT FILTER --
  mapAggregatorWithMap = mapAggregatorWithMap.filter(new ResultFilter());

}

private static class ResultFilter implements SerializablePredicate<Double> {
  public boolean test(Double t) {
    return t > 100.0;
  }
}
```

In the next we describe how to use
[multiple mappings and result filters](multiple-maps-and-filters.md).
