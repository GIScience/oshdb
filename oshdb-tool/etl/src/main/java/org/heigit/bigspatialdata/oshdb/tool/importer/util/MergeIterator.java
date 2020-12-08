package org.heigit.bigspatialdata.oshdb.tool.importer.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

public class MergeIterator<T> implements Iterator<T> {
  private final PriorityQueue<PeekingIterator<T>> queue;
  private final List<PeekingIterator<T>> peekingIters;
  private final Comparator<T> comparator;
  private final Function<List<T>, T> merge;

  public static <T> Iterator<T> of(List<Iterator<T>> iters, Comparator<T> comparator, Function<List<T>, T> merge) {
    List<PeekingIterator<T>> peekingIters = iters.stream().map(itr -> Iterators.peekingIterator(itr))
        .collect(Collectors.toList());
    return new MergeIterator<>(peekingIters, comparator, merge);
  }

  private MergeIterator(List<PeekingIterator<T>> peekingIters, Comparator<T> comparator, Function<List<T>, T> merge) {
    this.queue = new PriorityQueue<>(peekingIters.size(), (a, b) -> comparator.compare(a.peek(), b.peek()));
    this.peekingIters = peekingIters;
    this.comparator = comparator;
    this.merge = merge;
  }

  @Override
  public boolean hasNext() {
    return !queue.isEmpty() || !peekingIters.isEmpty();
  }

  @Override
  public T next() {
    if (!hasNext())
      throw new NoSuchElementException();

    queue.addAll(peekingIters);
    peekingIters.clear();

    T t = poll();

    List<T> collect = new ArrayList<>(queue.size());
    collect.add(t);

    while (!queue.isEmpty() && comparator.compare(t, queue.peek().peek()) == 0) {
      collect.add(poll());
    }
    if(collect.size() == 1)
      return collect.get(0);

    return merge.apply(collect);
  }

  private T poll() {
    final PeekingIterator<T> iter = queue.poll();
    final T item = iter.next();
    if (iter.hasNext()) {
      peekingIters.add(iter);
    }
    return item;
  }
}
