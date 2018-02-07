package org.heigit.bigspatialdata.oshdb.tool.importer.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

public class ExternalSort<T> {
  
  @FunctionalInterface
  public interface Serialize<T> {
    public void write(T obj, ObjectOutputStream out) throws IOException;
  }
  @FunctionalInterface
  public interface Deserialize<T> {
    public T read(ObjectInputStream in) throws IOException;
  }
  
  private final Comparator<T> cmp;
  private final long maxSize;
  
  private final ToLongFunction<T> estimator;
  
  private Serialize<T>  serialize = (it, out) -> {
      out.writeObject(it);
  };
  @SuppressWarnings("unchecked")
  private Deserialize<T> deserialize = in -> {
    try {
      return (T) in.readObject();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e.getMessage());
    }

  };
  private Function<OutputStream, OutputStream> output = Function.identity();
  private Function<InputStream, InputStream> input = Function.identity();

  private boolean parallel;
  private File tmpDirectory;
    
  
  
  
  public static <T> ExternalSort<T> of(Comparator<T> cmp, long maxSize, ToLongFunction<T> estimator){
    ExternalSort<T> sorter = new ExternalSort<T>(cmp,maxSize,estimator);
    return sorter;
  }
  
  private ExternalSort(Comparator<T> cmp, long maxSize, ToLongFunction<T> estimator){
    this.cmp = cmp;
    this.maxSize = maxSize;
    this.estimator = estimator;
  }
  
  public ExternalSort<T> with(Serialize<T>  serialize,Deserialize<T> deserialize){
    this.serialize = serialize;
    this.deserialize = deserialize;
    return this;
  }
  
  public ExternalSort<T> with(Function<OutputStream, OutputStream> output, Function<InputStream, InputStream> input){
    this.output = output;
    this.input = input;
    return this;
  }
  
  public ExternalSort<T> withTempDirectory(File dir){
    this.tmpDirectory = dir;
    this.tmpDirectory.mkdirs();
    return this;
  }
 
  
  public Iterator<T> sort(Iterator<T> source) throws IOException{
    return sort(source,true);
  }
  public Iterator<T> sort(Iterator<T> source, boolean parallel) throws IOException{
    this.parallel = parallel;
    return sortInBatch(source);
  }
  
  

  private Iterator<T> sortInBatch(Iterator<T> source) throws IOException {
    List<T> batch = new ArrayList<>();
    List<File> batches = null;

    long currentSize = 0;
    while (source.hasNext()) {
      T next = source.next();
      if (currentSize > maxSize) {
        File tmpFile = saveBatch(sortBatch(batch));
        if (batches == null)
          batches = new ArrayList<>();
        batches.add(tmpFile);
        currentSize = 0;
        batch.clear();
      }
      batch.add(next);
      currentSize += estimator.applyAsLong(next);
    }
    List<Iterator<T>> merge = new ArrayList<>(((batches != null)?batches.size():0) + 1);
    if(batches != null)
      for (File file : batches) {
        InputStream in = input.apply(new BufferedInputStream(new FileInputStream(file)));
        merge.add(new BatchFileIterator<T>(in, deserialize));
      }
    if (!batch.isEmpty())
      merge.add(sortBatch(batch).iterator());

    return MergeIterator.of(merge, cmp, l -> l.get(0));
  }

  private List<T> sortBatch(List<T> batch) {
    if (parallel) {
      batch = batch.parallelStream().sorted(cmp).collect(Collectors.toCollection(ArrayList<T>::new));
    } else {
      batch.sort(cmp);
    }
    return batch;
  }

  private File saveBatch(List<T> batch) throws IOException {
    File newTmpFile = File.createTempFile("sortInBatch", "flatfile", tmpDirectory);
    newTmpFile.deleteOnExit();
    try (ObjectOutputStream out = new ObjectOutputStream(output.apply(new BufferedOutputStream(new FileOutputStream(newTmpFile))))) {
      for (T item : batch) {
        serialize.write(item, out);
      }
      return newTmpFile;
    }
  }

  private static class BatchFileIterator<T> implements Iterator<T> {
    private final ObjectInputStream input;
    private final Deserialize<T> deserialize;
    private boolean closed = false;
    private T next = null;

    public BatchFileIterator(final InputStream in, Deserialize<T> deserialize) throws IOException {
      this.input = new ObjectInputStream(in);
      this.deserialize = deserialize;
      next = getNext();
    }

    @Override
    public boolean hasNext() {
      return !closed;
    }

    @Override
    public T next() {
      if (!hasNext())
        throw new NoSuchElementException();
      T ret = next;
      next = getNext();
      return ret;
    }

    private T getNext() {
      try {
        return deserialize.read(input);
      } catch (IOException e) {
        if (!(e instanceof EOFException))
          e.printStackTrace();
        try {
          input.close();
        } catch (IOException e1) {
        }

        closed = true;
      }
      return null;
    }

  }
}
