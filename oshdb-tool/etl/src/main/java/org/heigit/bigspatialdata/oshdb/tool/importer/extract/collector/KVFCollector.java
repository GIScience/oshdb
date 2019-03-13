package org.heigit.bigspatialdata.oshdb.tool.importer.extract.collector;


import static org.heigit.bigspatialdata.oshdb.tool.importer.util.lambda.ConsumerUtil.throwingConsumer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.KVF;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.VF;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.MergeIterator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.TagText;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Streams;

import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;

public class KVFCollector implements Iterable<KVF> {

  private Function<OutputStream, OutputStream> outputStreamFunction = (out) -> out;
  private Function<InputStream, InputStream> inputStreamFunction = (in) -> in;

  private final Object2IntAVLTreeMap<String> key2Frequency = new Object2IntAVLTreeMap<>();
  private final Object2ObjectAVLTreeMap<String, Object2IntAVLTreeMap<String>> key2Values = new Object2ObjectAVLTreeMap<>();

  private long estimatedSize = 0;
  private final List<File> splits;

  public static final  String tempPrefix = "temp_keyvaluefrequency_";
  private String tempSuffix = "_00.tmp"; 
  private File tempDir = null;
  private boolean tempDeleteOnExit = true;

  public KVFCollector() {
    this(new ArrayList<>());
  }

  public KVFCollector(List<File> files) {
    this.splits = files;
  }

  public void setWorkerId(int workerId) {
    tempSuffix = String.format("_%02d.tmp", workerId);
  }
  
  public void setTempDir(File dir) {
    dir.mkdirs();
    if (dir.exists())
      this.tempDir = dir;
  }

  public void setTempDeleteOneExit(boolean deleteOnExit) {
    this.tempDeleteOnExit = deleteOnExit;
  }

  public void addAll(Collection<TagText> item) {
    item.forEach(t -> {
      final String key = t.key;
      final String value = t.value;

      if (key2Frequency.addTo(key, 1) == 0) {
        estimatedSize += SizeEstimator.estimatedSizeOfAVLEntryKey(key);
      }

      Object2IntAVLTreeMap<String> value2Frequency = key2Values.get(key);
      if (value2Frequency == null) {
        value2Frequency = new Object2IntAVLTreeMap<>();
        key2Values.put(key, value2Frequency);
      }
      if (value2Frequency.addTo(value, 1) == 0) {
        estimatedSize += SizeEstimator.estimatedSizeOfAVLEntryValue(value);
      }
    });
  }

  public void inputOutputStream(Function<InputStream, InputStream> input, Function<OutputStream, OutputStream> output) {
    this.inputStreamFunction = input;
    this.outputStreamFunction = output;
  }

  public void writeTemp() throws IOException {
    if (key2Frequency.isEmpty())
      return;
    File newTempFile;
    newTempFile = File.createTempFile(tempPrefix, tempSuffix, tempDir);
    if (tempDeleteOnExit)
      newTempFile.deleteOnExit();
    writeTemp(new FileOutputStream(newTempFile));
    splits.add(newTempFile);
  }

  public void writeTemp(OutputStream outStream) throws FileNotFoundException, IOException {
    try (OutputStream outStream2 = outputStreamFunction.apply(outStream);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream2))) {
      out.writeInt(key2Frequency.size());
      key2Frequency.object2IntEntrySet().forEach(throwingConsumer(keyFrequency -> {
        final String key = keyFrequency.getKey();
        final int keyFreq = keyFrequency.getIntValue();
        writeKVFrequency(out, key, keyFreq, key2Values.get(key));
      }));
      key2Frequency.clear();
      key2Values.clear();
      estimatedSize = 0;
    }
  }

  private static void writeKVFrequency(DataOutputStream out, String key, int keyFreq,
      Object2IntAVLTreeMap<String> value) throws IOException {
    out.writeUTF(key);
    out.writeInt(keyFreq);
    out.writeInt(value.size());
    value.object2IntEntrySet().forEach(throwingConsumer(valueFrequency -> {
      out.writeUTF(valueFrequency.getKey());
      out.writeInt(valueFrequency.getIntValue());
    }));
  }

  public long getEstimatedSize() {
    return estimatedSize;
  }

  public List<File> getSplits() {
    return splits;
  }

  @Override
  public Iterator<KVF> iterator() {
    List<Iterator<KVF>> iters = new ArrayList<>(splits.size() + key2Frequency.size());
    splits.stream().map(file -> {
      try {
        InputStream input = inputStreamFunction.apply(new FileInputStream(file));
        DataInputStream dataInput = new DataInputStream(new BufferedInputStream(input));
        return KVFFileReader.of(dataInput);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e.getMessage());
      }
    }).forEach(iters::add);
    if (key2Frequency.size() > 0) {
      iters.add(KVFMapReader.of(key2Frequency, key2Values, true));
    }

    return MergeIterator.of(iters, (a, b) -> a.key.compareTo(b.key), list -> {
      final List<Iterator<VF>> values = new ArrayList<>(list.size());

      final String key = list.get(0).key;
      final int freq = list.stream().mapToInt(it -> {
        values.add(it.vfIterator);
        return it.freq;
      }).sum();

      Iterator<VF> valueItr = MergeIterator.of(values, 
          (a, b) -> a.value.compareTo(b.value), 
          valueList -> {
            final String value = valueList.get(0).value;
            final int valueFreq = valueList.stream().mapToInt(VF::freq).sum();
            return new VF(value, valueFreq);});
      return new KVF(key, freq, valueItr);
    });
  }
  
  public Stream<KVF> stream(){
    return Streams.stream(this);
  }

  public static class VFIterator implements Iterator<VF> {
    private final PriorityQueue<PeekingIterator<VF>> queue;
    private final List<PeekingIterator<VF>> peekingIters;
    private final Comparator<VF> comparator = (a, b) -> a.value.compareTo(b.value);

    public static VFIterator of(List<Iterator<VF>> iters) {
      final List<PeekingIterator<VF>> peekingIters = iters.stream().map(itr -> Iterators.peekingIterator(itr))
          .collect(Collectors.toList());
      return new VFIterator(peekingIters);
    }

    private VFIterator(List<PeekingIterator<VF>> peekingIters) {
      this.peekingIters = peekingIters;
      queue = new PriorityQueue<>(peekingIters.size(), (a, b) -> comparator.compare(a.peek(), b.peek()));
    }

    @Override
    public boolean hasNext() {
      return !queue.isEmpty() || !peekingIters.isEmpty();
    }

    @Override
    public VF next() {
      if (!hasNext())
        throw new NoSuchElementException();

      queue.addAll(peekingIters);
      peekingIters.clear();

      VF vf = poll();

      final String value = vf.value;
      int freq = vf.freq;

      while (!queue.isEmpty() && comparator.compare(vf, queue.peek().peek()) == 0) {
        vf = poll();
        freq += vf.freq;
      }

      return new VF(value, freq);
    }

    private VF poll() {
      final PeekingIterator<VF> iter = queue.poll();
      final VF ret = iter.next();
      if (iter.hasNext())
        peekingIters.add(iter);
      return ret;
    }

  }

  public static class KVFMapReader implements Iterator<KVF> {

    private final ObjectBidirectionalIterator<Entry<String>> keyIterator;
    private final Object2ObjectAVLTreeMap<String, Object2IntAVLTreeMap<String>> key2Values;
    private final boolean remove;
    private Map<String, Integer> lastValues = Collections.emptyMap();

    public static KVFMapReader of(Object2IntAVLTreeMap<String> key2Frequency,
        Object2ObjectAVLTreeMap<String, Object2IntAVLTreeMap<String>> key2Values, boolean remove) {
      final ObjectBidirectionalIterator<Entry<String>> keyIterator = key2Frequency.object2IntEntrySet().iterator();
      return new KVFMapReader(keyIterator, key2Values, remove);
    }

    private KVFMapReader(ObjectBidirectionalIterator<Entry<String>> keyIterator,
        Object2ObjectAVLTreeMap<String, Object2IntAVLTreeMap<String>> key2Values, boolean remove) {
      this.keyIterator = keyIterator;
      this.key2Values = key2Values;
      this.remove = remove;
    }

    @Override
    public boolean hasNext() {
      return keyIterator.hasNext();
    }

    @Override
    public KVF next() {
      if (!hasNext())
        throw new NoSuchElementException();

      final Entry<String> entry = keyIterator.next();
      final String key = entry.getKey();
      final int freq = entry.getIntValue();

      Object2IntAVLTreeMap<String> values = key2Values.get(key);
      if (remove) {
        lastValues.clear();
        keyIterator.remove();
        key2Values.remove(key);
        lastValues = values;
      }

      return new KVF(key, freq, VFMapReader.of(values, remove));
    }

  }

  public static class VFMapReader implements Iterator<VF> {
    final ObjectBidirectionalIterator<Entry<String>> valueIterator;
    final boolean remove;

    public static VFMapReader of(Object2IntAVLTreeMap<String> values, boolean remove) {
      ObjectBidirectionalIterator<Entry<String>> valueIterator = values.object2IntEntrySet().iterator();
      return new VFMapReader(valueIterator, remove);
    }

    private VFMapReader(ObjectBidirectionalIterator<Entry<String>> valueIterator, boolean remove) {
      this.valueIterator = valueIterator;
      this.remove = remove;
    }

    @Override
    public boolean hasNext() {
      return valueIterator.hasNext();
    }

    @Override
    public VF next() {
      final Entry<String> entry = valueIterator.next();
      final String value = entry.getKey();
      final int freq = entry.getIntValue();
      if (remove) {
        valueIterator.remove();
      }
      return new VF(value, freq);
    }

  }

  public static class KVFFileReader implements Iterator<KVF> {
    private final DataInputStream input;
    private final int keys;
    private int index = 0;
    private Iterator<VF> values = Collections.emptyIterator();

    public static KVFFileReader of(DataInputStream input) throws IOException {
      final int keys = input.readInt();
      return new KVFFileReader(input, keys);
    }

    private KVFFileReader(DataInputStream input, int keys) {
      this.input = input;
      this.keys = keys;
    }

    @Override
    public boolean hasNext() {
      return index < keys;
    }

    @Override
    public KVF next() {
      if (!hasNext())
        throw new NoSuchElementException();

      try {
        // skip all unread values
        while (values.hasNext())
          values.next();

        final String key = input.readUTF();
        final int freq = input.readInt();
        values = VFFileReader.of(input);

        index++;

        return new KVF(key, freq, values);

      } catch (IOException e) {
        index = Integer.MAX_VALUE;
        e.printStackTrace();
        throw new NoSuchElementException(e.getMessage());
      }
    }

  }

  public static class VFFileReader implements Iterator<VF> {
    private final DataInputStream input;
    private final int values;
    private int index = 0;

    public static VFFileReader of(DataInputStream input) throws IOException {
      final int values = input.readInt();
      return new VFFileReader(input, values);
    }

    private VFFileReader(DataInputStream input, int values) {
      this.input = input;
      this.values = values;
    }

    @Override
    public boolean hasNext() {
      return index < values;
    }

    @Override
    public VF next() {
      if (!hasNext())
        throw new NoSuchElementException();

      try {
        final String value = input.readUTF();
        final int freq = input.readInt();
        index++;
        return new VF(value, freq);
      } catch (IOException e) {
        index = Integer.MAX_VALUE;
        e.printStackTrace();
        throw new NoSuchElementException(e.getMessage());
      }
    }
  }
}
