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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.Role;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.MergeIterator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;

import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;

public class RoleCollector implements Iterable<Role> {

  private final List<File> tmpFiles;
  private Function<OutputStream, OutputStream> outputStreamFunction = (out) -> out;
  private Function<InputStream, InputStream> inputStreamFunction = (in) -> in;

  private final Object2IntAVLTreeMap<String> role2Frequency = new Object2IntAVLTreeMap<>();

  private int countRoles = 0;
  private long estimatedSize = 0;

  public static final String tempPrefix = "temp_rolefrequency_";
  private String tempSuffix = "_00.tmp";
  private File tempDir = null;
  private boolean tempDeleteOnExit = true;

  public RoleCollector() {
    this(new ArrayList<>());
  }

  public void setWorkerId(int workerId) {
    tempSuffix = String.format("_%02d.tmp", workerId);
  }

  public RoleCollector(List<File> tmpFiles) {
    this.tmpFiles = tmpFiles;
  }

  public void setTempDir(File dir) {
    dir.mkdirs();
    if (dir.exists())
      this.tempDir = dir;
  }

  public void setTempDeleteOneExit(boolean deleteOnExit) {
    this.tempDeleteOnExit = deleteOnExit;
  }

  public long getEstimatedSize() {
    return estimatedSize;
  }

  public void inputOutputStream(Function<InputStream, InputStream> input, Function<OutputStream, OutputStream> output) {
    this.inputStreamFunction = input;
    this.outputStreamFunction = output;
  }

  public void addAll(Collection<String> roles) {
    roles.forEach(role -> {
      if (role2Frequency.addTo(role, 1) == 0) {
        countRoles++;
        estimatedSize += SizeEstimator.estimatedSizeOfAVLEntryValue(role);
      }
    });
  }

  public void writeTemp(OutputStream outStream) throws FileNotFoundException, IOException {
    try (OutputStream outStream2 = outputStreamFunction.apply(outStream);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream2))) {
      out.writeInt(role2Frequency.size());
      role2Frequency.object2IntEntrySet().forEach(throwingConsumer(roleFrequency -> {
        out.writeUTF(roleFrequency.getKey());
        out.writeInt(roleFrequency.getIntValue());
      }));

      role2Frequency.clear();
      estimatedSize = 0;
    }
  }

  public void writeTemp() throws IOException {
    if (role2Frequency.isEmpty())
      return;
    File newTempFile;
    newTempFile = File.createTempFile(tempPrefix, tempSuffix, tempDir);
    if (tempDeleteOnExit)
      newTempFile.deleteOnExit();
    writeTemp(new FileOutputStream(newTempFile));
    tmpFiles.add(newTempFile);
  }

  private static final Comparator<Role> comparator = (a, b) -> a.role.compareTo(b.role);
  private static final Function<List<Role>, Role> combiner = (list) -> {
    final String k = list.get(0).role;
    int freq = 0;
    for (Role role : list) {
      freq += role.freq;
    }
    return new Role(k, freq);
  };

  public static class RoleFileReader implements Iterator<Role> {
    private final DataInputStream input;
    private final int roles;
    private int index = 0;

    public static RoleFileReader of(DataInputStream input) throws IOException {
      final int roles = input.readInt();
      return new RoleFileReader(input, roles);
    }

    private RoleFileReader(DataInputStream input, int roles) {
      this.input = input;
      this.roles = roles;
    }

    @Override
    public boolean hasNext() {
      return index < roles;
    }

    @Override
    public Role next() {
      if (!hasNext())
        throw new NoSuchElementException();

      try {
        final String role = input.readUTF();
        final int freq = input.readInt();
        index++;

        return new Role(role, freq);

      } catch (IOException e) {
        index = Integer.MAX_VALUE;
        e.printStackTrace();
        throw new NoSuchElementException(e.getMessage());
      }
    }
  }

  public static class RoleMapReader implements Iterator<Role> {

    private final ObjectBidirectionalIterator<Entry<String>> roleIterator;
    private final boolean remove;

    public static RoleMapReader of(Object2IntAVLTreeMap<String> role2Frequency, boolean remove) {
      final ObjectBidirectionalIterator<Entry<String>> roleIterator = role2Frequency.object2IntEntrySet().iterator();
      return new RoleMapReader(roleIterator, remove);
    }

    private RoleMapReader(ObjectBidirectionalIterator<Entry<String>> roleIterator, boolean remove) {
      this.roleIterator = roleIterator;
      this.remove = remove;
    }

    @Override
    public boolean hasNext() {
      return roleIterator.hasNext();
    }

    @Override
    public Role next() {
      if (!hasNext())
        throw new NoSuchElementException();

      final Entry<String> entry = roleIterator.next();
      final String role = entry.getKey();
      final int freq = entry.getIntValue();

      return new Role(role, freq);
    }
  }

  public Iterator<Role> iterator() {
    List<Iterator<Role>> iters = new ArrayList<>(tmpFiles.size() + role2Frequency.size());
    tmpFiles.stream().map(file -> {
      try {
        InputStream input = inputStreamFunction.apply(new FileInputStream(file));
        DataInputStream dataInput = new DataInputStream(new BufferedInputStream(input));
        return RoleFileReader.of(dataInput);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e.getMessage());
      }
    }).forEach(iters::add);
    if (role2Frequency.size() > 0) {
      iters.add(RoleMapReader.of(role2Frequency, true));
    }

    return MergeIterator.of(iters, (a, b) -> a.role.compareTo(b.role), list -> {

      final String role = list.get(0).role;
      final int freq = list.stream().mapToInt(it -> {
        return it.freq;
      }).sum();

      return new Role(role, freq);
    });

  }


  protected Role read(DataInputStream in) throws IOException {
    final String key = in.readUTF();
    final int frequency = in.readInt();
    return new Role(key, frequency);
  }

  protected void write(DataOutputStream out, Role role) throws IOException {
    out.writeUTF(role.role);
    out.writeInt(role.freq);
  }
}
