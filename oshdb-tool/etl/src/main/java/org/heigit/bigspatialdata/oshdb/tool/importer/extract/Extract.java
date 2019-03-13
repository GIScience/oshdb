package org.heigit.bigspatialdata.oshdb.tool.importer.extract;

import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.cli.ExtractArgs;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.collector.KVFCollector;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.collector.RoleCollector;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.collector.StatsCollector;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.OsmPbfMeta;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.Role;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.VF;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.ExternalSort;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.PolyFileReader;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Relation;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.RelationMember;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.Tag;
import org.heigit.bigspatialdata.oshpbf.parser.osm.v0_6.TagText;
import org.heigit.bigspatialdata.oshpbf.parser.rx.Osh;
import org.heigit.bigspatialdata.oshpbf.parser.rx.RxOshPbfReader;
import org.wololo.geojson.GeoJSON;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Functions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Streams;
import com.google.common.io.CountingOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import io.reactivex.Flowable;

public class Extract {

  public static class ExtractKeyTablesResult {
    public final KVFCollector kvFrequency;
    public final RoleCollector roleFrequency;

    public ExtractKeyTablesResult(KVFCollector kvFrequency, RoleCollector roleFrequency) {
      this.kvFrequency = kvFrequency;
      this.roleFrequency = roleFrequency;
    }
  }

  public static class KeyValuePointer {
    public final String key;
    public final int freq;
    public final int valuesNumber;
    public final long valuesOffset;

    private KeyValuePointer(String key, int freq, int valuesNumber, long valuesOffset) {
      this.key = key;
      this.freq = freq;
      this.valuesNumber = valuesNumber;
      this.valuesOffset = valuesOffset;
    }

    public void write(DataOutput out) throws IOException {
      out.writeUTF(key);
      out.writeInt(freq);
      out.writeInt(valuesNumber);
      out.writeLong(valuesOffset);
    }

    public static KeyValuePointer read(DataInput in) throws IOException {
      final String key = in.readUTF();
      final int freq = in.readInt();
      final int valuesNumber = in.readInt();
      final long valuesOffset = in.readLong();
      return new KeyValuePointer(key, freq, valuesNumber, valuesOffset);
    }

    public long estimateSize() {
      final long size = SizeEstimator.estimatedSizeOf("") + SizeEstimator.estimatedSizeOf(key) // value
          + 4 // freq
          + 4 // valueNumber
          + 8 // valuesOffset
      ;
      return size;
    }
  }

  private final long maxMemory;
  private Path workDirectory = Paths.get(".");
  private Path tempDirectory = Paths.get(".");

  public static OsmPbfMeta pbfMetaData(Path pbf) throws InvalidProtocolBufferException {
    return TypeStartFinder.getMetaData(pbf);
  }

  private Extract(long maxMemory) {
    this.maxMemory = maxMemory;
  }

  public static Extract withMaxMemory(long availableMemory) {
    return new Extract(availableMemory);
  }

  public Extract withWorkDirectory(Path workDirectory) {
    this.workDirectory = workDirectory;
    return this;
  }

  public Extract withTempDirectory(Path tempDirectory) {
    this.tempDirectory = tempDirectory;
    return this;
  }

  public ExtractKeyTablesResult extract(ExtractArgs config, int workerId, int workerTotal, boolean keepTemp) {
    final Path pbf = config.pbf;
    final StatsCollector stats = new StatsCollector(pbf);
    
    final KVFCollector kvFrequency = new KVFCollector();
    kvFrequency.setWorkerId(workerId);
    kvFrequency.setTempDir((workerTotal > 1) ? workDirectory.toFile() : tempDirectory.toFile());
    kvFrequency.setTempDeleteOneExit(workerTotal > 1 || keepTemp);

    final RoleCollector roleFrequency = new RoleCollector();
    roleFrequency.setWorkerId(workerId);
    roleFrequency.setTempDir((workerTotal > 1) ? workDirectory.toFile() : tempDirectory.toFile());
    roleFrequency.setTempDeleteOneExit(workerTotal > 1 || keepTemp);

    final long fileLength = pbf.toFile().length();
    final long workSize = (long) Math.ceil(fileLength / (double) workerTotal);
    final long start = workSize * workerId;
    final long softEnd = Math.min(start + workSize, fileLength);

    Flowable<Osh> oshFlow = RxOshPbfReader.readOsh(pbf, start, softEnd, -1,stats::addHeader);
    
    oshFlow = oshFlow.doOnNext(osh -> {
      stats.add(osh);
    });

    oshFlow = oshFlow.doOnNext(osh -> {
      if (kvFrequency.getEstimatedSize() + roleFrequency.getEstimatedSize() > maxMemory) {
        kvFrequency.writeTemp();
        roleFrequency.writeTemp();
      }

      final Set<TagText> uniqueTags = new HashSet<>();
      osh.getVersions().forEach(version -> {
        for (Tag tag : version.getTags()) {
          uniqueTags.add((TagText) tag);
        }
      });
      kvFrequency.addAll(uniqueTags);
    });

    oshFlow = oshFlow.doOnNext(osh -> {
      if (kvFrequency.getEstimatedSize() + roleFrequency.getEstimatedSize() > maxMemory) {
        kvFrequency.writeTemp();
        roleFrequency.writeTemp();
      }
      if (osh.getType() != OSMType.RELATION)
        return;
      final Set<String> uniqueRoles = new HashSet<>();

      osh.getVersions().forEach(version -> {
        Relation r = (Relation) version;
        for (RelationMember member : r.members) {
          uniqueRoles.add(member.role);
        }
      });
      roleFrequency.addAll(uniqueRoles);
    });

    oshFlow.count().blockingGet();
    
    try(FileOutputStream fos = new FileOutputStream(workDirectory.resolve("extract_meta").toFile());
        PrintStream out = new PrintStream(fos)){
      stats.print(out);
      
      if(!config.md5.trim().isEmpty())
        out.println("file.md5="+config.md5);
      
      if(config.polyFile != null){
        GeoJSON json = PolyFileReader.parse(config.polyFile);
        out.println("extract.region="+json.toString());
      }else if(config.bbox != null){
        out.println("extract.region={\"bbox\":["+config.bbox+"]}");
      }
      
      out.print("extract.timerange="+config.timeValidityFrom);
      if(config.timeValidityTo != null)
        out.println(","+config.timeValidityTo);
      else if(stats.header.hasOsmosisReplicationTimestamp() && stats.header.getOsmosisReplicationTimestamp() > 0){
        out.println(","+ZonedDateTime.ofInstant(Instant.ofEpochSecond(stats.header.getOsmosisReplicationTimestamp()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
      }else {
        out.println(","+ZonedDateTime.ofInstant(Instant.ofEpochSecond(stats.maxTs), ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
      }
      
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    ExtractKeyTablesResult result = new ExtractKeyTablesResult(kvFrequency, roleFrequency);
    return result;
  }

  public void sortByFrequency(ExtractKeyTablesResult extratKeyTablesResult) throws FileNotFoundException, IOException {
    System.out.print("sorting tags by frequency ...");
    sortByFrequency(extratKeyTablesResult.kvFrequency);
    System.out.println(" done!");
    System.out.print("sorting roles by frequency ...");
    sortByFrequency(extratKeyTablesResult.roleFrequency);
    System.out.println(" done!");
  }

  public void sortByFrequency(KVFCollector kvFrequency) throws FileNotFoundException, IOException {
    final long maxSize = maxMemory;
    final ExternalSort<VF> valueSorter = ExternalSort.of((a, b) -> {
      final int c = Integer.compare(a.freq, b.freq);
      if (c != 0)
        return c * -1; // reverse order
      return a.value.compareTo(b.value);
    }, maxSize, VF::estimateSize).with(VF::write, VF::read);

    final Function<OutputStream, OutputStream> output = Functions.identity();
    final List<KeyValuePointer> keys = new ArrayList<>();
    try (
        CountingOutputStream keyValuesPositionOutput = new CountingOutputStream(output.apply(
            new BufferedOutputStream(new FileOutputStream(workDirectory.resolve("extract_keyvalues").toFile()))));
        DataOutputStream keyValuesDataOutput = new DataOutputStream(keyValuesPositionOutput)) {
      kvFrequency.forEach(kvf -> {
        try {
          long offset = keyValuesPositionOutput.getCount();
          int values = (int) Streams.stream(valueSorter.sort(kvf.vfIterator)).peek(vf -> {
            try {
              vf.write(keyValuesDataOutput);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }).count();

          KeyValuePointer kvp = new KeyValuePointer(kvf.key, kvf.freq, values, offset);
          keys.add(kvp);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }

    try (
        OutputStream os = output
            .apply(new BufferedOutputStream(new FileOutputStream(workDirectory.resolve("extract_keys").toFile())));
        DataOutputStream keyValuesDataOutput = new DataOutputStream(os)) {
      keyValuesDataOutput.writeInt(keys.size());
      final int keyCount = (int) Streams.stream(ExternalSort.of((a, b) -> {
        final int c = Integer.compare(a.freq, b.freq);
        if (c != 0)
          return c * -1; // reverse order
        return a.key.compareTo(b.key);
      }, maxSize, KeyValuePointer::estimateSize).with(KeyValuePointer::write, KeyValuePointer::read)
          .sort(keys.iterator())).peek(kvp -> {
            try {
              kvp.write(keyValuesDataOutput);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }).count();
    }
  }

  public void sortByFrequency(RoleCollector roleFrequency) throws FileNotFoundException, IOException {
    final long maxSize = maxMemory;
    final Function<OutputStream, OutputStream> output = Functions.identity();
    try (
        OutputStream os = output
            .apply(new BufferedOutputStream(new FileOutputStream(workDirectory.resolve("extract_roles").toFile())));
        DataOutputStream rolesDataOutput = new DataOutputStream(os)) {
      final int keyCount = (int) Streams.stream(ExternalSort.of((a, b) -> {
        final int c = Integer.compare(a.freq, b.freq);
        if (c != 0)
          return c * -1; // reverse order
        return a.role.compareTo(b.role);
      }, maxSize, Role::estimateSize).with(Role::write, Role::read).sort(roleFrequency.iterator())).peek(r -> {
        try {
          r.write(rolesDataOutput);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }).count();
    }
  }
  
  public static void extract(ExtractArgs config) {
    Path pbf = config.pbf;
    Path workDir = config.common.workDir;
    Path tempDir = config.common.tempDir;
    boolean overwrite = config.overwrite;
    
    if(workDir == null)
      workDir = Paths.get(".");
    
    if(tempDir == null)
      tempDir = workDir;

    int worker = config.distribute.worker;
    int workerTotal = config.distribute.totalWorkers;
    if (worker >= workerTotal)
      throw new IllegalArgumentException("worker must be lesser than totalWorker!");

    long availableMemory = SizeEstimator.estimateAvailableMemory();
    System.out.print("extracting key tables ...");
    Extract extract = Extract.withMaxMemory(availableMemory).withWorkDirectory(workDir).withTempDirectory(tempDir);
    if (config.distribute.merge) {
      try {
        List<File> tmp;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(workDir, KVFCollector.tempPrefix + "_*")) {
          tmp = StreamSupport.stream(stream.spliterator(), false).map(Path::toFile).collect(Collectors.toList());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        if(tmp.isEmpty())
          throw new RuntimeException("no files to merge");
        KVFCollector kvFrequency = new KVFCollector(tmp);
        System.out.print("sorting tags by frequency ...");
        extract.sortByFrequency(kvFrequency);
        System.out.println(" done!");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      try {
        List<File> tmp;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(workDir, RoleCollector.tempPrefix + "_*")) {
          tmp = StreamSupport.stream(stream.spliterator(), false).map(Path::toFile).collect(Collectors.toList());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        if(tmp.isEmpty())
          throw new RuntimeException("no files to merge");
        RoleCollector roleFrequency = new RoleCollector(tmp);
        System.out.print("sorting roles by frequency ...");
        extract.sortByFrequency(roleFrequency);
        System.out.println(" done!");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    if (overwrite || !Files.exists(workDir.resolve("extract_keys"))
        || !Files.exists(workDir.resolve("extract_keyvalues")) || !Files.exists(workDir.resolve("extract_roles"))) {
      ExtractKeyTablesResult result = extract.extract(config, worker, workerTotal, false);

      try {
        if (workerTotal > 1) {
          result.kvFrequency.writeTemp();
          result.roleFrequency.writeTemp();
          return;
        }

        extract.sortByFrequency(result);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    
    
  }

  public static void main(String[] args) {
    ExtractArgs config = new ExtractArgs();
    JCommander jcom = JCommander.newBuilder().addObject(config).build();

    try {
      jcom.parse(args);
    } catch (ParameterException e) {
      System.out.println("");
      System.out.println(e.getLocalizedMessage());
      System.out.println("");
      jcom.usage();
      return;
    }
    if (config.common.help) {
      jcom.usage();
      return;
    }
    Stopwatch stopwatch = Stopwatch.createStarted();
    extract(config);
    System.out.println("extract done in "+stopwatch);
  }
}
