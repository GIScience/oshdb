package org.heigit.bigspatialdata.oshdb.tool.importer.transform;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract.KeyValuePointer;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.Role;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.VF;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.RoleToIdMapperImpl;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.SizeEstimator;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.StringToIdMappingImpl;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapper;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.TagToIdMapperImpl;

import com.google.common.base.Functions;

import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

public class TransformerTagRoles {

  public static TagToIdMapper getTagToIdMapper(Path workDirectory) throws FileNotFoundException, IOException {
    final ToIntFunction<String> hashFunction = s -> s.hashCode();
    final Function<OutputStream, OutputStream> output = Functions.identity();

    final Path tag2IdPath = workDirectory.resolve("transform_tag2Id");
    if (Files.exists(tag2IdPath)) {
      return TagToIdMapperImpl.load(tag2IdPath.toString(), hashFunction);
    }

    final Int2IntAVLTreeMap keyHash2Cnt = new Int2IntAVLTreeMap();
    final StringToIdMappingImpl[] value2IdMappings;

    final Function<InputStream, InputStream> input = Functions.identity();
    try (final RandomAccessFile raf = new RandomAccessFile(workDirectory.resolve("extract_keyvalues").toFile(), "r")) {
      final FileChannel channel = raf.getChannel();
      try (
          InputStream in = input
              .apply(new BufferedInputStream(new FileInputStream(workDirectory.resolve("extract_keys").toFile())));
          DataInputStream dataIn = new DataInputStream(in)) {
        final int length = dataIn.readInt();

        value2IdMappings = new StringToIdMappingImpl[length];

        for (int i = 0; i < length; i++) {
          final KeyValuePointer kvp = KeyValuePointer.read(dataIn);
          value2IdMappings[i] = value2IdMapping(kvp, channel, hashFunction);
          final int hash = hashFunction.applyAsInt(kvp.key);
          keyHash2Cnt.addTo(hash, 1);
        }
      }
    }

    final Int2IntMap uniqueKeys = new Int2IntAVLTreeMap();
    final Object2IntMap<String> notUniqueKeys = new Object2IntAVLTreeMap<String>();
    long estimatedSize = 0;
    try (
        InputStream in = input
            .apply(new BufferedInputStream(new FileInputStream(workDirectory.resolve("extract_keys").toFile())));
        DataInputStream dataIn = new DataInputStream(in)) {
      final int length = dataIn.readInt();

      for (int i = 0; i < length; i++) {
        final KeyValuePointer kvp = KeyValuePointer.read(dataIn);
        final int hash = hashFunction.applyAsInt(kvp.key);
        if (keyHash2Cnt.get(hash) > 1) {
          notUniqueKeys.put(kvp.key, i);
          estimatedSize += SizeEstimator.estimatedSizeOfAVLEntryValue(kvp.key)+4;
        } else {
          uniqueKeys.put(hash, i);
          estimatedSize += SizeEstimator.estimatedSizeOfAVLEntryValue("") + 8;
        }
      }
    }
    final StringToIdMappingImpl key2IdMapping = new StringToIdMappingImpl(uniqueKeys, notUniqueKeys, hashFunction, estimatedSize);
    final TagToIdMapperImpl tag2IdMapper = new TagToIdMapperImpl(key2IdMapping, value2IdMappings);
    try (DataOutputStream out = new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(workDirectory.resolve("transform_tag2Id").toFile())))) {
      tag2IdMapper.write(out);
    }
    return tag2IdMapper;
  }

  private static StringToIdMappingImpl value2IdMapping(KeyValuePointer kvp, FileChannel channel, ToIntFunction<String> hashFunction) throws IOException {
    final Int2IntAVLTreeMap valueHash2Cnt = new Int2IntAVLTreeMap();
    DataInputStream valueStream;

    channel.position(kvp.valuesOffset);
    valueStream = new DataInputStream(new BufferedInputStream(Channels.newInputStream(channel),1024*1024));
    for (int j = 0; j < kvp.valuesNumber; j++) {
      final VF vf = VF.read(valueStream);
      final int hash = hashFunction.applyAsInt(vf.value);
      valueHash2Cnt.addTo(hash, 1);
    }

    final Int2IntMap uniqueValues = new Int2IntAVLTreeMap();
    final Object2IntMap<String> notUniqueValues = new Object2IntAVLTreeMap<String>();
    long estimatedSize = 0;
    channel.position(kvp.valuesOffset);
    valueStream = new DataInputStream(Channels.newInputStream(channel));
    for (int j = 0; j < kvp.valuesNumber; j++) {
      final VF vf = VF.read(valueStream);
      final int hash = hashFunction.applyAsInt(vf.value);
      if (valueHash2Cnt.get(hash) > 1) {
        notUniqueValues.put(vf.value, j);
        estimatedSize += SizeEstimator.estimatedSizeOfAVLEntryValue(kvp.key)+4;
      } else {
        uniqueValues.put(hash, j);
        estimatedSize += SizeEstimator.avlTreeEntry() + 8;
      }
    }

    return new StringToIdMappingImpl(uniqueValues, notUniqueValues, hashFunction, estimatedSize);
  }

  public static RoleToIdMapper getRoleToIdMapper(Path workDirectory) throws FileNotFoundException, IOException {
    final ToIntFunction<String> hashFunction = s -> s.hashCode();
    final Function<OutputStream, OutputStream> output = Functions.identity();

    final Path role2IdPath = workDirectory.resolve("transform_role2Id");
    if (Files.exists(role2IdPath)) {
      return RoleToIdMapperImpl.load(role2IdPath.toString(), hashFunction);
    }

    final Int2IntAVLTreeMap roleHash2Cnt = new Int2IntAVLTreeMap();

    final Int2IntMap uniqueRoles = new Int2IntAVLTreeMap();
    final Object2IntMap<String> notUniqueRoles = new Object2IntAVLTreeMap<String>();

    final Function<InputStream, InputStream> input = Functions.identity();
    int roleCount = 0;
    try (
        InputStream in = input
            .apply(new BufferedInputStream(new FileInputStream(workDirectory.resolve("extract_roles").toFile())));
        DataInputStream dataIn = new DataInputStream(new BufferedInputStream(in))) {

      try {
        while (true) {
          final Role role = Role.read(dataIn);
          final int hash = hashFunction.applyAsInt(role.role);
          roleHash2Cnt.addTo(hash, 1);
          roleCount++;
        }
      } catch (EOFException e) {

      }

    }

    long estimatedSize = 0;
    try (

        InputStream in = input
            .apply(new BufferedInputStream(new FileInputStream(workDirectory.resolve("extract_roles").toFile())));
        DataInputStream dataIn = new DataInputStream(in)) {
      
      for (int i = 0; i < roleCount; i++) {
        final Role role = Role.read(dataIn);
        final int hash = hashFunction.applyAsInt(role.role);
        if (roleHash2Cnt.get(hash) > 1) {
          notUniqueRoles.put(role.role, i);
          estimatedSize += SizeEstimator.estimatedSizeOfAVLEntryValue(role.role) + 4;
        } else {
          uniqueRoles.put(hash, i);
          estimatedSize += SizeEstimator.avlTreeEntry() + 8;
        }
      }
    }

    RoleToIdMapperImpl idMapper = new RoleToIdMapperImpl(
        new StringToIdMappingImpl(uniqueRoles, notUniqueRoles, hashFunction,estimatedSize));

    try (DataOutputStream out = new DataOutputStream(
        new BufferedOutputStream(new FileOutputStream(workDirectory.resolve("transform_role2Id").toFile())))) {
      idMapper.write(out);
    }

    return idMapper;

  }

}
