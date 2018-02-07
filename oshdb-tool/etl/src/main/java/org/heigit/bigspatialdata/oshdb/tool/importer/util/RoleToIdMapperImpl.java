package org.heigit.bigspatialdata.oshdb.tool.importer.util;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.function.ToIntFunction;

public class RoleToIdMapperImpl implements RoleToIdMapper {
  private final StringToIdMappingImpl roleToId;

  public RoleToIdMapperImpl(StringToIdMappingImpl roleToId) {
    this.roleToId = roleToId;
  }

  @Override
  public int getRole(String role) {
    return roleToId.getId(role);
  }
  
  
  public static RoleToIdMapperImpl load(String roleToIdMapping,ToIntFunction<String> hashFunction) throws FileNotFoundException, IOException {
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(roleToIdMapping)))) {
      return read(in,hashFunction);
    }
  }

  public void write(DataOutput out) throws IOException {
    roleToId.write(out);
  }

  public static RoleToIdMapperImpl read(DataInput in,ToIntFunction<String> hashFunction) throws IOException{
    StringToIdMappingImpl roleToId = StringToIdMappingImpl.read(in,hashFunction);
    return new RoleToIdMapperImpl(roleToId);
  }

  @Override
  public long estimatedSize() {
    return roleToId.estimatedSize();
  }
}
