package org.heigit.bigspatialdata.oshdb.tool.etl.cmdarg;

import com.beust.jcommander.IStringConverter;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PathConverter implements IStringConverter<Path> {

  @Override
  public Path convert(String value) {
    return Paths.get(value);
  }
}
