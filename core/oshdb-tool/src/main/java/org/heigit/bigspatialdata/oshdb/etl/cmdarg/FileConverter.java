package org.heigit.bigspatialdata.oshdb.etl.cmdarg;

import com.beust.jcommander.IStringConverter;
import java.io.File;

public class FileConverter implements IStringConverter<File> {

  @Override
  public File convert(String value) {
    return new File(value);
  }
}
