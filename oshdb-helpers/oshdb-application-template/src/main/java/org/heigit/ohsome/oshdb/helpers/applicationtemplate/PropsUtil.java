package org.heigit.ohsome.oshdb.helpers.applicationtemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.Properties;

public class PropsUtil {

  private PropsUtil() {
  }

  public static void load(Properties props, Path path) throws IOException {
    if (path != null && Files.exists(path)) {
      try (var in = Files.newInputStream(path, StandardOpenOption.READ)) {
        props.load(in);
      }
    }
  }

  public static void set(Properties props, String key, String value) {
    if (value != null) {
      props.put(key, value);
    }
  }

  public static void set(Properties props, String key, Boolean value) {
    if (value != null) {
      props.setProperty(key, Boolean.toString(value));
    }
  }

  public static Optional<String> get(Properties props, String key) {
    return Optional.ofNullable(props.getProperty(key));
  }

}
