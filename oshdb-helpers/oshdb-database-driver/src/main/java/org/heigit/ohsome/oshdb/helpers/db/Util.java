package org.heigit.ohsome.oshdb.helpers.db;

import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

public class Util {

  private static final Pattern SUBSTITUTE = Pattern.compile("\\$\\{(\\w+)\\}");

  private Util() {}
  public static Optional<String> getInterpolated(Properties props, String key) {
    return Optional.ofNullable(props.getProperty(key)).map(value -> interpolate(props, value));
  }

  private static String interpolate(Properties props, String value) {
    var matcher = SUBSTITUTE.matcher(value);
    var sb = new StringBuffer();
    while (matcher.find()) {
      var sub = matcher.group(1);
      matcher.appendReplacement(sb, getInterpolated(props, sub).orElse("\\${" + sub + "}"));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }
}
