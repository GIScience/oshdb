package org.heigit.bigspatialdata.updater.util;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import java.net.MalformedURLException;
import java.net.URL;
import org.slf4j.LoggerFactory;

public class URLValidator implements IParameterValidator {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(URLValidator.class);

  @Override
  public void validate(String name, String value) throws ParameterException {
    try {
      final URL url = new URL(value);
    } catch (MalformedURLException ex) {
      LOG.error("URL not valid!", ex);
    }
  }
}
