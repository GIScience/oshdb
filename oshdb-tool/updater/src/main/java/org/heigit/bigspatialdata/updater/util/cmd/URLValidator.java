package org.heigit.bigspatialdata.updater.util.cmd;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Validats a URL passed via CMD.
 */
public class URLValidator implements IParameterValidator {

  @Override
  public void validate(String name, String value) throws ParameterException {
    try {
      new URL(value);
    } catch (MalformedURLException ex) {
      throw new ParameterException("The URL passed via " + name + " is not valid", ex);
    }
  }

}
