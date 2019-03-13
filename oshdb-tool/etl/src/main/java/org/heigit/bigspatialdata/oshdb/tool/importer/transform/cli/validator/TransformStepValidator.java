package org.heigit.bigspatialdata.oshdb.tool.importer.transform.cli.validator;

import java.util.Set;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Sets;

public class TransformStepValidator implements IParameterValidator{
  private static final Set<String> stepSet;
  
  static {
    stepSet = Sets.newHashSet("a","all","n","node","w","way","r","relation");
  }

  @Override
  public void validate(String name, String value) throws ParameterException {
    final String step = value.trim().toLowerCase();

    if(!stepSet.contains(step))
      throw new ParameterException(value+" for parameter " + name + " is not a valid value. Allowed values are (a,all,n,node,w,way,r,relation)");
  }

}
