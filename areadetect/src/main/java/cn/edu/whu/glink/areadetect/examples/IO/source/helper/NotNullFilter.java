package cn.edu.whu.glink.areadetect.examples.IO.source.helper;

import org.apache.flink.api.common.functions.FilterFunction;

public class NotNullFilter implements FilterFunction<String> {

  int[] toCheckFields;

  public NotNullFilter(int[] toCheckFields) {
    this.toCheckFields = toCheckFields;
  }

  @Override
  public boolean filter(String s) {
    String[] vals = s.split("\\s|,");
    for (int toCheckField : toCheckFields) {
      if (vals[toCheckField].equals("")) {
        return false;
      }
    }
    return true;
  }
}
