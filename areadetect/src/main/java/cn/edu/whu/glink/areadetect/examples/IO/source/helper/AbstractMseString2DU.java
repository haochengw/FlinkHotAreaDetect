package cn.edu.whu.glink.areadetect.examples.IO.source.helper;

import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Mse data format : 2021-10-11 18:06:56 291 114000000 22539000 107 \N
 */

public abstract class AbstractMseString2DU implements MapFunction<String, DetectUnit> {

  String format = "yyyy-MM-dd HH:mm:ss";

  @Override
  public DetectUnit map(String line) throws Exception {
    String[] vals = line.split("\\s|,");
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
    long timestamp = LocalDateTime.parse(vals[0] + " " + vals[1], formatter).toEpochSecond(ZoneOffset.ofHours(8)) * 1000;
    double val;
    if (vals[6].equals("\\N")) {
      val = -1;
    } else {
      val = 1;
    }
    long id = getId(vals);
    return new DetectUnit(id, timestamp, val);
  }

  protected abstract long getId(String[] vals);
}
