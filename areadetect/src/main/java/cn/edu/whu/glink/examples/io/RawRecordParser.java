package cn.edu.whu.glink.examples.io;

import cn.edu.whu.glink.areadetect.datatypes.PickDropPoint;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * input format: 'row-id, datetime, lng, lat, type'
 * @author Haocheng Wang
 * Created on 2022/10/8
 */
public class RawRecordParser implements MapFunction<String, PickDropPoint> {
  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @Override
  public PickDropPoint map(String value) throws Exception {
    String[] tokens = value.split(",");
    Date dateTime = sdf.parse(tokens[0]);
    long timestamp = dateTime.getTime();
    double lng = Double.parseDouble(tokens[1]);
    double lat = Double.parseDouble(tokens[2]);
    int type = Integer.parseInt(tokens[3]);
    boolean isPickUp = type == 0;
    return new PickDropPoint(lng, lat, timestamp, isPickUp, null);
  }
}
