package cn.edu.whu.glink.areadetect.examples.IO.sink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.locationtech.jts.geom.Geometry;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.StringJoiner;

/**
 * 4项
 * MultiPolygon[wkt] | Convex hull[wkt] | 区域id[long] | 时间id[long] | 区域数量/面积[integer]
 */
public class ResultFileSinkFunction extends RichSinkFunction<Geometry> {
  String outputFilePath;
  BufferedWriter bw;

  public ResultFileSinkFunction(String outputFilePath) {
    this.outputFilePath = outputFilePath;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    bw = new BufferedWriter(new FileWriter(outputFilePath));
  }

  @Override
  public void invoke(Geometry value, Context context) throws Exception {
    super.invoke(value, context);
    Tuple userData = (Tuple) value.getUserData();
    StringJoiner joiner = new StringJoiner("|");
    joiner.add(value.toString());
    for (int i = 0; i < userData.getArity(); i++) {
      joiner.add(userData.getField(i).toString());
    }
    bw.write(joiner.toString());
    bw.newLine();
  }

  @Override
  public void close() throws Exception {
    bw.flush();
    bw.close();
  }
}
