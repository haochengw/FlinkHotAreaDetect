package cn.edu.whu.glink.experiment.shenzhen.areadetect;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.locationtech.jts.geom.Geometry;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringJoiner;

public class FileSink implements SinkFunction<Geometry> {

  transient BufferedWriter br;
  String fileName;

  public FileSink(String fileName) throws IOException {
    this.fileName = fileName;
  }

  public void invoke(Geometry value, Context context) throws Exception {
    if(br == null) {
      br = new BufferedWriter(new FileWriter(fileName));
    }
    StringJoiner sj = new StringJoiner("|");
    Tuple tuple = (Tuple) value.getUserData();
    // 1. 区域ID 2. 时间 3. 平均值 4. 上客占比 5. 单元数量
    sj.add(value.toString());
    for (int i = 0; i < 5; i++) {
      sj.add(tuple.getField(i).toString());
    }
    br.write(sj.toString());
    br.newLine();
    br.flush();
  }
}
