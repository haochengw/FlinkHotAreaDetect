package cn.edu.whu.glink.experiment.shenzhen.thresholdcal;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.StringJoiner;

public class StatFileSink implements SinkFunction<Tuple2<Long, Integer>> {

  transient BufferedWriter br;
  String fileName;

  public StatFileSink(String fileName) throws IOException {
    this.fileName = fileName;
  }

  public void invoke(Tuple2<Long, Integer> value, Context context) throws Exception {
    if(br == null) {
      br = new BufferedWriter(new FileWriter(fileName));
    }
    StringJoiner sj = new StringJoiner(",");
    sj.add(String.valueOf(value.f0));
    sj.add(String.valueOf(value.f1));
    br.write(sj.toString());
    br.newLine();
    br.flush();
  }
}
