package cn.edu.whu.glink.examples.io;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.locationtech.jts.geom.Geometry;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.StringJoiner;

/**
 * @author Haocheng Wang
 * Created on 2022/10/8
 */
public class KafkaUtil {
  public static KafkaSource<String> getKafkaSource(String topicName) {
    return KafkaSource.<String>builder()
        .setBootstrapServers("localhost:9092")
        .setTopics(topicName)
        .setGroupId("my-group")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setBounded(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new SimpleStringSchema()).build();
  }

  static Properties props = new Properties();
  static {
      props.put("bootstrap.servers", "u0:9092,u1:9092,u2:9092");
  }


  public static FlinkKafkaProducer<Geometry> getKafkaSink(String topicName) {
      return new FlinkKafkaProducer<Geometry>(topicName, (SerializationSchema<Geometry>) value -> {
          StringJoiner sj = new StringJoiner("|");
          Tuple tuple = (Tuple) value.getUserData();
          // 1. 区域ID 2. 时间 3. 平均值 4. 单元数量
          sj.add(value.toString());
          for (int i = 0; i < 4; i++) {
              sj.add(tuple.getField(i).toString());
          }
          return sj.toString().getBytes(StandardCharsets.UTF_8);
      }, props);
  }
}
