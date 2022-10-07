package cn.edu.whu.glink.areadetect.examples.IO.source;

import cn.edu.whu.glink.areadetect.examples.IO.source.helper.NotNullFilter;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class MseKafkaDataSource {

  static Properties properties = new Properties();
  static String topicname = "msedata";


  static {
    properties.put("bootstrap.servers", "7.185.18.250:9092");
    properties.put("zookeeper.connect", "7.185.18.250:2182");
    properties.put("auto.offset.reset", "latest");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
  }

  public static SingleOutputStreamOperator<String> getSource(StreamExecutionEnvironment env) {
    return env.addSource(new FlinkKafkaConsumer<>(topicname, new SimpleStringSchema(), properties))
        .filter(new NotNullFilter(new int[]{0, 1, 3, 4, 5, 6}));
  }
}
