package cn.edu.whu.glink.areadetect.examples.IO.source;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class MseKafkaProducer {
  public static void main(String[] args) throws IOException {

    String filePath = "";
    Properties props = new Properties();
    props.put("bootstrap.servers", "7.185.18.250:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

    InputStream is = new FileInputStream(filePath);
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    List<String> buffer = new LinkedList<>();
    int count = 0;
    // 只使用400万数据测试
    while (br.ready() && count < 4000000) {
      String line = br.readLine();
      String[] vals = line.split("\\s|,");
      int[] toCheckFields = new int[]{0, 1, 4, 5, 6};
      for (int toCheckField : toCheckFields) {
        if (vals[toCheckField].equals("")) {
          return;
        }
      }
      buffer.add(line);
      count++;
    }

    for (String line : buffer) {
      producer.send(new ProducerRecord<>("msedata", line));
    }
  }
}
