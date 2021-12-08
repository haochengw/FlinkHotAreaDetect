package cn.edu.whu.glink.areadetect.examples.IO.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class MseFileDataSource implements SourceFunction<String> {

  private String filePath;
  private static Logger logger = LoggerFactory.getLogger("SOURCE");


  public MseFileDataSource(String filePath) throws FileNotFoundException {
    this.filePath = filePath;
  }

  @Override
  public void run(SourceContext<String> sourceContext) throws Exception {
    logger.info("Start scanning the file at " + System.currentTimeMillis());
    InputStream is = new FileInputStream(filePath);
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    List<String> buffer = new LinkedList<>();
    while (br.ready()) {
      String line = br.readLine();
      String[] vals = line.split("\\s|,");
      int[] toCheckFields = new int[]{0, 1, 4, 5, 6};
      for (int toCheckField : toCheckFields) {
        if (vals[toCheckField].equals("")) {
          return;
        }
      }
      buffer.add(line);
    }

    logger.info("Start generating source at " + System.currentTimeMillis());
    long start = System.currentTimeMillis();
    for (String line : buffer) {
      sourceContext.collect(line);
    }
    long cost = System.currentTimeMillis() - start;
    logger.info("Generate all detection units with cost of  " + cost / 1000 + " seconds.");

  }

  @Override
  public void cancel() {
  }
}
