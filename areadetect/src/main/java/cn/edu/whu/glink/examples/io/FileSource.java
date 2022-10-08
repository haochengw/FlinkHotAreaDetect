package cn.edu.whu.glink.examples.io;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSource implements SourceFunction<String> {

  private final String filePath;
  private final Logger logger = LoggerFactory.getLogger(FileSource.class);
  private boolean flag = false;

  public FileSource(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public void run(SourceContext<String> sourceContext) throws Exception {
    InputStream is = Files.newInputStream(Paths.get(filePath));
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    int i = 0;
    while (br.ready()) {
      if (!flag) {
        logger.info("Start scanning the file at " + System.currentTimeMillis());
      }
      flag = true;
      String line = br.readLine();
      if(i == 0) {
        i++;
        continue;
      }
      i++;
      sourceContext.collect(line);
    }
    logger.info("Start scanning the file at " + System.currentTimeMillis());
  }

  @Override
  public void cancel() {

  }
}
