
import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.index.GeographicalGridIndex;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;

public class ExampleDataSource implements SourceFunction<DetectUnit> {

  private final String filePath;
  private final GeographicalGridIndex geographicalGridIndex;


  public ExampleDataSource(String filePath, GeographicalGridIndex geographicalGridIndex) throws FileNotFoundException {
    this.filePath = filePath;
    this.geographicalGridIndex = geographicalGridIndex;
  }

  @Override
  public void run(SourceContext<DetectUnit> sourceContext) throws Exception {
//    InputStream is = ExampleUniGridTest.class.getResourceAsStream(filePath);
    InputStream is = new FileInputStream(filePath);
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    while(br.ready()){
      String line = br.readLine();
      String[] vals = line.split(" ");
      double lat = Double.parseDouble(vals[1]);
      double lng = Double.parseDouble(vals[2]);
      Long id = geographicalGridIndex.getIndex(lat, lng);
      DetectUnit unigridUnit = new DetectUnit(id, Long.parseLong(vals[3]), Double.parseDouble(vals[4]));
      sourceContext.collect(unigridUnit);
    }
  }

  @Override
  public void cancel() {
  }
}
