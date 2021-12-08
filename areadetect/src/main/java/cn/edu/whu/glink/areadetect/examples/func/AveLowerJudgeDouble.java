package cn.edu.whu.glink.areadetect.examples.func;

import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 计算DetectionUnit.val在时间窗口内的平均值，如果小于一个阈值，认为是兴趣/异常单元。
 */
public class AveLowerJudgeDouble extends ProcessWindowFunction<DetectUnit, DetectUnit, Long, TimeWindow> {

  private final double threshold;
  private static final Logger LOG = LoggerFactory.getLogger(AveLowerJudgeDouble.class);

  public AveLowerJudgeDouble(double threshold) {
    this.threshold = threshold;

  }

  @Override
  public void process(Long aLong, Context context, Iterable<DetectUnit> elements, Collector<DetectUnit> out) throws Exception {
    long start = System.currentTimeMillis();
    if (isAbnormal(elements)) {
      out.collect(aggregate(elements));
    }
    long end = System.currentTimeMillis();
    LOG.warn(context.window().getEnd() + "," + "abnormaljudge," + (end - start));
  }

  protected boolean isAbnormal(Iterable<DetectUnit> elements) {
    int count = 0;
    double sum = 0;
    for (DetectUnit element : elements) {
      count++;
      sum += (Double) element.getVal();
    }
    return sum / count < threshold;
  }

  protected DetectUnit aggregate(Iterable<DetectUnit> elements) {
    int count = 0;
    double sum = 0;
    for (DetectUnit element : elements) {
      count++;
      sum += (Double) element.getVal();
    }
    double ave = sum / count;
    DetectUnit res = elements.iterator().next();
    res.setVal(ave);
    return res;
  }
}