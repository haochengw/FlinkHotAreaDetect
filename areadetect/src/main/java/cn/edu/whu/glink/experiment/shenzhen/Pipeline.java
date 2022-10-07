package cn.edu.whu.glink.experiment.shenzhen;

import cn.edu.whu.glink.experiment.shenzhen.areadetect.Main;
import cn.edu.whu.glink.experiment.shenzhen.thresholdcal.Stats;
import cn.edu.whu.glink.shp.ShapeFileUtil;
import cn.edu.whu.glink.experiment.shenzhen.thresholdcal.ThresholdCalculator;

import static cn.edu.whu.glink.experiment.shenzhen.Common.getSinkFileName;

public class Pipeline {

    public static String pickDropFilePath = "D:\\wuhan\\20150502\\pickOrDropSorted.txt";
    public static double RasterSize = 0.4; // 100米网格
    public static int threshold;




    public static void main(String[] args) throws Exception {
        Common.init();
        // 1. 获取阈值
        String statsFileName = Stats.getStatFile();
        threshold = ThresholdCalculator.process(statsFileName);
        threshold = 9;
        System.out.println("RasterSize: " + RasterSize + ", threshold: " + threshold);
//         2. 识别热点区域
        Common.init();
        Main.headAreaDetect();
        // 3. 生成shp
        String heatFileName = "hot-" + getSinkFileName();
        ShapeFileUtil.createShp(heatFileName, "D:\\wuhan\\hotarea_shp", RasterSize);
    }
}
