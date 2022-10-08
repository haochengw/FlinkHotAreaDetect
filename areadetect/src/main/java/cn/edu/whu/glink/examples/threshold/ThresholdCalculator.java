package cn.edu.whu.glink.examples.threshold;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * 单峰阈值选取
 */
public class ThresholdCalculator {

    public static int getThreshold(double leftX, double leftY, double rightX, double rightY, HashMap<Integer, Integer> points) {
        double maxDist = 0;
        int threshold = 0;
        for (Map.Entry<Integer, Integer> entry : points.entrySet()) {
            double dist = pointToLine(leftX, leftY, rightX, rightY, entry.getKey(), entry.getValue());
            if (dist >= maxDist) {
                threshold = entry.getKey();
                maxDist = Math.max(dist, maxDist);
            }
        }
        return threshold;
    }

    // 点到直线的最短距离的判断 点（x0,y0） 到由两点组成的线段（x1,y1） ,( x2,y2 )

    private static double pointToLine(double x1, double y1, double x2, double y2, double x0, double y0) {
        double space = 0;
        double a, b, c;
        a = lineSpace(x1, y1, x2, y2);// 线段的长度
        b = lineSpace(x1, y1, x0, y0);// (x1,y1)到点的距离
        c = lineSpace(x2, y2, x0, y0);// (x2,y2)到点的距离
        if (c <= 0.000001 || b <= 0.000001) {
            space = 0;
            return space;
        }
        if (a <= 0.000001) {
            space = b;
            return space;
        }
        if (c * c >= a * a + b * b) {
            space = b;
            return space;
        }
        if (b * b >= a * a + c * c) {
            space = c;
            return space;
        }
        double p = (a + b + c) / 2;// 半周长
        double s = Math.sqrt(p * (p - a) * (p - b) * (p - c));// 海伦公式求面积
        space = 2 * s / a;// 返回点到线的距离（利用三角形面积公式求高）
        return space;
    }

    // 计算两点之间的距离

    private static double lineSpace(double x1, double y1, double x2, double y2) {
        double lineLength = 0;
        lineLength = Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2)
                * (y1 - y2));

        return lineLength;

    }


    public static int getThreshold(String statsFile) throws Exception {
        HashMap<Integer, Integer> map = new HashMap<>();
        initMap(statsFile, map);
        return (getThreshold(1, map.get(1), maxX, map.get(maxX), map));
    }

    static int maxX;

    private static void initMap(String filePath, HashMap<Integer, Integer> map) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        while ((line = br.readLine()) != null) {
            String[] tokens = line.split(",");
            maxX = Integer.parseInt(tokens[0]);
            map.put(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
        }
    }
}
