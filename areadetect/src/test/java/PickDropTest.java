import cn.edu.whu.glink.areadetect.feature.DetectUnit;
import cn.edu.whu.glink.index.GeographicalGridIndex;
import cn.edu.whu.glink.experiment.shenzhen.Common;
import cn.edu.whu.glink.experiment.shenzhen.areadetect.FileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

import static cn.edu.whu.glink.experiment.shenzhen.Common.*;

public class PickDropTest {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = Common.env;
        SingleOutputStreamOperator<Tuple2<DetectUnit, String>> raw =
                env.addSource(new FileSource( "D:\\shenzhen\\shenzhen_result_sorted.txt"))
                        .map(new Parser(partitionSize));

        raw.filter(r -> r.f0.getId() == 3513283249381L && r.f0.getTimestamp() <= 1451061600000L && r.f0.getTimestamp() >= 1451059800000L).print();
        env.execute();
    }

    public static class Parser implements MapFunction<String, Tuple2<DetectUnit, String>> {

        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private final GeographicalGridIndex partitionIndex;

        public Parser(double rasterSize) {
            partitionIndex = new GeographicalGridIndex(rasterSize);
        }

        @Override
        public Tuple2<DetectUnit, String> map(String value) throws Exception {
            String[] tokens = value.split(",");
            long timestamp = Long.parseLong(tokens[2]) * 1000;
            double lng = Double.parseDouble(tokens[0]);
            double lat = Double.parseDouble(tokens[1]);
            return new Tuple2<>(new DetectUnit(partitionIndex.getIndex(lng, lat), timestamp, 1), value);
        }
    }
}
