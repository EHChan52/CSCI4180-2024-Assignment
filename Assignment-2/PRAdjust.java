import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PRAdjust {
    public static class PRAdjustMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("adjust"), value);
        }
    }

    public static class PRAdjustReducer extends Reducer<Text, Text, Text, Text> {
        private double alpha;
        private double threshold;
        private int nodeCount;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            alpha = conf.getDouble("alpha", 0.15);
            threshold = conf.getDouble("threshold", 0.01);
            nodeCount = conf.getInt("nodeCount", 1);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double missingMass = 0.0;
            for (Text value : values) {
                missingMass += Double.parseDouble(value.toString());
            }

            double adjustedPR = alpha / nodeCount + (1 - alpha) * (missingMass / nodeCount);
            if (adjustedPR > threshold) {
                context.write(key, new Text(String.valueOf(adjustedPR)));
            }
        }
    }
}