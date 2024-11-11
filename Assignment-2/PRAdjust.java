package assg2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PRAdjust {
    public static class PRAdjustMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable, DoubleWritable> {
        private IntWritable pID = new IntWritable();
        // private FloatWritable rank = new FloatWritable();
        private PRNodeWritable node = new PRNodeWritable();
        private float alpha;
        private float missingMass;
        private int numPages;
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            alpha = Float.parseFloat(conf.get("alpha"));
            numNodes = Integer.parseInt(conf.get("numNodes"));
        }

        public void map(IntWritable key, PRNodeWritable value, IntWritable missingMass, Context context) throws IOException, InterruptedException {

            DoubleWritable sum = value.getPageRankValue();
            DoubleWritable adjustedRank = sum;
            if ( alpha != 0 && missingMass != 0) {
               adjustedRank = alpha * (1 / numNodes) + (1 - alpha) * (sum + missingMass/numNodes);
            }
            context.write(key, adjustedRank);
        }
    }
    public static class PRAdjustReducer extends Reducer<IntWritable, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<IntWritable, DoubleWritable,> values, Context context) throws IOException, InterruptedException {
            context.write(key, new FloatWritable(adjustedRank));
        }
    }
    
}