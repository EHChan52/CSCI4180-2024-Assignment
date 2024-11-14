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
    public static class PRAdjustMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, DoubleWritable> {
        private IntWritable pID = new IntWritable();
        // private FloatWritable rank = new FloatWritable();
        private PRNodeWritable node = new PRNodeWritable();
        private float alpha;
        private float missingMass;
        private int numNodes;
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Counter someCount = conf.getCounter(nodeCounter.CountersEnum.NUM_NODES);

            alpha = Float.parseFloat(conf.get("alpha"));
            threshold = Float.parseFloat(conf.get("threshold"));
            numNodes =  someCount.getValue();
            missingMass = Double.parseDouble(conf.get("missingMass"));
        }

        public void map(IntWritable key, PRNodeWritable value, Context context) throws IOException, InterruptedException {

            DoubleWritable sum = value.getPageRankValue();
            DoubleWritable adjustedRank = sum;
            if ( alpha != 0 && missingMass != 0) {
               adjustedRank = alpha * (1 / numNodes) + (1 - alpha) * (sum + missingMass/numNodes);
            }
            if(adjustedRank > threshold){
                value.setPageRankValue(adjustedRank);
                context.write(key, value);
            }
        }
    }
    public static class PRAdjustReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {

        public void reduce(IntWritable key, Iterable< PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            
            context.write(key,values);
        }
    }

    public static Job getPRAdjustJob( Configuration conf, Path outputPath) {
        Job PRAdjustJob = Job.getInstance(conf, "PRAdjust");

        PRAdjustJob.setJarByClass(PRAdjust.class);
        PRAdjustJob.setMapperClass(PRAdjustMapper.class);
        PRAdjustJob.setReducerClass(PRAdjustReducer.class);

        PRAdjustJob.setMapOutputKeyClass(IntWritable.class);
        PRAdjustJob.setMapOutputValueClass(DoubleWritable.class);

        PRAdjustJob.setOutputKeyClass(Text.class);
        PRAdjustJob.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(PRAdjustJob, inputPath);
        FileOutputFormat.setOutputPath(PRAdjustJob, outputPath);

        return PRAdjustJob;
    }
    
}