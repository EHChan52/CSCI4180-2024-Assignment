package assg2p2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PRAdjust {
    public static class PRAdjustMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        private float alpha;
        private float threshold;
        private int numNodes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            alpha = conf.getFloat("alpha", 0.15f);
            threshold = conf.getFloat("threshold", 0.001f);
            numNodes = conf.getInt("numNodes", 1);
        }

        @Override
        public void map(IntWritable key, PRNodeWritable value, Context context) throws IOException, InterruptedException {
            double sum = value.getPageRankValue().get();
            double missingMass = context.getConfiguration().getDouble("missingMass", 0.0);
            double adjustedRank = alpha / numNodes + (1 - alpha) * (sum + missingMass / numNodes);

            value.setPageRankValue(new DoubleWritable(adjustedRank));
            context.write(key, value);
        }
    }

    public static class PRAdjustReducer extends Reducer<IntWritable, PRNodeWritable, Text, Text> {
        private float threshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            threshold = conf.getFloat("threshold", 0.001f);
        }

        @Override
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            PRNodeWritable adjustedNode = new PRNodeWritable();
            double adjustedRank = 0.0;
            for (PRNodeWritable val : values) {
                adjustedRank += val.getPageRankValue().get();
                adjustedNode.setNodeID(val.getNodeID());
                adjustedNode.setWholeAdjList(val.getWholeAdjList());
            }
            adjustedNode.setPageRankValue(new DoubleWritable(adjustedRank));
            if (adjustedRank > threshold) {
                context.write(new Text(adjustedNode.getNodeID().toString()), new Text(Double.toString(adjustedRank)));
            }
        }
    }

    public static Job getPRAdjustJob(Configuration conf, Path inputPath, Path outputPath) throws IOException {
        Job prAdjustJob = Job.getInstance(conf, "PRAdjust");

        prAdjustJob.setJarByClass(PRAdjust.class);
        prAdjustJob.setMapperClass(PRAdjustMapper.class);
        prAdjustJob.setReducerClass(PRAdjustReducer.class);

        prAdjustJob.setMapOutputKeyClass(IntWritable.class);
        prAdjustJob.setMapOutputValueClass(PRNodeWritable.class);

        prAdjustJob.setOutputKeyClass(Text.class);
        prAdjustJob.setOutputValueClass(Text.class);

        prAdjustJob.setInputFormatClass(SequenceFileInputFormat.class);
        prAdjustJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(prAdjustJob, inputPath);
        FileOutputFormat.setOutputPath(prAdjustJob, outputPath);

        return prAdjustJob;
    }
}