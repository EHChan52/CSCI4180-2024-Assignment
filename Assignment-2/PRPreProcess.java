package assg2p2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class PRPreProcess {

    public static class PreprocessMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable sourceNode = new IntWritable();
        private IntWritable destinationNode = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            if (itr.hasMoreTokens()) {
                sourceNode.set(Integer.parseInt(itr.nextToken()));
                if (itr.hasMoreTokens()) {
                    destinationNode.set(Integer.parseInt(itr.nextToken()));
                    // Skip the third token
                    if (itr.hasMoreTokens()) {
                        itr.nextToken();
                    }
                    context.write(sourceNode, destinationNode);
                }
            }
        }
    }

    public static class PreprocessReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            StringBuilder adjListStr = new StringBuilder();
            for (IntWritable val : values) {
                if (adjListStr.length() > 0) {
                    adjListStr.append(" ");
                }
                adjListStr.append(val.get());
            }
            context.write(key, new Text(adjListStr.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PRProcess");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PreprocessMapper.class);
        job.setReducerClass(PreprocessReducer.class);

        // Set the output key and value classes for the mapper
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set the output key and value classes for the reducer
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}