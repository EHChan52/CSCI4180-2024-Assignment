package assg2p2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Driver;
import java.util.StringTokenizer;

import javax.naming.Context;

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

    public class CountersClass {
        public static enum NodeCounter {
            NODE_COUNT
        }
    }

    public static class PreprocessReducer extends Reducer<IntWritable, IntWritable, IntWritable, PRNodeWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable adjList = new MapWritable(); // Instantiate adjList for each key

            for (IntWritable val : values) {
                adjList.put(val, new FloatWritable(0.0f)); // Adding neighbors to adjList
            }

            PRNodeWritable prNode = new PRNodeWritable();
            prNode.setNodeID(key);
            prNode.setWholeAdjList(adjList);
            context.write(key, prNode);

            // Increment the counter for each node processed
            context.getCounter(CountersClass.NodeCounter.NODE_COUNT).increment(1);
        }
    }

    public static Job getPRPreProcessJob(Configuration conf, Path inputPath, Path outputPath) throws IOException {
        Job prPreprocessJob = Job.getInstance(conf, "PRPreProcess");

        prPreprocessJob.setJarByClass(PRPreProcess.class);
        prPreprocessJob.setMapperClass(PreprocessMapper.class);
        prPreprocessJob.setReducerClass(PreprocessReducer.class);
        prPreprocessJob.setNumReduceTasks(1);

        // Set the output key and value classes for the mapper
        prPreprocessJob.setMapOutputKeyClass(IntWritable.class);
        prPreprocessJob.setMapOutputValueClass(IntWritable.class);

        // Set the output key and value classes for the reducer
        prPreprocessJob.setOutputKeyClass(IntWritable.class);
        prPreprocessJob.setOutputValueClass(PRNodeWritable.class);

        // Set input and output format classes
        prPreprocessJob.setInputFormatClass(TextInputFormat.class);
        prPreprocessJob.setOutputFormatClass(TextOutputFormat.class);

        // Set input and output paths
        FileInputFormat.addInputPath(prPreprocessJob, inputPath);
        FileOutputFormat.setOutputPath(prPreprocessJob, outputPath);

        return prPreprocessJob;
    }
}