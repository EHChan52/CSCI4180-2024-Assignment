package assg2p2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
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
                    // Ignore the third token (weight)
                    context.write(sourceNode, destinationNode);
                    context.write(destinationNode, new IntWritable(-1)); // Mark destination node as non-dangling
                }
            }
        }
    }

    public static class PreprocessReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
        public static enum NodeCounter {
            NODE_COUNT
        }

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Set<Integer> uniqueNeighbors = new HashSet<>();
            boolean isDangling = true;

            for (IntWritable val : values) {
                if (val.get() != -1) {
                    uniqueNeighbors.add(val.get());
                } else {
                    isDangling = false;
                }
            }

            StringBuilder adjList = new StringBuilder();
            boolean first = true;
            for (Integer neighbor : uniqueNeighbors) {
                if (!first) {
                    adjList.append(" ");
                }
                adjList.append(neighbor);
                first = false;
            }

            if (isDangling) {
                context.write(key, new Text("")); // Dangling node with empty adjacency list
            } else {
                context.write(key, new Text(adjList.toString()));
            }

            // Increment the counter for each node processed
            context.getCounter(NodeCounter.NODE_COUNT).increment(1);
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
        prPreprocessJob.setOutputValueClass(Text.class);

        // Set input and output format classes
        prPreprocessJob.setInputFormatClass(TextInputFormat.class);
        prPreprocessJob.setOutputFormatClass(TextOutputFormat.class);

        // Set input and output paths
        FileInputFormat.addInputPath(prPreprocessJob, inputPath);
        FileOutputFormat.setOutputPath(prPreprocessJob, outputPath);

        return prPreprocessJob;
    }
}