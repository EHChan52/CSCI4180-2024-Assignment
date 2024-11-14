package assg2p2;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import assg2p2.PRNodeWritable;
import assg2p2.PRPreProcess.PreprocessMapper;
import assg2p2.PRPreProcess.PreprocessReducer;
import assg2p2.PRAdjust;

public class PageRank {

    public static class PageRankMapper extends Mapper<Object, Text, IntWritable, PRNodeWritable> {
        private double initialPRValue;
        private int iteration;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            initialPRValue = conf.getDouble("initialPRValue", 0.0);
            iteration = Integer.parseInt(conf.get("iteration"));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            IntWritable nodeID = new IntWritable(Integer.parseInt(itr.nextToken()));
            DoubleWritable pageRankValue = new DoubleWritable(Double.parseDouble(itr.nextToken()));
            MapWritable adjList = new MapWritable();
            while (itr.hasMoreTokens()) {
                adjList.put(new IntWritable(Integer.parseInt(itr.nextToken())), new DoubleWritable(0.0));
            }

            PRNodeWritable prNode = new PRNodeWritable();
            prNode.setNodeID(nodeID);
            prNode.setPageRankValue(pageRankValue);
            prNode.setWholeAdjList(adjList);

            // Emit the node structure for the given key (so it remains in the graph)
            context.write(nodeID, prNode);

            // Get the adjacency list as a MapWritable
            if (adjList.size() > 0) {
                double contribution;
                if (iteration == 1) {
                    contribution = initialPRValue;
                } else {
                    contribution = pageRankValue.get() / adjList.size();
                }

                for (Map.Entry<Writable, Writable> entry : adjList.entrySet()) {
                    IntWritable neighborID = (IntWritable) entry.getKey();
                    PRNodeWritable contributionNode = new PRNodeWritable(neighborID, new DoubleWritable(contribution));
                    context.write(neighborID, contributionNode);
                }
            } else {
                // Dangling node, emit its PageRank value to a special key
                context.write(new IntWritable(-1), new PRNodeWritable(nodeID, pageRankValue));
            }
        }
    }

    public static class PageRankReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        public static enum RemainingMassCounter { 
            R_MASS_COUNT 
        };
        private double remainingMass = 0.0;
        private double danglingMass = 0.0;

        @Override
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            if (key.get() == -1) {
                // Accumulate dangling mass
                for (PRNodeWritable value : values) {
                    danglingMass += value.getPageRankValue().get();
                }
                return;
            }

            PRNodeWritable prNode = new PRNodeWritable();
            double sumOfRank = 0.0;

            for (PRNodeWritable value : values) {
                if (value.isNode().get()) {
                    // Preserve the structure of the node
                    prNode.setNodeID(value.getNodeID());
                    prNode.setWholeAdjList(value.getWholeAdjList());
                } else {
                    // Accumulate contributions from neighbors
                    sumOfRank += value.getPageRankValue().get();
                }
            }

            // Set the new PageRank value for the node
            prNode.setPageRankValue(new DoubleWritable(sumOfRank));

            // Accumulate the total remaining mass
            remainingMass += sumOfRank;

            // Write the updated node with the new PageRank value
            context.write(key, prNode);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Here we can set the remaining mass as a counter or some variable in the context
            context.getCounter(RemainingMassCounter.R_MASS_COUNT).increment((long) (remainingMass * 1e9));
            context.getCounter(RemainingMassCounter.R_MASS_COUNT).increment((long) (danglingMass * 1e9));
        }
    }

    public static Job getPRValueJob(Configuration conf, Path inputPath, Path outputPath) throws IOException {
        Job prValueJob = Job.getInstance(conf, "PRValue");

        prValueJob.setJarByClass(PageRank.class);
        prValueJob.setMapperClass(PageRankMapper.class);
        prValueJob.setReducerClass(PageRankReducer.class);
        prValueJob.setNumReduceTasks(1);

        // Set the output key and value classes for the mapper
        prValueJob.setMapOutputKeyClass(IntWritable.class);
        prValueJob.setMapOutputValueClass(PRNodeWritable.class);

        // Set the output key and value classes for the reducer
        prValueJob.setOutputKeyClass(IntWritable.class);
        prValueJob.setOutputValueClass(PRNodeWritable.class);

        // Set input and output format classes
        prValueJob.setInputFormatClass(TextInputFormat.class);
        prValueJob.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Set input and output paths
        FileInputFormat.addInputPath(prValueJob, inputPath);
        FileOutputFormat.setOutputPath(prValueJob, outputPath);

        return prValueJob;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: hadoop jar [.jar file] PageRank [alpha] [iteration] [threshold] [infile] [outdir]");
            System.exit(-1);
        }

        float alpha = Float.parseFloat(args[0]);
        int iterationMax = Integer.parseInt(args[1]);
        float threshold = Float.parseFloat(args[2]);

        Configuration conf = new Configuration();
        conf.setFloat("alpha", alpha);
        conf.setInt("iterationMax", iterationMax);
        conf.setFloat("threshold", threshold);

        Path inputPath = new Path(args[3]);
        Path outputPathPRPreProcess = new Path(args[4] + "/PRPreProcess");
        Path outputPathPRValue = new Path(args[4] + "/PRValue");
        Path outputPathPRAdjust = new Path(args[4]);

        // Job 1: Preprocess
        Job prPreProcessJob = PRPreProcess.getPRPreProcessJob(conf, inputPath, outputPathPRPreProcess);
        prPreProcessJob.setOutputFormatClass(TextOutputFormat.class); // Ensure the output is a Text file
        prPreProcessJob.waitForCompletion(true);
        Counter nodeCounter = prPreProcessJob.getCounters().findCounter(PRPreProcess.PreprocessReducer.NodeCounter.NODE_COUNT);
        int numNodes = (int) nodeCounter.getValue();
        conf.setInt("numNodes", numNodes);

        // Set initial PageRank value as 1/N
        conf.setDouble("initialPRValue", 1.0 / numNodes);

        Path currentInputPath = outputPathPRPreProcess;
        for (int i = 1; i <= iterationMax; i++) {
            conf.setInt("iteration", i);

            // Job 2: PageRank Value Calculation
            Job prValueJob = getPRValueJob(conf, currentInputPath, outputPathPRValue);

            // Delete the output path if it already exists
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outputPathPRValue)) {
                fs.delete(outputPathPRValue, true);
            }

            prValueJob.waitForCompletion(true); // Ensure the job completes before accessing counters

            // Calculate missing Mass
            Counters counters = prValueJob.getCounters();
            long remainingMassLong = counters.findCounter(PageRankReducer.RemainingMassCounter.R_MASS_COUNT).getValue();
            double remainingMass = remainingMassLong / 1e9;
            conf.setDouble("missingMass", 1.0 - remainingMass);

            // Job 3: PageRank Adjustment (with Damping Factor)
            Job prAdjustJob = PRAdjust.getPRAdjustJob(conf, outputPathPRValue, outputPathPRAdjust);

            // Delete the output path if it already exists
            if (fs.exists(outputPathPRAdjust)) {
                fs.delete(outputPathPRAdjust, true);
            }

            prAdjustJob.waitForCompletion(true); // Ensure the job completes before proceeding

            currentInputPath = outputPathPRAdjust;  // Update the current input path to point to the adjusted values for the next iteration
        }

        System.exit(0);
    }
}