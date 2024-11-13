package assg2p2;

import java.io.IOException;
import java.sql.Driver;
import java.util.Map;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.w3c.dom.Text;

import assg2p2.PRNodeWritable;
import assg2p2.PRPreProcess;
import org.w3c.dom.css.Counter;

public class PageRank{

    public static class PageRankMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        @Override
        public void map(IntWritable key, PRNodeWritable value, Context context) throws IOException, InterruptedException {
            // Emit the node structure for the given key (so it remains in the graph)
            context.write(key, value);
            
            // Get the adjacency list as a MapWritable
            MapWritable adjList = value.getWholeAdjList();
            if (adjList.size() > 0) {
                float contribution = value.getPageRankValue() / adjList.size();
                for (Map.Entry<Writable, Writable> entry : adjList.entrySet()) {
                    IntWritable neighborID = (IntWritable) entry.getKey();
                    PRNodeWritable contributionNode = new PRNodeWritable(neighborID.get(), new FloatWritable(contribution));
                    context.write(neighborID, contributionNode);
                }
            }
        }
    }

    public static class PageRankReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            PRNodeWritable prNode = new PRNodeWritable();
            float sumOfRank = 0.0f;

            for (PRNodeWritable value : values) {
                if (value.isNode()) {
                    // Preserve the structure of the node
                    prNode.setNodeID(value.getNodeID());
                    prNode.setWholeAdjList(value.getWholeAdjList());
                } else {
                    // Accumulate contributions from neighbors
                    sumOfRank += value.getPageRankValue();
                }
            }

            // Set the new PageRank value without random jump
            prNode.setPageRankValue(sumOfRank);

            // Write the updated node with the new PageRank value
            context.write(key, prNode);
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
        prValueJob.setOutputFormatClass(TextOutputFormat.class);

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
        int i = 1;

        float alpha = Float.parseFloat(args[0]);
        int iterationMax = Integer.parseInt(args[1]);
        float threshold = Float.parseFloat(args[2]);
        
        Configuration conf = new Configuration();
        conf.set("alpha", alpha);
        conf.set("iterationMax", iterationMax);
        conf.set("threshold", threshold);
        
        //declare a new variable to store the output of the mapreduce jobs
        String outputPRProcess = new String("/tmp/PRPreProcess");
        String outputPRValue = new String("/tmp/PRValue");
        String outputPRAdjust = new String("/tmp/PRAdjust");

        //Jobs
        Job prPreProcessJob = PRPreProcess.getPRPreProcessJob(conf, new Path(args[3]), new String[]{args[3], outputPRProcess});
        Job prValueJob = getPRValueJob(conf, alpha, outputPRValue);

        prPreProcessJob.waitForCompletion(true);
        Counter nodeCounter = prPreProcessJob.getCounters.findCounter(CountersClass.NodeCounter.NODE_COUNT);


        //output is redirected to another mapreduce job
        ControlledJob jControlPRPreprocess = new ControlledJob(prPreProcessJob.getConfiguration());
        ControlledJob jControlPRValue = new ControlledJob(prValueJob.getConfiguration());

        jControlPRPreprocess.setJob(prPreProcessJob);
        jControlPRValue.setJob(prValueJob);

        jControlPRValue.addDependingJob(jControlPRPreprocess);

        //Job Control
        JobControl jControl = new JobControl("PageRank");
        jControl.addJob(jControlPRValue);

        //pagerank loop
        while(i <= iterationMax){
            jControl.run();
            i++;
        }
        //Final output dir
        //FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(0);
    }

    
}