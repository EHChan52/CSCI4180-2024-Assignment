package assg2p2;

import java.io.IOException;
import java.util.Map;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Text;

import assg2p2.PRNodeWritable;

public class PageRank{

    public static class PageRankMapper extends Mapper<Object, PRNodeWritable, IntWritable, PRNodeWritable> {
        @Override
        public void map(Object key, PRNodeWritable value, Context context) throws IOException, InterruptedException {
            // Emit the node structure for the given key (so it remains in the graph)
            context.write(new IntWritable(value.getNodeID()), value);
            
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
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: hadoop jar [.jar file] PageRank [alpha] [iteration] [threshold] [infile] [outdir]");
            System.exit(-1);
        }
        int i = 1;

        float alpha = Float.parseFloat(args[0]);
        int iterationMax = Integer.parseInt(args[1]);
        float threshold = Float.parseFloat(args[2]);
        
        Configuration confPRP = new Configuration();
        //confPRP.set("alpha", args[0]);
        confPRP.set("iterationMax", args[1]);
        //confPRP.set("threshold", args[2]);
        
        Job jobPRP = Job.getInstance(confPRP, "PRPreProcess");
        FileInputFormat.addInputPath(jobPRP, new Path(args[3]));
        
        //declare a new variable to store the output of the first mapreduce job
        String outputPRP = new String("/tmp/PRPreProcess");
        //output is redirected to another mapreduce job
        PRPreProcess.main(new String[]{args[3], outputPRP});
        
        //declare an integer variable to store the value of numNodes within PRPreProcess
        int numNodes = confPRP.getInt("numNodes", 0);

        //pagerank loop
        while(i <= iterationMax){
            Configuration confPR = new Configuration();
            confPR.set("mapreduce.output.textoutputformat.separator", " ");
            confPR.set("outdir", args[4]);
            confPR.set("PRvalueoutdir", "/tmp/PRValue" + i);

            Job jobPR = Job.getInstance(confPR, "PageRank");
            
            jobPR.setJarByClass(PageRank.class);
            jobPR.setMapperClass(PageRankMapper.class);
            jobPR.setReducerClass(PageRankReducer.class);

            // Set the output key and value classes for the mapper
            jobPR.setMapOutputKeyClass(IntWritable.class);
            jobPR.setMapOutputValueClass(PRNodeWritable.class);

            // Set the output key and value classes for the reducer
            jobPR.setOutputKeyClass(IntWritable.class);
            jobPR.setOutputValueClass(PRNodeWritable.class);
            
            FileInputFormat.addInputPath(jobPR, new Path(outputPRP));
            FileOutputFormat.setOutputPath(jobPR, new Path(new String("/tmp/PR-" + i)));
            prJob.waitForCompletion(true);
            i++;
        }
        //Final output dir
        //FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(0);
    }
}