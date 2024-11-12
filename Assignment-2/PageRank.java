package assg2p2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Text;

import assg2p2.PRNodeWritable;

public class PageRank{
    public static class PageRankMapper extends Mapper<>{

    }

    public static class PageRankReducer extends Reducer<>{

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