package assg2p2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank{
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
        conf.set("alpha", args[0]);
        conf.set("iterationMax", args[1]);
        conf.set("threshold", args[2]);
        
        Job job = Job.getInstance(conf, "PageRank");
        FileInputFormat.addInputPath(job, new Path(args[3]));
        
        //declare a new variable to store the output of the first mapreduce job
        String outputPRP = new String("/tmp/PRPreProcess");
        //output is redirected to another mapreduce job
        PRPreProcess.main(new String[]{args[3], outputPRP});
        
        //declare an integer variable to store the value of numNodes in PRPreProcess
        int numNodes = conf.getInt("numNodes", 0);

        //pagerank loop
        while(i <= iterationMax){
            //
        }
        //Final output dir
        //FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}