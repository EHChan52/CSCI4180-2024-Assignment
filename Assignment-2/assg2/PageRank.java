package assg2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class PageRank{

    public static class PageRankMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, IntWritable> {
        private IntWritable sourceNode = new IntWritable();
        private IntWritable destinationNode = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FloatWritable rankValue = value.getPageRankValue / value.getWholeAdjList.length;
            context.write(key, value);

            for (Writable nodeID : value.getWholeAdjList.keySet()) {
                context.write(nodeID, rankValue);
            }
        }
    }

    public static class PageRankReducer extends Reducer<IntWritable, Writable, IntWritable, PRNodeWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Writable> values, Context context) throws IOException, InterruptedException {
            PRNodeWritable prNode = new PRNodeWritable();
            FloatWritable mass = new FloarWritable(0);
            for (Writeable value : values) {
                if (value.isNode()){
                    prNode = value;
                } else {
                    mass += value;
                }
            }
            prNode.setPageRankValue(mass);
            context.write(key, prNode);
        }
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: hadoop jar [.jar file] PageRank [alpha] [iteration] [threshold] [infile] [outdir]");
            System.exit(-1);
        }

        float alpha = Float.parseFloat(args[0]);
        int iteration = Integer.parseInt(args[1]);
        float threshold = Float.parseFloat(args[2]);
        
        Configuration conf = new Configuration();
        conf.set("alpha", args[0]);
        conf.set("iteration", args[1]);
        conf.set("threshold", args[2]);

        

        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}