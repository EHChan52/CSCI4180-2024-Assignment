import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PRPreProcess {
    public static class PRPreProcessMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(" ");
            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }

    public static class PRPreProcessReducer extends Reducer<Text, Text, Text, PRNodeWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            PRNodeWritable node = new PRNodeWritable();
            List<String> adjacencyList = new ArrayList<>();
            for (Text value : values) {
                adjacencyList.add(value.toString());
            }
            node.setAdjacencyList(adjacencyList);
            node.setPageRank(1.0); // Initial PageRank value
            context.write(key, node);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PRPreProcess <input> <output>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PRPreProcess");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PRPreProcessMapper.class);
        job.setReducerClass(PRPreProcessReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PRNodeWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}