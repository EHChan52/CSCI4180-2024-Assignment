import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {
    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: PageRank <alpha> <iterations> <threshold> <input> <output>");
            System.exit(-1);
        }

        double alpha = Double.parseDouble(args[0]);
        int iterations = Integer.parseInt(args[1]);
        double threshold = Double.parseDouble(args[2]);
        String input = args[3];
        String output = args[4];

        Configuration conf = new Configuration();
        conf.setDouble("alpha", alpha);
        conf.setDouble("threshold", threshold);

        for (int i = 0; i < iterations; i++) {
            Job job = Job.getInstance(conf, "PageRank Iteration " + i);
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(PRNodeWritable.class);

            FileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, new Path(output + i));

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }

            input = output + i;
        }

        Job adjustJob = Job.getInstance(conf, "PageRank Adjustment");
        adjustJob.setJarByClass(PageRank.class);
        adjustJob.setMapperClass(PRAdjustMapper.class);
        adjustJob.setReducerClass(PRAdjustReducer.class);
        adjustJob.setOutputKeyClass(Text.class);
        adjustJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(adjustJob, new Path(input));
        FileOutputFormat.setOutputPath(adjustJob, new Path(output + "final"));

        System.exit(adjustJob.waitForCompletion(true) ? 0 : 1);
    }
}