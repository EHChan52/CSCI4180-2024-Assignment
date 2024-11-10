package assg2;

public class PageRank{
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