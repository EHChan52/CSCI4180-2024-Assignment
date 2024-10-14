import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NgramCount{

     public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{
            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();
            private List<String> ngramQueue = new ArrayList<String>();
            private int N;
            private Map<String, Integer> ngramCounts = new HashMap<String, Integer>();
            private Map<String, Map<String, Integer>> cleanupMap = new HashMap<String, Map<String, Integer>>();
            

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                Configuration conf = context.getConfiguration();
                N = Integer.parseInt(conf.get("N"));

                StringTokenizer itr = new StringTokenizer(value.toString(), "\\P{Alpha}+");
                while (itr.hasMoreTokens()) {
                    ngramQueue.add(itr.nextToken());
                    if (ngramQueue.size() > N) {
                        ngramQueue.remove(0);  
                    }
                    if (ngramQueue.size() == N) {
                        ngramCounts.clear();
                        boolean isFirst = true;
                        StringBuilder tempString = new StringBuilder("");
                        for (String ngram : ngramQueue) {
                            if (isFirst) {
                                isFirst = false;
                                continue;
                            }
                            tempString.append(ngram).append(" ");
                        }
                        String valueOfKey = tempString.toString().trim();
                        // check if cleanupMap contains the key
                        // if (cleanupMap.containsKey(ngramQueue.get(0))) {
                        //     IntWritable count = new IntWritable(ngramCounts.get(ngramQueue.get(0))); //seem buggy
                        //     ngramCounts.set(count.get() + 1);
                        // } else {
                        //     ngramCounts.put(ngram, one);
                        // }
                        if (cleanupMap.containsKey(ngramQueue.get(0))) {
                            Map<String, Integer> innerMap = cleanupMap.get(ngramQueue.get(0));
                            if (innerMap.containsKey(valueOfKey)) {
                                IntWritable count = new IntWritable(innerMap.get(valueOfKey));
                                ngramCounts.put(valueOfKey, count.get() + 1);
                            } else {
                                ngramCounts.put(valueOfKey, one);
                            }
                        } else {
                            ngramCounts.put(valueOfKey, one);
                        }
                        cleanupMap.put(ngramQueue.get(0), ngramCounts);

                    }
                }

            }

            public void cleanup(Context context) throws IOException, InterruptedException {
                for (String key : cleanupMap.keySet()) {
                    context.write(new Text(key), cleanupMap.get(key));
                }
            }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
            private IntWritable result = new IntWritable();
            private MapWritable itemCountOfKey = new MapWritable();
            private Map<String, Map<String, Integer>> cleanupMap = new HashMap<String, Map<String, Integer>>();

            public void reduce(IntWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
                itemCountOfKey.clear(); // map for each item
                for (MapWritable val : values) {
                    for( Writable word : val.keySet()) {
                        IntWritable currentCount = (IntWritable) val.get(word);
                        if (itemCountOfKey.containsKey(word)) {
                            IntWritable storedCount = (IntWritable) itemCountOfKey.get(word);
                            itemCountOfKey.set(storedCount.get() +currentCount.get());
                        } else {
                            itemCountOfKey.put(word, new IntWritable(currentCount));
                        }
                    }
                }
                cleanupMap.put(key, itemCountOfKey);
            }

            public void cleanup(Context context) throws IOException, InterruptedException {
                for (String key : cleanupMap.keySet()) {
                    MapWritable innerMap = cleanupMap.get(key);
                    for ( Writable innerKey : innerMap.keySet()) {
                        context.write(new Text(key + " " + innerKey.toString()), innerMapMap.get(innerKey));
                    }
                }
            }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("N", args[2]);
        Job job = Job.getInstance(conf, "N-gram count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}