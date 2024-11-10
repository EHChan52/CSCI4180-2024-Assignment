package assg2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class PRPreProcess {
    public class Graph{
        private Map<Integer, List<Integer> > adjacencyList;

        public Graph() {
            adjacencyList = new HashMap<>();
        }

        public void addVertex(int vertex){
            adjacencyList.put(vertex, new ArrayList<>());
        }

        public void addEdge(int source, int destination){
            adjacencyList.get(source).add(destination);
        }
        
        public void removeVertex(int vertex){
            adjacencyList.remove(vertex);
            for (List<Integer> neighbors :
                adjacencyList.values()) {
                neighbors.remove(Integer.valueOf(vertex));
            }
        }

        public void removeEdge(int source, int destination){
            adjacencyList.get(source).remove(
                Integer.valueOf(destination));
        }
        
    }

    public class CountMapper extends Mapper<>{

    }

    public class CountReducer extends Reducer<>{
        
    }

    public class PreprocessMapper extends Mapper<>{

    }

    public class PreprocessReducer extends Reducer<>{
        
    }

    public static void main(String fileName) throws Exception {
        Graph adjList = new Graph();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PRProcess");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PreprocessMapper.class);
        job.setReducerClass(PreprocessReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        //job.setOutputValueClass(ArrayWritable.class);

        FileInputFormat.addInputPath(job, new Path(fileName));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}