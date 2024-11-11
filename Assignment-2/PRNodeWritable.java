package assg2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;

public class PRNodeWritable implements Writable {
    private IntWritable nodeID;
    private MapWritable adjList;
    private FloatWritable pageRankValue;
    private BooleanWritable pageRankValueFixed;

    public PRNodeWritable(){
        this.nodeID = new IntWritable();
        this.adjList = new MapWritable();
        this.pageRankValue = new FloatWritable(Float.NEGATIVE_INFINITY);
        this.pageRankValueFixed = new BooleanWritable(false);
    }

    public PRNodeWritableUpdate(IntWritable nodeID, FloatWritable pageRankValue) {
        this.nodeID = nodeID;
        this.adjList = new MapWritable();
        this.pageRankValue = pageRankValue;
        this.pageRankValueFixed = new BooleanWritable(false);
    }

    public void setNodeID(int nodeID) {
        this.nodeID = nodeID;
    }

    public IntWritable getNodeID() {
        return this.nodeID;
    }

    public void setPageRankValue(float pageRankValue) {
        this.pageRankValue = pageRankValue;
    }

    public FloatWritable getPageRankValue() {
        return this.pageRankValue;
    }

    public void setPageRankValueFixed(bool pageRankValueFixed) {
        this.pageRankValueFixed = pageRankValueFixed;
    }

    public FloatWritable getPageRankValueFixed() {
        return this.pageRankValueFixed;
    }

    public void setWholeAdjList(MapWritable adjList) {
        this.adjList = adjList;
    }

    public MapWritable getWholeAdjList() {
        return this.adjList;
    }

    @Override
    public String toString() {
        string adjListStr = " ";
        for(Writable k: adjList.keySet()) {
            adjListStr += ((IntWritable)k).get() + " ";
        }
        return "" + nodeID.get() + " " + pageRankValue.get() + " " + adjListStr; 
    }

    public static PRNodeWritable fromString(String str) {
        String[] parts = str.split(" ");
        //parts[0] is the starting node,parts[1] is the ending node, parts[2] is the weight
        PRNodeWritable prNode = new PRNodeWritable();
        prNode.setNodeID(Integer.parseInt(parts[0]));
        //put parts[0] to parts[1] in adjList
        for(int i = 2; i < parts.length; i++) {
            prNode.adjList.put(new IntWritable(Integer.parseInt(parts[0])), new IntWritable(Integer.parseInt(parts[1])));
        }
        return prNode;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        nodeID.readFields(in);
        adjList.readFields(in);
        pageRankValue.readFields(in);
        pageRankValueFixed.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        nodeID.write(out);
        adjList.write(out);
        pageRankValue.write(out);
        pageRankValueFixed.write(out);
    }
}