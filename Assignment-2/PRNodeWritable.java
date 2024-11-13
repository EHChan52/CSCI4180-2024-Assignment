package assg2p2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;

public class PRNodeWritable implements Writable {
    private IntWritable nodeID;
    private MapWritable adjList;
    private DoubleWritable pageRankValue;
    private BooleanWritable pageRankValueFixed;

    public PRNodeWritable(){
        this.nodeID = new IntWritable();
        this.adjList = new MapWritable();
        this.pageRankValue = new DoubleWritable(Float.NEGATIVE_INFINITY);
        this.pageRankValueFixed = new BooleanWritable(false);
    }

    public PRNodeWritable(IntWritable nodeID, DoubleWritable pageRankValue) {
        this.nodeID = nodeID;
        this.adjList = new MapWritable();
        this.pageRankValue = pageRankValue;
        this.pageRankValueFixed = new BooleanWritable(false);
    }

    public void setNodeID(int nodeID) {
        this.nodeID.set(nodeID);
    }

    public IntWritable getNodeID() {
        return this.nodeID;
    }

    public void setPageRankValue(Double pageRankValue) {
        this.pageRankValue.set(pageRankValue);
    }

    public DoubleWritable getPageRankValue() {
        return this.pageRankValue;
    }

    public void setPageRankValueFixed(boolean pageRankValueFixed) {
        this.pageRankValueFixed.set(pageRankValueFixed);
    }

    public BooleanWritable getPageRankValueFixed() {
        return this.pageRankValueFixed;
    }

    public void setWholeAdjList(MapWritable adjList) {
        this.adjList = adjList;
    }

    public MapWritable getWholeAdjList() {
        return this.adjList;
    }

    public BooleanWritable isNode() {
        return this.adjList.size > 0;
    }

    @Override
    public String toString() {
        StringBuilder adjListStr = new StringBuilder();
        for (Writable k : adjList.keySet()) {
            if (adjListStr.length() > 0) {
                adjListStr.append(" ");
            }
            adjListStr.append(((IntWritable) k).get());
        }
        return this.nodeID + " " + this.pageRankValue + " " + adjListStr.toString();
    }

    public static PRNodeWritable fromString(String str) {
        String[] parts = str.split(" ");
        PRNodeWritable prNode = new PRNodeWritable();
        prNode.setNodeID(Integer.parseInt(parts[0]));
        prNode.setPageRankValue(Double.parseDouble(parts[1]));
        for (int i = 2; i < parts.length; i++) {
            prNode.adjList.put(new IntWritable(Integer.parseInt(parts[i])), new DoubleWritable(1.0));
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