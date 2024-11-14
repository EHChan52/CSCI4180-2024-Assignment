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
    private DoubleWritable pageRankValue;
    private MapWritable adjList;
    private BooleanWritable isNode;

    public PRNodeWritable() {
        this.nodeID = new IntWritable();
        this.pageRankValue = new DoubleWritable();
        this.adjList = new MapWritable();
        this.isNode = new BooleanWritable(true);
    }

    public PRNodeWritable(IntWritable nodeID, DoubleWritable pageRankValue) {
        this.nodeID = nodeID;
        this.pageRankValue = pageRankValue;
        this.adjList = new MapWritable();
        this.isNode = new BooleanWritable(false);
    }

    public IntWritable getNodeID() {
        return nodeID;
    }

    public void setNodeID(IntWritable nodeID) {
        this.nodeID = nodeID;
    }

    public DoubleWritable getPageRankValue() {
        return pageRankValue;
    }

    public void setPageRankValue(DoubleWritable pageRankValue) {
        this.pageRankValue = pageRankValue;
    }

    public MapWritable getWholeAdjList() {
        return adjList;
    }

    public void setWholeAdjList(MapWritable adjList) {
        this.adjList = adjList;
    }

    public BooleanWritable isNode() {
        return isNode;
    }

    public void setIsNode(BooleanWritable isNode) {
        this.isNode = isNode;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        nodeID.write(out);
        pageRankValue.write(out);
        adjList.write(out);
        isNode.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        nodeID.readFields(in);
        pageRankValue.readFields(in);
        adjList.readFields(in);
        isNode.readFields(in);
    }
}