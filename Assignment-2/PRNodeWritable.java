import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PRNodeWritable implements Writable {
    private double pageRank;
    private List<String> adjacencyList;

    public PRNodeWritable() {
        this.adjacencyList = new ArrayList<>();
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public List<String> getAdjacencyList() {
        return adjacencyList;
    }

    public void setAdjacencyList(List<String> adjacencyList) {
        this.adjacencyList = adjacencyList;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(pageRank);
        out.writeInt(adjacencyList.size());
        for (String node : adjacencyList) {
            out.writeUTF(node);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pageRank = in.readDouble();
        int size = in.readInt();
        adjacencyList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            adjacencyList.add(in.readUTF());
        }
    }
}