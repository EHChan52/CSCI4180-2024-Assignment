import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Container {
    public long containerID;
    public long size;
    public long maxSize;
    public ArrayList<Chunk> chunkContents = new ArrayList<Chunk>();
    public boolean safeToDelete = false;

    public Container() {}

    public Container(long containerSize, long containerID) {
        this.containerID = containerID;
        this.maxSize = containerSize;
    }

    public long getContainerID() {
        return containerID;
    }

    public void setContainerID(long id) {
        this.containerID = id;
    }

    public boolean getSafeToDelete() {
        return safeToDelete;
    }

    public void setSafeToDelete(boolean status) {
        this.safeToDelete = status;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void addToContainer(Chunk chunk1){
        chunkContents.add(chunk1);
    }

    public void removeFromContainer(Chunk chunk1) {
        chunkContents.remove(chunk1);
    }

    public void setChunkContents(ArrayList<Chunk> chunkContents) {
        this.chunkContents = chunkContents;
    }

    public ArrayList<Chunk> getChunkContents() {
        return this.chunkContents;
    }

    @Override
    public String toString() {
        //All raw data in the container
        StringBuilder chunkDataStr = new StringBuilder();
        for (Chunk chunk : chunkContents) {
            chunkDataStr.append(new String(chunk.getData(), StandardCharsets.UTF_8));
        }
    
        return chunkDataStr.toString();
    }

    public void parseString(String entry) {
        // Format: Container{containerID=id, size=size, maxSize=maxSize, chunkContents=[chunks], safeToDelete=safeToDelete}
        try {
            Map<String, String> containerMap = new HashMap<>();
            String[] parts = entry.replace("Container{", "").replace("}", "").split(", ");
            for (String part : parts) {
                String[] keyValue = part.split("=");
                containerMap.put(keyValue[0], keyValue[1]);
            }

            this.containerID = Long.parseLong(containerMap.get("containerID"));
            this.size = Long.parseLong(containerMap.get("size"));
            this.maxSize = Long.parseLong(containerMap.get("maxSize"));
            this.safeToDelete = Boolean.parseBoolean(containerMap.get("safeToDelete"));
            
            String chunkContentsStr = containerMap.get("chunkContents").replace("[", "").replace("]", "");
            String[] chunkEntries = chunkContentsStr.split(", ");
            for (String chunkEntry : chunkEntries) {
                if (!chunkEntry.isEmpty()) {
                    Chunk chunk = new Chunk();
                    chunk.parseString(chunkEntry);
                    this.chunkContents.add(chunk);
                }
            }
        } catch (Exception e) {
            System.err.println("Error parsing container entry: " + e.getMessage());
        }
    }
}
