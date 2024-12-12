import java.util.ArrayList;
import java.util.List;  

public class Recipe {
    // Inner class to represent a chunk entry
    public static class ChunkEntry {
        private String chunkAddress;
        private String containerID;

        public ChunkEntry(String chunkAddress, String containerID) {
            this.chunkAddress = chunkAddress;
            this.containerID = containerID;
        }

        public String getChunkAddress() {
            return chunkAddress;
        }

        public void setChunkAddress(String chunkAddress) {
            this.chunkAddress = chunkAddress;
        }

        public String getContainerID() {
            return containerID;
        }

        public void setContainerID(String containerID) {
            this.containerID = containerID;
        }
    }

    // List to store chunk entries
    private List<ChunkEntry> chunkEntries;

    public Recipe() {
        this.chunkEntries = new ArrayList<>();
    }

    public void addChunkEntry(String chunkAddress, String containerID) {
        this.chunkEntries.add(new ChunkEntry(chunkAddress, containerID));
    }

    public List<ChunkEntry> getChunkEntries() {
        return chunkEntries;
    }
}