import java.util.ArrayList;
import java.util.List;

public class ChunkStorage {
    private static final int CONTAINER_SIZE = 1024 * 1024; // 1 MiB
    private List<Chunk> buffer;
    private List<Container> containers;
    private int currentContainerId;

    public ChunkStorage() {
        this.buffer = new ArrayList<>();
        this.containers = new ArrayList<>();
        this.currentContainerId = 0;
    }

    public void addChunk(Chunk chunk) {
        if (getBufferSize() + chunk.size > CONTAINER_SIZE) {
            flushBuffer();
        }
        buffer.add(chunk);
    }

    private int getBufferSize() {
        int size = 0;
        for (Chunk chunk : buffer) {
            size += chunk.size;
        }
        return size;
    }

    private void flushBuffer() {
        Container container = new Container(currentContainerId, new ArrayList<>(buffer));
        containers.add(container);
        buffer.clear();
        currentContainerId++;
    }

    public void finalizeStorage() {
        if (!buffer.isEmpty()) {
            flushBuffer();
        }
    }

    public Chunk getChunk(long chunkAddress) {
        for (Container container : containers) {
            for (Chunk chunk : container.getChunks()) {
                if (chunk.chunkAddress == chunkAddress) {
                    return chunk;
                }
            }
        }
        return null;
    }

    public List<Container> getAllContainers() {
        return new ArrayList<>(containers);
    }

    private static class Container {
        private int containerId;
        private List<Chunk> chunks;

        public Container(int containerId, List<Chunk> chunks) {
            this.containerId = containerId;
            this.chunks = chunks;
        }

        public int getContainerId() {
            return containerId;
        }

        public List<Chunk> getChunks() {
            return chunks;
        }
    }
}