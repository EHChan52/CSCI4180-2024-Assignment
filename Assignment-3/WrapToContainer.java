import java.util.ArrayList;

public class WrapToContainer {
    private static final long CONTAINER_SIZE = 1024 * 1024;
    
    public ArrayList<Container> createContainers(Chunk[] chunklist) {
        ArrayList<Container> containerList = new ArrayList<>();
        Container buffer = new Container(CONTAINER_SIZE);
        for (Chunk chunk : chunklist) {
            // If container current size + size of current chunk is smaller than CONTAINER_SIZE
            if (buffer.size + chunk.size < buffer.maxSize) {
                buffer.addToContainer(chunk);
                buffer.setSize(buffer.getSize() + chunk.size);
            } else {
                // Add the full buffer to the container list
                containerList.add(buffer);
                // Create a new buffer and add the current chunk to it
                buffer = new Container(CONTAINER_SIZE);
                buffer.addToContainer(chunk);
                buffer.setSize(chunk.size);
            }
        }
        // Add the last buffer to the container list if it has any chunks
        if (!buffer.chunkContents.isEmpty()) {
            containerList.add(buffer);
        }
        return containerList;
    }
}