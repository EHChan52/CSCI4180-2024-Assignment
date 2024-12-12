import java.util.ArrayList;

public class WrapToContainer {
    private static final long CONTAINER_SIZE = 1024 * 1024;
    Integer id = 0;

    public ArrayList<Container> createContainers(ArrayList<Chunk> chunksList) {
        ArrayList<Container> containerList = new ArrayList<>();
        Container buffer = new Container(CONTAINER_SIZE, id);
        int currentOffset = 0;

        for (Chunk chunk : chunksList) {
            if (buffer.size + chunk.size < buffer.maxSize) {
                chunk.setContainerID(id);
                chunk.setOffset(currentOffset);
                currentOffset += chunk.getSize();

                buffer.addToContainer(chunk);
                buffer.setSize(buffer.getSize() + chunk.size);
            } else {
                containerList.add(buffer);
                id++;
                buffer = new Container(CONTAINER_SIZE, id);
                currentOffset = 0;

                chunk.setContainerID(id);
                chunk.setOffset(currentOffset);
                currentOffset += chunk.getSize();

                buffer.addToContainer(chunk);
                buffer.setSize(chunk.size);
            }
        }

        if (!buffer.chunkContents.isEmpty()) {
            containerList.add(buffer);
        }

        return containerList;
    }
}