import java.util.ArrayList;

public class Container {
    public long containerID;
    public long size;
    public ArrayList<Chunk> chunkContents = new ArrayList<>();
    public boolean safeToDelete = false;

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
}
