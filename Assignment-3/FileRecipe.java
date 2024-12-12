import java.util.ArrayList;
import java.util.List;  

public class FileRecipe {

    public String fileName;
    public ArrayList<Chunk> chunkList = new ArrayList<Chunk>();

    public void setChunkList(ArrayList<Chunk> chunkList) {
        this.chunkList = chunkList;
    }

    public ArrayList<Chunk> getChunkList() {
        return chunkList;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileName() {
        return fileName;
    }
    
    @Override
    public String toString() {
        return "FileRecipe{" +
                "fileName='" + fileName + '\'' +
                ", chunkList=" + chunkList +
                '}';
    }
}