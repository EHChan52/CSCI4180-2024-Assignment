import java.util.ArrayList;
import java.util.List;  

public class FileRecipe {

    public String fileName;
    public ArrayList<Chunk> chunkList = new ArrayList<Chunk>();

    public FileRecipe() {}

    public FileRecipe(String fileName, ArrayList<Chunk> chunkList) {
        this.fileName = fileName;
        this.chunkList = chunkList;
    }

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
        String chunkListString = "";
        for (Chunk chunk : chunkList) {
            chunkListString += chunk.toString() + "\n";
        }
        return chunkListString;
    }
}