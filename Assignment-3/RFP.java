import java.io.File;
import java.io.IOException;
import java.util.List;

public class RFP {
    public void generateChunks(int windowSize, int modulus, int maxSize, File fileToUpload) {
        System.out.println("Hello from"+ fileToUpload);
        List <Chunk> chunks = null;
        int d = 257;

        //copy
        File copy = 
        new File(fileToUpload.getParent(), "copy_" + fileToUpload.getName());
        try {
            java.nio.file.Files.copy(fileToUpload.toPath(), copy.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
        }
        
        byte[] fileContent = null;
        try {
            fileContent = java.nio.file.Files.readAllBytes(copy.toPath());
        } catch (IOException e) {
        }
        if (fileContent != null) {
            for (int i = 0; i < 1; i++) {
            System.out.print(fileContent[i]);
            }
        }
            
        /* 
        for(int i = 1; i <= windowSize; i++){

        }
        chunks.get(0).fingerprint =  chunks.get(0).fingerprint % modulus;
    */
    }
}
