import java.io.File;
import java.io.IOException;

public class RFP {
    public void generateChunks(int windowSize, int modulus, int maxSize, File fileToUpload) {
        System.out.println("Hello from"+ fileToUpload);
        int d = 257;
        int[] fingerprint = new int[windowSize];
        
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
        //binary content debug
        if (fileContent != null) {
            for (int i = 0; i < 1; i++) {
            System.out.println(fileContent[i]);
            }
        }


        //chunks.set(0, fileContent[0]);
            
        /* 
        for(int i = 1; i <= windowSize; i++){

        }
        chunks.get(0).fingerprint =  chunks.get(0).fingerprint % modulus;
    */
    }
}
