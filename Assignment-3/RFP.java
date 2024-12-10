import java.io.File;
import java.io.IOException;

public class RFP {  
    public int[] generateFingerprints(int windowSize, int modulus, File fileToUpload) {
        System.out.println("Hello from"+ fileToUpload);
        int d = 257;
        byte[] fileContent = null;
        try {
            fileContent = java.nio.file.Files.readAllBytes(fileToUpload.toPath());
        } catch (IOException e) {
        }
        
        if (fileContent != null) {
            int[] fingerprint = new int[fileContent.length - windowSize + 1];
            fingerprint[0] = 0;
            for(int i = 1; i <= windowSize; i++){
                fingerprint[0] = (int) (fingerprint[0] + (((int)fileContent[i - 1]* Math.pow(d, windowSize - i)) % modulus));
            }
            fingerprint[0] = fingerprint[0] % modulus;
            for (int i = 1; i <= fileContent.length - windowSize; i++) {
                //binary content debug
                //System.out.println(fileContent[i]);
                fingerprint[i] = (d * (fingerprint[i - 1] - (int)(Math.pow(d, windowSize - 1) % modulus) * (int)fileContent[i]) + (int)fileContent[i + windowSize - 1]) % modulus;
                if (fingerprint[i] < 0) {
                    fingerprint[i] += modulus;
                }
            }

            //printout all fingerprints
            /* 
            for(int i = 0; i < fingerprint.length; i++){
                System.out.println(fingerprint[i]);
            }
            */
            
            return fingerprint;
        }
        else{
            return new int[0];
        }
    }
}
