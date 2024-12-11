import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class Chunking {
    public Chunk[] generateChunks(byte[] data, int[] anchors, int chunkCount) throws IOException, NoSuchAlgorithmException{
        Chunk[] chunkList = new Chunk[chunkCount + 1];
        int chunkCounter = 0;
        System.out.println("chunkCount: "+chunkCount);
        for (int i = 0; i <= chunkCount; i++) {
            chunkList[i] = new Chunk();
        }

        for(int i = 0; i < data.length; i++){
            if(chunkCounter < anchors.length && i == anchors[chunkCounter] + 1){
                MessageDigest md = MessageDigest.getInstance("MD5");
                int len = i - (chunkCounter == 0 ? 0 : anchors[chunkCounter - 1] + 1);
                md.update(data, chunkCounter == 0 ? 0 : anchors[chunkCounter - 1] + 1, len);
                byte[] checksumBytes = md.digest();
                chunkList[chunkCounter].setChecksum(checksumBytes);
                System.out.println(chunkList[chunkCounter]);
                chunkCounter++;
            }
            else{
                chunkList[chunkCounter].setData(data[i]);
            }
        }


        return chunkList;
    }
}
