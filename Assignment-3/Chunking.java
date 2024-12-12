import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

public class Chunking {
    public ArrayList<Chunk> generateChunks(byte[] data, int[] anchors, int chunkCount) throws IOException, NoSuchAlgorithmException {
        ArrayList<Chunk> chunkList = new ArrayList<>();
        int chunkCounter = 0;
        int currentOffset = 0;

        // Initialize the first chunk
        chunkList.add(new Chunk());

        for (int i = 0; i < data.length; i++) {
            if (chunkCounter < anchors.length && i == anchors[chunkCounter] + 1) {
                MessageDigest md = MessageDigest.getInstance("MD5");
                int len = i - (chunkCounter == 0 ? 0 : anchors[chunkCounter - 1] + 1);
                md.update(data, chunkCounter == 0 ? 0 : anchors[chunkCounter - 1] + 1, len);
                byte[] checksumBytes = md.digest();
                chunkList.get(chunkCounter).setChecksum(checksumBytes);
                chunkList.get(chunkCounter).setOffset(currentOffset);
                currentOffset += chunkList.get(chunkCounter).getSize();
                chunkCounter++;
                if (chunkCounter < chunkCount) {
                    chunkList.add(new Chunk());
                }
            } else {
                if (chunkCounter < chunkCount) {
                    chunkList.get(chunkCounter).setData(data[i]);
                }
            }
        }

        // Finalize the last chunk
        if (chunkCounter < chunkCount) {
            MessageDigest md = MessageDigest.getInstance("MD5");
            int len = data.length - (chunkCounter == 0 ? 0 : anchors[chunkCounter - 1] + 1);
            md.update(data, chunkCounter == 0 ? 0 : anchors[chunkCounter - 1] + 1, len);
            byte[] checksumBytes = md.digest();
            chunkList.get(chunkCounter).setChecksum(checksumBytes);
            chunkList.get(chunkCounter).setOffset(currentOffset);
        }

        return chunkList;
    }
}