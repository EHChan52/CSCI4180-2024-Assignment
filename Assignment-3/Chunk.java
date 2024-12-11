import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class Chunk {
    public long chunkAddress = 0x00;
    public byte[] checksum;
    public int size = 0;
    public byte[] datas = new byte[0];
    public int referenceCount = 0;

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    public long getChunkAddress() {
        return chunkAddress;
    }

    public void setChunkAddress(long address) {
        this.chunkAddress = address;
    }

    public byte[] getData() {
        return datas;
    }

    public void setData(byte data) throws IOException{
        baos.write(data);
        this.datas = baos.toByteArray();
        this.size += 1;
    }

    public byte[] getChecksum() {
        return checksum;
    }

    public void setChecksum(byte[] checksum) {
        this.checksum = checksum;
    }

    public void incrementReferenceCount() {
        referenceCount++;
    }

    public void decrementReferenceCount() {
        if (referenceCount > 0) {
            referenceCount--;
        }
    }

    @Override
    public String toString() {
        return "Chunk{" +
                "chunkAddress=" + chunkAddress +
                ", checksum=" + Arrays.toString(checksum) +
                ", size=" + size +
                ", referenceCount=" + referenceCount +
                '}';
    }
}
