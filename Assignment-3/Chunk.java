import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Chunk {
    //This part is for metadata in mydedup.index

    // public long chunkAddress = 0x00000000;
    public Integer containerID;
    public Integer offset = 0;
    public byte[] checksum;
    public long size = 0;
    public byte[] datas = new byte[0];
    public long referenceCount = 0;

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // public long getChunkAddress() {
    //     return chunkAddress;
    // }

    // public void setChunkAddress(long address) {
    //     this.chunkAddress = address;
    // }

    public Integer getContainerID() {
        return containerID;
    }

    public void setContainerID(Integer id) {
        this.containerID = id;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public byte[] getData() {
        return datas;
    }

    public void setData(byte data) throws IOException {
        baos.write(data);
        this.datas = baos.toByteArray();
        this.size += 1;
    }

    public void setData(byte[] data) throws IOException {
        baos.write(data);
        this.datas = baos.toByteArray();
        this.size += data.length;
    }

    public byte[] getChecksum() {
        return checksum;
    }

    public void setChecksum(byte[] checksum) {
        this.checksum = checksum;
    }

    public long getSize() {
        return size;
    }

    public long getReferenceCount() {
        return referenceCount;
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
                "containerID=" + containerID +
                ", offset=" + offset +
                ", checksum=" + Arrays.toString(checksum) +
                ", size=" + size +
                ", referenceCount=" + referenceCount +
                '}';
    }

    public void parseString(String entry) {
        // Format: Chunk{chunkAddress=address, checksum=[checksum], size=size, referenceCount=referenceCount}
        if (entry.startsWith("Chunk")) {
            try {
                Map<String, String> chunkMap = new HashMap<>();
                String[] parts = entry.replace("Chunk{", "").replace("}", "").split(", ");
                for (String part : parts) {
                    String[] keyValue = part.split("=");
                    chunkMap.put(keyValue[0], keyValue[1]);
                }

                // this.chunkAddress = Long.parseLong(chunkMap.get("chunkAddress"));
                this.containerID = Integer.parseInt(chunkMap.get("containerID"));
                this.offset = Integer.parseInt(chunkMap.get("offset"));
                this.size = Long.parseLong(chunkMap.get("size"));
                this.referenceCount = Integer.parseInt(chunkMap.get("referenceCount"));
                
                String checksumStr = chunkMap.get("checksum").replace("[", "").replace("]", "");
                String[] byteValues = checksumStr.split(", ");
                byte[] checksumValue = new byte[byteValues.length];
                
                for (int i = 0; i < byteValues.length; i++) {
                    checksumValue[i] = Byte.parseByte(byteValues[i]);
                }
                this.checksum = checksumValue;
            } catch (Exception e) {
                System.err.println("Error parsing chunk entry: " + e.getMessage());
            }
        } else {
            System.err.println("Invalid entry format: " + entry);
        }
    }
}