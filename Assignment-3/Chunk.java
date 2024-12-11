public class Chunk {
    public long chunkAddress = 0x00;
    public long fingerprint;
    public int size = 0;
    public byte[] data = new byte[size];
    public int referenceCount = 0;
}
