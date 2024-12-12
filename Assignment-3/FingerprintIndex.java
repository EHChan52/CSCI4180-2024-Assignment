import java.util.HashMap;
import java.util.Map;

public class FingerprintIndex {
    private Map<Integer, Chunk> fingerprintIndex;

    public FingerprintIndex() {
        fingerprintIndex = new HashMap<>();
    }

    public void addChunk(int fingerprint, Chunk chunk) {
        fingerprintIndex.put(fingerprint, chunk);
    }

    public boolean chunkExists(int fingerprint) {
        return fingerprintIndex.containsKey(fingerprint);
    }

    public Chunk getChunk(int fingerprint) {
        return fingerprintIndex.get(fingerprint);
    }
}