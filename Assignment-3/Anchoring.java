public class Anchoring {
    public int[] generateAnchors(int[] fingerprint, int windowSize, int avgChunkSize) {
        int[] anchor = new int[fingerprint.length];
        int mask = avgChunkSize - 1;
        int anchorCount = 0;

        for (int i = 0; i < fingerprint.length; i++) {
            if ((fingerprint[i] & mask) == 0) {
                anchor[anchorCount] = i;
                anchorCount++;
            }
        }

        System.out.println("Anchor count: " + anchorCount);
        System.out.println("Anchors: ");
        for (int i = 0; i < anchorCount; i++) {
            System.out.print(anchor[i] + " ");
        }
        System.out.println();


        return new int[0];
    }
}
