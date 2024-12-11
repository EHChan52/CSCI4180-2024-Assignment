

public class Anchoring {
    public int[] generateAnchors(int[] fingerprint, int windowSize, int avgChunkSize, int maxChunkSize) {
        int[] anchor = new int[fingerprint.length];
        int[] RFP = new int[fingerprint.length];
        int mask = avgChunkSize - 1;
        int anchorCount = 0;
        int chunkLength = 0;
        //intialize RFP to all 0
        for (int i = 0; i < fingerprint.length; i++) {
            RFP[i] = 0;
        }
        System.out.println(fingerprint.length - windowSize);
        //not checked
        for (int i = 0; i < fingerprint.length - windowSize; i++) {
            for (int j = 0; j < windowSize; j++) {
                RFP[i] = RFP[i] + (fingerprint[i+j] * (int) Math.pow(10, windowSize - j - 1));
            }
            RFP[i] = RFP[i] % avgChunkSize;
            System.out.println("RFP[" + i + "] = " + RFP[i]);

            if (i + windowSize >= fingerprint.length) {
                break;
            }

            if ((chunkLength == maxChunkSize - windowSize) && (i + windowSize < fingerprint.length - windowSize)) {
                anchor[anchorCount] = i + windowSize - 1;
                anchorCount++;
                chunkLength = 0;
                i = i + windowSize - 1;
            }
            else if (((RFP[i] & mask) == 0) && (i + windowSize < fingerprint.length - windowSize)) {
                anchor[anchorCount] = i + windowSize - 1;
                anchorCount++;
                i = i + windowSize - 1;
            }
            else {
                chunkLength++;               
            }
        }

        //shrink the size of anchor array to length of anchorCount
        int[] result = new int[anchorCount];
        System.arraycopy(anchor, 0, result, 0, anchorCount);
        return result;
    }
}
