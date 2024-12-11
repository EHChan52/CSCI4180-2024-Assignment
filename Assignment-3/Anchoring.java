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

        //not checked
        for (int i = 0; i < fingerprint.length - windowSize; i++) {
            for (int j = 0; j < windowSize; j++) {
                RFP[i] = RFP[i] + (fingerprint[i+j] * (int) Math.pow(10, windowSize - j - 1));
            }
            RFP[i] = RFP[i] % avgChunkSize;
            System.out.println("RFP[" + i + "] = " + RFP[i]);
            
            if (chunkLength == maxChunkSize - windowSize + 1) {
                anchor[anchorCount] = i + windowSize - 1;
                anchorCount++;
                chunkLength = 0;
                i = i + windowSize - 1;
            }
            else if ((RFP[i] & mask) == 0) {
                anchor[anchorCount] = i + windowSize - 1;
                anchorCount++;
                i = i + windowSize - 1;
            }
            else {
                chunkLength++;               
            }
        }

        /* debug
        System.out.println("Anchor count: " + anchorCount);
        System.out.println("Anchors: ");
        for (int i = 0; i < anchorCount; i++) {
            System.out.print(anchor[i] + " ");
        }
        System.out.println();
        */

        return anchor;
    }
}
