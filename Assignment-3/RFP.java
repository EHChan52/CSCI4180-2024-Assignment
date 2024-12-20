
public class RFP {  
    public int[] generateFingerprints(int windowSize, int modulus, byte[] fileContent) {
        int d = 257;
        
        if (fileContent != null) {
            int[] fingerprint = new int[fileContent.length - windowSize + 1];
            fingerprint[0] = 0;
            for(int i = 1; i <= windowSize; i++){
                fingerprint[0] = (int) (fingerprint[0] + (((int)fileContent[i - 1]* Math.pow(d, windowSize - i)) % modulus));
            }
            fingerprint[0] = fingerprint[0] % modulus;
            for (int i = 1; i <= fileContent.length - windowSize; i++) {

                //have bug(have not check)
                fingerprint[i] = (d * (fingerprint[i - 1] - (int)(Math.pow(d, windowSize - 1) % modulus) * (int)fileContent[i]) + (int)fileContent[i + windowSize - 1]) % modulus;
                if (fingerprint[i] < 0) {
                    fingerprint[i] += modulus;
                }
            }
            return fingerprint;
        }
        //will not trigger
        else{
            return new int[0];
        }
    }
}
