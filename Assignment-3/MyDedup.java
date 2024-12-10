import java.io.File;
import java.io.IOException;

class MyDedup {
    private static boolean isPowerOfTwo(int n) {
        return (n > 0) && ((n & (n - 1)) == 0);
    }


    public static void main(String[] args) {

        // Create mydedup.data file if it does not exist
        File indexFile = new File("mydedup.index");
        if (!indexFile.exists()) {
            try {
                if (indexFile.createNewFile()) {
                    System.out.println("mydedup.index file created.");
                } else {
                    System.out.println("Failed to create mydedup.index file.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred while creating mydedup.index file.");
            }
        } else {
            System.out.println("mydedup.index file already exists.");
        }

        if("upload".equals(args[0]) && args.length == 5){
            try {
                int minChunkSize = Integer.parseInt(args[1]);
                int avgChunkSize = Integer.parseInt(args[2]);
                int maxChunkSize = Integer.parseInt(args[3]);

                if (!isPowerOfTwo(minChunkSize) || !isPowerOfTwo(avgChunkSize) || !isPowerOfTwo(maxChunkSize)) {
                    System.out.println("Error: min_chunk, avg_chunk and max_chunk must be powers of 2.");
                } else {
                    //file not exist, error;
                    File fileToUpload = new File(args[4]);
                    if (!fileToUpload.exists()) {
                        System.out.println("Error: File " + args[4] + " does not exist.");
                    } 
                    else {
                        System.out.println("File " + args[4] + " exists. Proceeding with upload.");
                        RFP rfp = new RFP();
                        rfp.generateChunks(minChunkSize,avgChunkSize,maxChunkSize,fileToUpload);
                    }
                }
            } catch (NumberFormatException e) {
                System.out.println("Error: min_chunk, avg_chunk, and max_chunk must be integers.");
            }
        }

        else if("upload".equals(args[0]) && (args.length != 5)){
            System.out.println("Usage: java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <file_to_upload>");
        }

        else if("download".equals(args[0]) && args.length == 3){
            System.out.println("download");

        }
        
        else if("download".equals(args[0]) && (args.length != 3)){
            System.out.println("Usage: java MyDedup download <file_to_download> <local_file_name>");
        }
        
        else if("delete".equals(args[0]) && (args.length == 2)){
            System.out.println("delete");
        }

        else if("delete".equals(args[0]) && (args.length != 2)){
            System.out.println("Usage: java MyDedup delete <file_to_delete>");
        }

        else{
            System.out.println("Invalid command");
        }
    }
}
