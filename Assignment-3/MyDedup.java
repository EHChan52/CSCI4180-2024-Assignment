import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

class MyDedup {
    private static boolean isPowerOfTwo(int n) {
        return (n > 0) && ((n & (n - 1)) == 0);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }


    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        ArrayList<Chunk> chunkMetadata = new ArrayList<>();
        ArrayList<Container> containerList = new ArrayList<>();
        ArrayList<FileRecipe> recipeList = new ArrayList<>();
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
            chunkMetadata = Indexing.loadIndex(indexFile);
        }

        // Create data folder if it does not exist
        File dataFolder = new File("data");
        if (!dataFolder.exists()) {
            if (dataFolder.mkdir()) {
                System.out.println("data folder created.");
            } else {
                System.out.println("Failed to create data folder.");
            }
        } else {
            System.out.println("data folder already exists.");
            File[] containerFiles = dataFolder.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.startsWith("container-");
                }
            });
            if (containerFiles != null) {
                for (File file : containerFiles) {
                    System.out.println("Found container file: " + file.getName());
                    containerList.add(Indexing.loadContainer(file));
                }
            } else {
                System.out.println("No container files found.");
            }

            File[] recipeFiles = dataFolder.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.startsWith("recipe-");
                }
            });
            if (recipeFiles != null) {
                for (File file : recipeFiles) {
                    System.out.println("Found recipe file: " + file.getName());
                    recipeList.add(Indexing.loadRecipe(file));
                }
            } else {
                System.out.println("No recipe files found.");
            }
        }

        if("upload".equals(args[0]) && args.length == 5){
            try {
                int minChunkSize = Integer.parseInt(args[1]); //windowsize
                int avgChunkSize = Integer.parseInt(args[2]); //modulas
                int maxChunkSize = Integer.parseInt(args[3]); 

                if (!isPowerOfTwo(minChunkSize) || !isPowerOfTwo(avgChunkSize) || !isPowerOfTwo(maxChunkSize)) {
                    System.out.println("Error: min_chunk, avg_chunk and max_chunk must be powers of 2.");
                } else {
                    //file not exist, error;
                    File fileToUpload = new File(args[4]);
                    if (!fileToUpload.exists()) {
                        System.out.println("Error: File " + args[4] + " does not exist.");
                    } 
                    //if file is null, error;
                    else if (fileToUpload.length() == 0) {
                        System.out.println("Error: File " + args[4] + " is empty.");
                    }
                    else {
                        System.out.println("File " + args[4] + " exists. Proceeding with upload.");
                        //Check the path of the file is already exist or not
                        if (Files.exists(Paths.get("./data/recipe-" + args[4]))) {
                            System.out.println("Recipe file for " + args[4] + " already exists in /data folder. Exiting.");
                            System.exit(1);
                        }

                        byte[] fileContent = null;
                        try {
                            fileContent = java.nio.file.Files.readAllBytes(fileToUpload.toPath());
                        } catch (IOException e) {}

                        RFP rfp = new RFP();
                        int[] fingerprints = rfp.generateFingerprints(minChunkSize, avgChunkSize, fileContent);
                        //print out the contents of fingerprints

                        Anchoring anchoring = new Anchoring();
                        int[] anchors = anchoring.generateAnchors(fingerprints, minChunkSize, avgChunkSize, maxChunkSize);
                        //print out the contents of anchors
                        /*
                        for (int anchor : anchors) {
                            System.out.println(anchor);
                        }*/
                        Chunking chunker = new Chunking();
                        ArrayList<Chunk> chunksList = chunker.generateChunks(fileContent, anchors, anchors.length);
                        /* 
                        for (Chunk chunk : chunksList) {
                            System.out.println(chunk);
                        }*/
                        WrapToContainer containers = new WrapToContainer();
                        containerList = containers.createContainers(chunksList);
                        /*
                        for (Container container : containerList) {
                            System.out.println(container);
                        }*/

                        FileRecipe recipe = new FileRecipe(fileToUpload.getName(), chunksList);

                        ArrayList<String> checksums = new ArrayList<>();
                        for (Chunk chunk : chunksList) {
                            checksums.add(bytesToHex(chunk.getChecksum()));
                        }

                        try (java.io.FileWriter checksumWriter = new java.io.FileWriter("./temp/file_recipe.tmp")) {
                            for (String checksum : checksums) {
                                checksumWriter.write(checksum + System.lineSeparator());
                            }
                            System.out.println("checksums.tmp file created with checksums.");
                        } catch (IOException e) {
                            System.out.println("An error occurred while writing to checksums.tmp file.");
                        }



                        try (java.io.FileWriter writer = new java.io.FileWriter("./temp/containerList.tmp")) {
                            for (Container container : containerList) {
                                writer.write(container.toString() + System.lineSeparator());
                            }
                            System.out.println("containerList.txt file created with container contents.");
                        } catch (IOException e) {
                            System.out.println("An error occurred while writing to containerList.txt file.");
                        }

                        Indexing.saveIndex(indexFile, chunkMetadata);
                        for (Container container : containerList) {
                            File containerFile = new File("data/container-" + container.getContainerID() + ".bin");
                            Indexing.saveContainer(containerFile, container);
                        }

                        File recipeFile = new File("data/recipe-" + fileToUpload.getName() + ".txt");
                        Indexing.saveRecipe(recipeFile, recipe);

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

            String fileToDownload = new String(args[1]);
            String localFileName = new String(args[2]);
            if (!fileToDownload.isEmpty()) {
                System.out.println("Error: File " + args[4] + " is empty.");
            } 
            else {
                System.out.println("File " + args[4] + " exists. Proceeding with upload.");
                Indexing.downloadFile(fileToDownload, localFileName, chunkMetadata);
                
            }



        }
        
        else if("download".equals(args[0]) && (args.length != 3)){
            System.out.println("Usage: java MyDedup download <file_to_download> <local_file_name>");
        }
        
        else if("delete".equals(args[0]) && (args.length == 2)){
            System.out.println("delete");
            String fileToDelete = new String(args[1]);
            if (!fileToDelete.isEmpty()) {
                System.out.println("Error: File " + args[4] + " is empty.");
            } 
            else {
                System.out.println("File " + args[4] + " exists. Proceeding with upload.");
                Indexing.deleteFile(fileToDelete, chunkMetadata);
                
            }


        }

        else if("delete".equals(args[0]) && (args.length != 2)){
            System.out.println("Usage: java MyDedup delete <file_to_delete>");
        }

        else{
            System.out.println("Invalid command");
        }
    }
}

