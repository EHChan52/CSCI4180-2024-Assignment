import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class Indexing {
    // private Map<String, Chunk> chunkMap; // Maps checksum to chunk
    // private Map<String, List<Container>> containerMap; // Maps container ID to container
    // private Map<String, FileRecipe> fileRecipes; // Maps filename to list of chunk checksums
    // private Map<String, Integer> chunkReferences; // Maps checksum to reference count
    // private long nextContainerID;
    private static Set<Chunk> containersMarkedForDeletion;
    private static final String INDEX_FILE = "mydedup.index";
    private static final String RECIPE_DIR = "recipes";
    private static final String DATA_DIR = "data";

    // public Indexing() {
    //     chunkMap = new HashMap<>();
    //     containerMap = new HashMap<>();
    //     fileRecipes = new HashMap<>();
    //     chunkReferences = new HashMap<>();
    //     containersMarkedForDeletion = new HashSet<>();
    //     nextContainerID = 0;
    //     initializeDirectories();
    //     loadIndex();
    // }

    // private void initializeDirectories() {
    //     new File(RECIPE_DIR).mkdirs();
    //     new File(DATA_DIR).mkdirs();
    // }

    public static ArrayList<Chunk> loadIndex(File indexFile) {
        ArrayList<Chunk> chunkMetadata = new ArrayList<>();
        if (indexFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(indexFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    try {
                        Chunk chunk = new Chunk();
                        chunk.parseString(line);
            
                        chunkMetadata.add(chunk);
                    } catch (Exception e) {
                        System.err.println("Error parsing chunk entry: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                System.err.println("Error loading index: " + e.getMessage());
            }
            return chunkMetadata;
        } else {
            return null;
        }
    }

    public static void saveIndex(File indexFile, ArrayList<Chunk> chunks) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(indexFile))) {
            for (Chunk chunk : chunks) {
                writer.println(chunk.toString());
            }
        } catch (IOException e) {
            System.err.println("Error saving index: " + e.getMessage());
        }
    }

    public static Container loadContainer(File containerFile) throws IOException {
        try (DataInputStream dis = new DataInputStream(new FileInputStream(containerFile))) {
            // Read container metadata
            int containerID = dis.readInt();
            long size = dis.readLong();
            long maxSize = dis.readLong();

            // Create container
            Container container = new Container(containerID);
            container.setSize(size);

            // Read chunk data
            while (dis.available() > 0) {
                long chunkSize = dis.readLong(); // Read chunk size
                byte[] chunkData = new byte[(int) chunkSize];
                dis.readFully(chunkData); // Read raw chunk data

                Chunk chunk = new Chunk();
                chunk.setData(chunkData);
                container.getChunkContents().add(chunk);
            }

            return container;
        }
    }

    public static void saveContainer(File containerFile, Container container) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(containerFile))) {
            // Write container metadata
            dos.writeInt(container.getContainerID());
            dos.writeLong(container.getSize());
            dos.writeLong(container.maxSize);

            // Write chunk data
            for (Chunk chunk : container.getChunkContents()) {
                dos.writeLong(chunk.getSize()); // Write chunk size
                dos.write(chunk.getData());     // Write raw chunk data
            }
        }
    }

    public static FileRecipe loadRecipe(File recipeFile) {
        FileRecipe recipe = new FileRecipe();
        ArrayList<Chunk> chunkList = new ArrayList<>();
        if (recipeFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(recipeFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    try {
                        Chunk chunk = new Chunk();
                        chunk.parseString(line);
                        chunkList.add(chunk);
                    } catch (Exception e) {
                        System.err.println("Error parsing chunk entry: " + e.getMessage());
                    }
                }
            } catch (IOException e) {
                System.err.println("Error loading recipe: " + e.getMessage());
            }
            recipe.setFileName(recipeFile.getName().replace("recipe-", ""));
            recipe.setChunkList(chunkList);
            return recipe;
        }
        return null;
    }

    public static void saveRecipe(File recipeFile, FileRecipe recipe) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(recipeFile))) {
            for (Chunk chunk : recipe.getChunkList()) {
                writer.println(chunk.toString());
            }
        } catch (IOException e) {
            System.err.println("Error saving recipe: " + e.getMessage());
        }
    }

    //download file
    public static void downloadFile(String filename, String localFileName, ArrayList<Chunk> chunkMetadata) {
        
        FileRecipe recipe = .get(filename);
        if (recipe == null) {
            throw new IllegalArgumentException("File not found: " + filename);
        }

        // Create a new file and write the chunks to it
        try (FileOutputStream fos = new FileOutputStream(DATA_DIR + File.separator + filename)) {
            // for (String checksum : recipe) {
            //     Chunk storedChunk= chunkMap.get(checksum);
            //     if (storedChunk == null) {
            //         throw new IllegalArgumentException("Chunk not found: " + checksum);
            //     }
            //     Container container = findContainerForChunk(storedChunk);
            //     if (container != null) {
            //         byte[] data = readChunkFromContainer(container, storedChunk);
            //         fos.write(data);
            //     } else {
            //         throw new IOException("Container not found for chunk: " + checksum);
            //     }
            // }
            for (Chunk chunk : recipe.getChunkList()) {
                if (chunk != null) {
                    long currentContainerID = chunk.getContainerID();
                    byte[] data = readChunkFromContainer(currentContainerID, chunk);
                    fos.write(data);
                } else {
                    throw new IOException("Container not found for chunk: " + chunk);
                }
            }
        } catch (IOException e) {
            System.err.println("Error downloading file: " + e.getMessage());
        }
    }

    private byte[] readChunkFromContainer(long containerID, Chunk chunk) throws IOException {
        Container container = 
        try (RandomAccessFile raf = new RandomAccessFile(DATA_DIR + File.separator + container.getContainerID(), "r")) {
           for  
            raf.seek(chunk.getAddress());
            byte[] data = new byte[(int) chunk.getSize()];
            raf.readFully(data);
            return data;
        }
    }

    // end of download file

    //delete file
    public static void deleteFile(String filename, ArrayList<Chunk> chunkMetadata) {
        FileRecipe recipe = loadRecipe(new File(filename));
        if (recipe == null) {
            throw new IllegalArgumentException("File not found: " + filename);
        }

        ArrayList<Chunk> chunkList = recipe.getChunkList();

        for (Chunk chunk : chunkList) {
            chunk.decrementReferenceCount();
            if (chunk.getReferenceCount() == 0) {
                removeChunkFromIndex(chunk);
            }
        }
        // Remove file recipe
        // chunkMetadata.removeIf(chunk -> chunkList.contains(chunk));
        new File(RECIPE_DIR + File.separator + filename + ".recipe").delete();

        boolean hasValidChunks = chunkList.stream().anyMatch(chunk -> chunk.getReferenceCount() > 0);
        if (!hasValidChunks) {
            long containerID = chunkList.get(0).getContainerID();
            // Delete the container file from the storage
            File containerFile = new File(DATA_DIR + File.separator + containerID);
            if (containerFile.exists() && containerFile.delete()) {
                System.out.println("Container file deleted: " + containerID);
            } else {
                System.err.println("Failed to delete container file: " + containerID);
            }
            // Update the index file to remove the container entry
            removeContainerFromIndex(containerID);
        }

        
        
        
        // Save updated index
        saveIndex(new File(INDEX_FILE), chunkMetadata);
    }

    //testing
    private static void removeChunkFromIndex(Chunk chunk) {
        File indexFile = new File(INDEX_FILE);
        File tempFile = new File(INDEX_FILE + ".tmp");

        try (BufferedReader reader = new BufferedReader(new FileReader(indexFile));
             PrintWriter writer = new PrintWriter(new FileWriter(tempFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.contains(chunk.toString())) {
                    writer.println(line);
                }
            }
        } catch (IOException e) {
            System.err.println("Error updating index: " + e.getMessage());
        }

        if (!indexFile.delete()) {
            System.err.println("Could not delete original index file");
        }
        if (!tempFile.renameTo(indexFile)) {
            System.err.println("Could not rename temporary index file");
        }
    }

    private static void removeContainerFromIndex(long containerID) {
        File indexFile = new File(INDEX_FILE);
        File tempFile = new File(INDEX_FILE + ".tmp");

        try (BufferedReader reader = new BufferedReader(new FileReader(indexFile));
             PrintWriter writer = new PrintWriter(new FileWriter(tempFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.contains("container-" + containerID)) {
                    writer.println(line);
                }
            }
        } catch (IOException e) {
            System.err.println("Error updating index: " + e.getMessage());
        }

        if (!indexFile.delete()) {
            System.err.println("Could not delete original index file");
        }
        if (!tempFile.renameTo(indexFile)) {
            System.err.println("Could not rename temporary index file");
        }
    }

    //end of testing

    // private void markContainerForDeletion(Chunk chunk) {
    //     // Find and mark containers that might be eligible for deletion
    //     for (List<Container> containers : containerMap.values()) {
    //         for (Container container : containers) {
    //             ArrayList<Chunk> currentChunkContents = container.getChunkContents();
    //             if (currentChunkContents.contains(chunk)) {
    //                 boolean allChunksUnreferenced = currentChunkContents.stream()
    //                     .allMatch(c -> chunkReferences.getOrDefault(bytesToHex(c.getChecksum()), 0) == 0);
    //                 // .allMatch(c -> !chunkMap.containsKey(bytesToHex(c.getChecksum())));
    //                 container.setSafeToDelete(allChunksUnreferenced);
    //                 if(allChunksUnreferenced) {
    //                     containersMarkedForDeletion.add(String.valueOf(container.getContainerID()));
    //                 }
    //             }
    //         }
    //     }
    // }

    public static void deleteContainer(long containerID) {
        // Check if the container exists in the containerMap
        Container container = loadContainer(containerID);
        if (container == null) {
            System.err.println("Container not found: " + containerID);
            return;
        }

        // Verify if the container has no valid chunks
        ArrayList<Chunk> chunkList = container.getChunkContents();
        boolean hasValidChunks = chunkList.stream().anyMatch(chunk -> chunk.getReferenceCount() > 0);

        if (!hasValidChunks) {
            // Delete the container file from the storage
            File containerFile = new File(DATA_DIR + File.separator + containerID);
            if (containerFile.exists() && containerFile.delete()) {
                System.out.println("Container file deleted: " + containerID);
            } else {
                System.err.println("Failed to delete container file: " + containerID);
            }

            // Remove the container entry from the containerMap
            containerMap.remove(containerID);

            // Update the index file to remove the container entry
            updateIndexFile(containerID);
        } else {
            System.out.println("Container has valid chunks and cannot be deleted: " + containerID);
        }
    }

    private void saveFileRecipe(String filename, List<String> recipe) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(RECIPE_DIR + File.separator + filename + ".recipe"))) {
            for (String checksum : recipe) {
                writer.println(checksum);
            }
        } catch (IOException e) {
            System.err.println("Error saving file recipe: " + e.getMessage());
        }
    }

    public void printStatistics() {
        System.out.println("Total files: " + fileRecipes.size());
        System.out.println("Unique chunks: " + chunkMap.size());
        System.out.println("Total containers: " + containerMap.values().stream().mapToInt(List::size).sum());
        
        long totalBytes = chunkMap.values().stream().mapToLong(Chunk::getSize).sum();
        System.out.println("Total unique bytes: " + totalBytes);
        
        double deduplicationRatio = calculateDeduplicationRatio();
        System.out.printf("Deduplication ratio: %.2f%n", deduplicationRatio);
    }

    // Helper methods
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private double calculateDeduplicationRatio() {
        long totalPreDedupBytes = fileRecipes.values().stream()
            .flatMap(List::stream)
            .mapToLong(checksum -> chunkMap.get(checksum).getSize())
            .sum();
        long uniqueBytes = chunkMap.values().stream()
            .mapToLong(Chunk::getSize)
            .sum();
        return totalPreDedupBytes > 0 ? (double) totalPreDedupBytes / uniqueBytes : 1.0;
    }

    private void parseContainerEntry(String entry) {
        try {
            Container container = new Container();
            container.parseString(entry);

            containerMap.computeIfAbsent(String.valueOf(container.containerID), k -> new ArrayList<>())
                       .add(container);
            nextContainerID = Math.max(nextContainerID, container.containerID + 1);
        } catch (Exception e) {
            System.err.println("Error parsing container entry: " + e.getMessage());
        }
    }

    public String serializeChunkEntry(Chunk chunk) {
        return chunk.toString();
    }

    public String serializeContainer(Container container) {
        return container.toString();
    }

    private byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                 + Character.digit(hex.charAt(i+1), 16));
        }
        return data;
    }

    // Add these utility methods to support file operations
    public long getNextContainerID() {
        return nextContainerID;
    }

    public void incrementNextContainerID() {
        nextContainerID++;
    }

    public boolean chunkExists(String checksum) {
        return chunkMap.containsKey(checksum);
    }

    public Chunk getChunk(String checksum) {
        return chunkMap.get(checksum);
    }
}
