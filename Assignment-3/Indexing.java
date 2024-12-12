import java.io.*;
import java.util.*;

public class Indexing {
    private Map<String, Chunk> chunkMap; // Maps checksum to chunk
    private Map<String, List<Container>> containerMap; // Maps container ID to container
    private Map<String, List<String>> fileRecipes; // Maps filename to list of chunk checksums
    private Map<String, Integer> chunkReferences; // Maps checksum to reference count
    private long nextContainerID;
    
    private static final String INDEX_FILE = "mydedup.index";
    private static final String RECIPE_DIR = "recipes";
    private static final String DATA_DIR = "data";

    public Indexing() {
        chunkMap = new HashMap<>();
        containerMap = new HashMap<>();
        fileRecipes = new HashMap<>();
        chunkReferences = new HashMap<>();
        nextContainerID = 0;
        initializeDirectories();
        loadIndex();
    }

    private void initializeDirectories() {
        new File(RECIPE_DIR).mkdirs();
        new File(DATA_DIR).mkdirs();
    }

    public void processNewFile(String filename, ArrayList<Chunk> chunks, ArrayList<Container> containers) {
        // Store file recipe
        List<String> recipe = new ArrayList<>();
        for (Chunk chunk : chunks) {
            String checksum = bytesToHex(chunk.getChecksum());
            recipe.add(checksum);
            
            if (!chunkMap.containsKey(checksum)) {
                // New unique chunk
                chunkMap.put(checksum, chunk);
                chunkReferences.put(checksum, 1);
            } else {
                // Existing chunk
                chunkReferences.merge(checksum, 1, Integer::sum);
            }
        }
        fileRecipes.put(filename, recipe);

        // Store containers
        for (Container container : containers) {
            String containerId = String.valueOf(container.getContainerID());
            containerMap.computeIfAbsent(containerId, k -> new ArrayList<>()).add(container);
            nextContainerID = Math.max(nextContainerID, container.getContainerID() + 1);
        }

        // Save the updated index
        saveIndex();
        saveFileRecipe(filename, recipe);
    }

    public void deleteFile(String filename) {
        List<String> recipe = fileRecipes.get(filename);
        if (recipe == null) {
            throw new IllegalArgumentException("File not found: " + filename);
        }

        // Decrement reference counts and identify chunks to delete
        for (String checksum : recipe) {
            int newRefCount = chunkReferences.merge(checksum, -1, Integer::sum);
            if (newRefCount == 0) {
                Chunk chunk = chunkMap.remove(checksum);
                markContainerForDeletion(chunk);
            }
        }

        // Remove file recipe
        fileRecipes.remove(filename);
        new File(RECIPE_DIR + File.separator + filename + ".recipe").delete();
        
        // Save updated index
        saveIndex();
    }

    private void markContainerForDeletion(Chunk chunk) {
        // Find and mark containers that might be eligible for deletion
        for (List<Container> containers : containerMap.values()) {
            for (Container container : containers) {
                if (container.chunkContents.contains(chunk)) {
                    boolean allChunksUnreferenced = container.chunkContents.stream()
                        .allMatch(c -> !chunkMap.containsKey(bytesToHex(c.getChecksum())));
                    container.setSafeToDelete(allChunksUnreferenced);
                }
            }
        }
    }

    private void loadIndex() {
        File indexFile = new File(INDEX_FILE);
        if (indexFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(indexFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    parseIndexLine(line);
                }
            } catch (IOException e) {
                System.err.println("Error loading index: " + e.getMessage());
            }
        }
    }

    private void saveIndex() {
        try (PrintWriter writer = new PrintWriter(new FileWriter(INDEX_FILE))) {
            // Write chunk information
            for (Map.Entry<String, Chunk> entry : chunkMap.entrySet()) {
                writer.println(serializeChunkEntry(entry.getValue()));
            }
            // Write container information
            for (Map.Entry<String, List<Container>> entry : containerMap.entrySet()) {
                for (Container container : entry.getValue()) {
                    writer.println(serializeContainer(container));
                }
            }
        } catch (IOException e) {
            System.err.println("Error saving index: " + e.getMessage());
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

    private void parseIndexLine(String line) {
        String[] parts = line.split("\\{");
        if (parts[0].equals("Chunk")) {
            parseChunkEntry(line);
        } else if (parts[0].equals("Container")) {
            parseContainerEntry(line);
        }
    }

    private void parseChunkEntry(String entry) {
        // Format: Chunk{chunkAddress=address, checksum=[checksum], size=size, referenceCount=referenceCount}
        try {
            Chunk chunk = new Chunk();
            chunk.parseString(entry);

            String checksum = Arrays.toString(chunk.getChecksum());
            chunkMap.put(checksum, chunk);
            chunkReferences.put(checksum, 0); // Will be updated when processing recipes
        } catch (Exception e) {
            System.err.println("Error parsing chunk entry: " + e.getMessage());
        }
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