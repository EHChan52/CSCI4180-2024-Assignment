import java.io.*;
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
            FileRecipe recipe = new FileRecipe(recipeFile.getName().replace("recipe-", ""), chunkList);
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

}
