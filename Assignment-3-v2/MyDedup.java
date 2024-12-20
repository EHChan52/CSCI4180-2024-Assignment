import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MyDedup {

    // Constants
    public static final int d = 257;
    private static final String DATA_DIR = "data/";
    private static final String INDEX_FILE = "data/mydedup.index";
    private static final String RECIPE_FILE = "data/filerecipes.index";

    // Helper for computing MD5 hash
    public static String MD5(byte[] input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("MD5");
        md.update(input);
        byte[] hashbyte = md.digest();
        return String.format("%032x", new BigInteger(1, hashbyte));
    }

    // Load metadata from file
    @SuppressWarnings("unchecked")
    private static <T> T loadMetadata(String filepath, Class<T> type) throws IOException, ClassNotFoundException {
        File file = new File(filepath);
        if (!file.exists()) {
            if (file.getParentFile() != null) file.getParentFile().mkdirs();
            file.createNewFile();
            return type == FingerprintIndexing.class ? (T) new FingerprintIndexing() : (T) new FileRecipes();
        }
        try (FileInputStream fin = new FileInputStream(file);
             ObjectInputStream oin = new ObjectInputStream(fin)) {
            return (T) oin.readObject();
        }
    }

    // Save metadata to file
    private static void saveMetadata(String filepath, Object metadata) throws IOException {
        try (FileOutputStream fout = new FileOutputStream(filepath, false);
             ObjectOutputStream oout = new ObjectOutputStream(fout)) {
            oout.writeObject(metadata);
        }
    }

    // Compute using Rabin fingerprinting
    public static List<Integer> Anchoring(byte[] input, int minChunk, int avgChunk, int maxChunk) {
        if ((minChunk & (minChunk - 1)) != 0 || (avgChunk & (avgChunk - 1)) != 0 || (maxChunk & (maxChunk - 1)) != 0) {
            throw new IllegalArgumentException("Chunk sizes must be powers of 2");
        }
    
        List<Integer> anchors = new ArrayList<>();
        anchors.add(0); // Start with the first anchor at position 0
    
        int dm1 = ModPower(d, minChunk - 1, avgChunk); // Precompute d^(m-1) % q
        int rollingHash = 0;
    
        for (int s = 0; s + minChunk <= input.length; s++) {
            // Create a boundary if max_chunk is exceeded
            if ((s - anchors.get(anchors.size() - 1) + 1) >= maxChunk) {
                anchors.add(s + 1);
                continue;
            }
    
            // Calculate the Rabin fingerprint (rfp)
            if (s == 0 || s == anchors.get(anchors.size() - 1)) {
                // Start of a file or chunk: Compute the initial fingerprint
                rollingHash = 0;
                for (int i = 0; i < minChunk; i++) {
                    rollingHash += (input[s + i] * ModPower(d, minChunk - i, avgChunk)) % avgChunk;
                }
            } else {
                // Rolling hash: Update based on the previous fingerprint
                rollingHash = d * (rollingHash - dm1 * input[s - 1]) + input[s + minChunk - 1];
            }
            rollingHash = rollingHash % avgChunk;
    
            // Create a boundary if the anchor condition is met
            if ((rollingHash & (avgChunk - 1)) == 0 && s + minChunk < input.length) {
                anchors.add(s + minChunk);
                s = s + minChunk - 1;
            }
        }
    
        return anchors;
    }

    public static int ModPower(int base, int exp, int q) {
        int result = base % q; // Start with base mod q
        while (exp - 1 > 0) {  // Iterate until exp reduces to 1
            result = (result * (base % q)) % q; // Multiply and reduce mod q
            exp--;
        }
        return result % q; // Final reduction mod q
    }

    // private static int computeInitialRfp(byte[] data, int start, int chunkSize, int avgChunk) {
    //     int rfp = 0;
    //     for (int i = 0; i < chunkSize; i++) {
    //         rfp += data[start + i] * Math.pow(d, chunkSize - i) % avgChunk;
    //     }
    //     return rfp % avgChunk;
    // }

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("Usage: java MyDedup <command> [args]");
                return;
            }
            String command = args[0];
            switch (command) {
                case "upload":
                    if (args.length < 5) throw new IllegalArgumentException("Not enough arguments for upload");
                    handleUpload(args);
                    break;
                case "download":
                    if (args.length < 3) throw new IllegalArgumentException("Not enough arguments for download");
                    handleDownload(args[1], args[2]);
                    break;
                case "delete":
                    if (args.length < 2) throw new IllegalArgumentException("Not enough arguments for delete");
                    handleDelete(args[1]);
                    break;
                default:
                    System.out.println("Unknown command: " + command);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void handleUpload(String[] args) throws IOException, ClassNotFoundException, NoSuchAlgorithmException {
        int minChunk = Integer.parseInt(args[1]);
        int avgChunk = Integer.parseInt(args[2]);
        int maxChunk = Integer.parseInt(args[3]);
        String filePath = args[4];
    
        File inputFile = new File(filePath);
        if (!inputFile.exists() || !inputFile.isFile()) {
            throw new FileNotFoundException("File not found: " + filePath);
        }
    
        byte[] fileData = Files.readAllBytes(inputFile.toPath());
    
        // Load metadata
        FingerprintIndexing index = loadMetadata(INDEX_FILE, FingerprintIndexing.class);
        FileRecipes recipes = loadMetadata(RECIPE_FILE, FileRecipes.class);
    
        // Check if the file is already uploaded
        if (recipes.recipe.containsKey(filePath)) {
            System.out.println("File is already uploaded.");
            return;
        }
    
        // Perform chunking using optimized computeAnchors method
        List<Integer> anchors = Anchoring(fileData, minChunk, avgChunk, maxChunk);
        ByteArrayOutputStream containerBuffer = new ByteArrayOutputStream();
        int containerBytes = 0;
    
        File containerDir = new File(DATA_DIR);
        if (!containerDir.exists()) containerDir.mkdirs();
    
        List<ChunkMetadata> fileChunks = new ArrayList<>();
    
        for (int i = 0; i < anchors.size(); i++) {
            int chunkStart = anchors.get(i);
            int chunkEnd = (i == anchors.size() - 1) ? fileData.length : anchors.get(i + 1);
    
            byte[] chunkData = Arrays.copyOfRange(fileData, chunkStart, chunkEnd);
            String hash = MD5(chunkData);
    
            ChunkMetadata metadata;
            if (index.index.containsKey(hash)) {
                metadata = index.index.get(hash);
                metadata.refCount++;
            } else {
                if (containerBytes + chunkData.length > 1048576) {
                    flushContainer(containerBuffer, index, containerBytes);
                    containerBuffer.reset();
                    containerBytes = 0;
                }
    
                metadata = new ChunkMetadata();
                metadata.containerId = "container-" + index.containerNo;
                metadata.offset = containerBytes;
                metadata.chunkSize = chunkData.length;
                metadata.refCount = 1;
    
                containerBuffer.write(chunkData);
                containerBytes += chunkData.length;
    
                index.index.put(hash, metadata);
                index.uniqueChunks++;
                index.uniqueBytes += chunkData.length;
            }
    
            fileChunks.add(metadata);
            index.logicalChunks++;
            index.logicalBytes += chunkData.length;
        }
    
        if (containerBytes > 0) {
            flushContainer(containerBuffer, index, containerBytes);
        }
    
        recipes.recipe.put(filePath, fileChunks);
        index.numOfFiles++;
    
        saveMetadata(INDEX_FILE, index);
        saveMetadata(RECIPE_FILE, recipes);
    
        printStatistics(index);
    }
  
  private static void flushContainer(ByteArrayOutputStream containerBuffer, FingerprintIndexing index, int containerBytes) throws IOException {
      File containerFile = new File(DATA_DIR + "container-" + index.containerNo);
      try (FileOutputStream fos = new FileOutputStream(containerFile)) {
          containerBuffer.writeTo(fos);
      }
      index.containerNo++;
  }
  
  private static void handleDownload(String filePath, String outputPath) throws IOException, ClassNotFoundException {
      FileRecipes recipes = loadMetadata(RECIPE_FILE, FileRecipes.class);
      if (!recipes.recipe.containsKey(filePath)) {
          throw new FileNotFoundException("File not found in storage: " + filePath);
      }
  
      List<ChunkMetadata> fileChunks = recipes.recipe.get(filePath);
      ByteArrayOutputStream reconstructedData = new ByteArrayOutputStream();
  
      for (ChunkMetadata chunk : fileChunks) {
          File containerFile = new File(DATA_DIR + chunk.containerId);
          if (!containerFile.exists()) {
              throw new FileNotFoundException("Missing container file: " + chunk.containerId);
          }
  
          try (RandomAccessFile raf = new RandomAccessFile(containerFile, "r")) {
              raf.seek(chunk.offset);
              byte[] chunkData = new byte[chunk.chunkSize];
              raf.readFully(chunkData);
              reconstructedData.write(chunkData);
          }
      }
  
      File outputFile = new File(outputPath);
      if (!outputFile.exists() && outputFile.getParentFile() != null) {
          outputFile.getParentFile().mkdirs();
      }
  
      try (FileOutputStream fos = new FileOutputStream(outputFile)) {
          reconstructedData.writeTo(fos);
      }
  
      System.out.println("File downloaded successfully to: " + outputPath);
  }
  
  private static void handleDelete(String filePath) throws IOException, ClassNotFoundException {
    // Load metadata
    FileRecipes recipes = loadMetadata(RECIPE_FILE, FileRecipes.class);
    FingerprintIndexing index = loadMetadata(INDEX_FILE, FingerprintIndexing.class);

    // Check if the file exists in the metadata
    if (!recipes.recipe.containsKey(filePath)) {
        System.out.println("File not found in storage.");
        return;
    }

    // Get the list of chunks associated with the file
    List<ChunkMetadata> fileChunks = recipes.recipe.get(filePath);

    // Track containers that need to be checked for emptiness
    Set<String> affectedContainers = new HashSet<>();

    // Step 1: Update reference counts and identify invalid chunks
    for (ChunkMetadata chunk : fileChunks) {
        String chunkHash = null;

        // Find the chunk in the index by matching metadata
        for (Map.Entry<String, ChunkMetadata> entry : index.index.entrySet()) {
            ChunkMetadata metadata = entry.getValue();
            if (metadata.containerId.equals(chunk.containerId) &&
                metadata.offset == chunk.offset &&
                metadata.chunkSize == chunk.chunkSize) {
                chunkHash = entry.getKey();
                break;
            }
        }

        // Decrement reference count and handle invalid chunks
        if (chunkHash != null) {
            ChunkMetadata metadata = index.index.get(chunkHash);
            metadata.refCount--;

            // If the chunk is no longer referenced, mark it as invalid
            if (metadata.refCount <= 0) {
                index.index.remove(chunkHash);
                index.uniqueChunks--;
                index.uniqueBytes -= metadata.chunkSize;
            }

            // Update logicalBytes and logicalChunks
            index.logicalBytes -= metadata.chunkSize;
            index.logicalChunks--;

            // Add the container to the list of affected containers
            affectedContainers.add(chunk.containerId);
        }
    }

    // Step 2: Remove file metadata from recipes
    recipes.recipe.remove(filePath);
    index.numOfFiles--;

    // Step 3: Identify and delete empty containers
    for (String containerId : affectedContainers) {
        File containerFile = new File(DATA_DIR + containerId);

        // Check if all chunks in the container are invalid
        boolean isContainerEmpty = index.index.values().stream()
            .noneMatch(chunk -> chunk.containerId.equals(containerId));

        // If the container is empty, delete it
        if (isContainerEmpty) {
            if (containerFile.exists()) {
                containerFile.delete();
            }
        }
    }

    // Step 4: Save updated metadata
    saveMetadata(RECIPE_FILE, recipes);
    saveMetadata(INDEX_FILE, index);

    // Report updated statistics
    printStatistics(index);
}
  
private static void printStatistics(FingerprintIndexing index) {
    // Calculate the total number of containers
    File containerDir = new File(DATA_DIR);
    int totalContainers = 0;

    if (containerDir.exists() && containerDir.isDirectory()) {
        // Count files that start with "container_"
        totalContainers = (int) Arrays.stream(containerDir.list())
                                      .filter(fileName -> fileName.startsWith("container-"))
                                      .count();
    }

    System.out.println("Total number of files that have been stored: " + index.numOfFiles);
    System.out.println("Total number of pre-deduplicated chunks in storage: " + index.logicalChunks);
    System.out.println("Total number of unique chunks in storage: " + index.uniqueChunks);
    System.out.println("Total number of bytes of pre-deduplicated chunks in storage: " + index.logicalBytes);
    System.out.println("Total number of bytes of unique chunks in storage: " + index.uniqueBytes);
    System.out.println("Total number of containers in storage: " + totalContainers);

    if (index.uniqueBytes > 0) {
        System.out.println("Deduplication ratio: " + String.format("%.2f", (float) index.logicalBytes / index.uniqueBytes));
    } else {
        System.out.println("Deduplication ratio: N/A (No unique bytes stored)");
    }
}

  // Chunk Metadata Class
  public static class ChunkMetadata implements Serializable {
    public String containerId;
    public int offset;
    public int chunkSize;
    public int refCount;

    public ChunkMetadata() {
        this.containerId = "";
        this.offset = 0;
        this.chunkSize = 0;
        this.refCount = 0;
    }
  }

  // File Recipes Class
  public static class FileRecipes implements Serializable {
    public HashMap<String, List<ChunkMetadata>> recipe;

    public FileRecipes() {
        this.recipe = new HashMap<>();
    }
  }

  // Fingerprint Indexing Class
  public static class FingerprintIndexing implements Serializable {
    public int numOfFiles;
    public long logicalChunks;
    public long uniqueChunks;
    public long logicalBytes;
    public long uniqueBytes;
    public int containerNo;
    public HashMap<String, ChunkMetadata> index;

    public FingerprintIndexing() {
        this.numOfFiles = 0;
        this.logicalChunks = 0L;
        this.uniqueChunks = 0L;
        this.logicalBytes = 0L;
        this.uniqueBytes = 0L;
        this.containerNo = 0;
        this.index = new HashMap<>();
    }
  }
}
