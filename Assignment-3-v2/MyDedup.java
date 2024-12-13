import java.io.*;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MyDedup {

    // Constants
    public static final int d = 257;
    private static final String DATA_DIR = "data/";
    private static final String INDEX_FILE = "data/mydedup.index";
    private static final String RECIPE_FILE = "data/filerecipes.index";

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

    // Helper for computing MD5 hash
    public static String md5(byte[] input) throws NoSuchAlgorithmException {
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

    // Compute chunk boundaries using Rabin fingerprinting
    public static List<Integer> computeBoundaries(byte[] data, int minChunk, int avgChunk, int maxChunk) {
        if ((minChunk & (minChunk - 1)) != 0 || (avgChunk & (avgChunk - 1)) != 0 || (maxChunk & (maxChunk - 1)) != 0) {
            throw new IllegalArgumentException("Chunk sizes must be powers of 2");
        }
        List<Integer> boundaries = new ArrayList<>();
        boundaries.add(0);

        int dm1 = (int) Math.pow(d, minChunk - 1) % avgChunk;
        int prevRfp = 0;

        for (int i = 0; i + minChunk <= data.length; i++) {
            if (i - boundaries.get(boundaries.size() - 1) + 1 >= maxChunk) {
                boundaries.add(i + 1);
                continue;
            }
            int rfp = (i == 0 || i == boundaries.get(boundaries.size() - 1))
                    ? computeInitialRfp(data, i, minChunk, avgChunk)
                    : d * (prevRfp - dm1 * data[i - 1]) + data[i + minChunk - 1];
            rfp %= avgChunk;
            prevRfp = rfp;

            if ((rfp & (avgChunk - 1)) == 0 && i + minChunk < data.length) {
                boundaries.add(i + minChunk);
                i += minChunk - 1;
            }
        }
        return boundaries;
    }

    private static int computeInitialRfp(byte[] data, int start, int chunkSize, int avgChunk) {
        int rfp = 0;
        for (int i = 0; i < chunkSize; i++) {
            rfp += data[start + i] * Math.pow(d, chunkSize - i) % avgChunk;
        }
        return rfp % avgChunk;
    }

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
  
      // Perform chunking
      List<Integer> boundaries = computeBoundaries(fileData, minChunk, avgChunk, maxChunk);
      ByteArrayOutputStream containerBuffer = new ByteArrayOutputStream();
      int containerBytes = 0;
  
      File containerDir = new File(DATA_DIR);
      if (!containerDir.exists()) containerDir.mkdirs();
  
      List<ChunkMetadata> fileChunks = new ArrayList<>();
  
      for (int i = 0; i < boundaries.size(); i++) {
          int chunkStart = boundaries.get(i);
          int chunkEnd = (i == boundaries.size() - 1) ? fileData.length : boundaries.get(i + 1);
  
          byte[] chunkData = Arrays.copyOfRange(fileData, chunkStart, chunkEnd);
          String hash = md5(chunkData);
  
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
      FileRecipes recipes = loadMetadata(RECIPE_FILE, FileRecipes.class);
      FingerprintIndexing index = loadMetadata(INDEX_FILE, FingerprintIndexing.class);
  
      if (!recipes.recipe.containsKey(filePath)) {
          System.out.println("File not found in storage.");
          return;
      }
  
      List<ChunkMetadata> fileChunks = recipes.recipe.get(filePath);
      Map<String, Boolean> containerUsed = new HashMap<>();
  
      for (ChunkMetadata chunk : fileChunks) {
          String chunkHash = null;
  
          for (Map.Entry<String, ChunkMetadata> entry : index.index.entrySet()) {
              if (entry.getValue().containerId.equals(chunk.containerId)
                      && entry.getValue().offset == chunk.offset
                      && entry.getValue().chunkSize == chunk.chunkSize) {
                  chunkHash = entry.getKey();
                  break;
              }
          }
  
          if (chunkHash != null) {
              ChunkMetadata metadata = index.index.get(chunkHash);
              metadata.refCount--;
  
              if (metadata.refCount <= 0) {
                  index.index.remove(chunkHash);
                  index.uniqueChunks--;
                  index.uniqueBytes -= metadata.chunkSize;
              }
          }
  
          containerUsed.put(chunk.containerId, true);
      }
  
      recipes.recipe.remove(filePath);
      index.numOfFiles--;
  
      for (String containerId : containerUsed.keySet()) {
          boolean containerStillInUse = index.index.values().stream()
                  .anyMatch(chunk -> chunk.containerId.equals(containerId));
  
          if (!containerStillInUse) {
              File containerFile = new File(DATA_DIR + containerId);
              if (containerFile.exists()) {
                  containerFile.delete();
              }
          }
      }
  
      saveMetadata(INDEX_FILE, index);
      saveMetadata(RECIPE_FILE, recipes);
  
      printStatistics(index);
  }
  
  private static void printStatistics(FingerprintIndexing index) {
      System.out.println("Total number of files stored: " + index.numOfFiles);
      System.out.println("Total number of pre-deduplicated chunks: " + index.logicalChunks);
      System.out.println("Total number of unique chunks: " + index.uniqueChunks);
      System.out.println("Total bytes of pre-deduplicated chunks: " + index.logicalBytes);
      System.out.println("Total bytes of unique chunks: " + index.uniqueBytes);
      System.out.println("Deduplication ratio: " + String.format("%.2f", (float) index.logicalBytes / index.uniqueBytes));
  }
  
}
