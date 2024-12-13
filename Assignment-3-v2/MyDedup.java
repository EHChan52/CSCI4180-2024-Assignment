import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MyDedup {
  public static final int d = 257;

  public static class ChunkMetadata implements Serializable {
    public String containerId;
    public int offset;
    public int chunkSize; 
    public int refCount;

    public ChunkMetadata() {
      this.containerId = new String();
      this.offset = 0;
      this.chunkSize = 0;
      this.refCount = 0;
    }
  }

  public static class FileRecipes implements Serializable {
    public HashMap<String, List<ChunkMetadata>> recipe;

    public FileRecipes() {
      this.recipe = new HashMap<String, List<ChunkMetadata>>();
    }
  }

  public static class FingerprintIndexing implements Serializable {
    public int numOfFiles;
    public long logicalChunks;
    public long uniqueChunks;
    public long logicalBytes;
    public long uniqueBytes;
    public int containerNo;
    public HashMap<String, ChunkMetadata> index; // (checksum, chunkIndex)

    public FingerprintIndexing() {
      this.numOfFiles = 0;
      this.logicalChunks = 0L;
      this.uniqueChunks = 0L;
      this.logicalBytes = 0L;
      this.uniqueBytes = 0L;
      this.containerNo = 0;

      this.index = new HashMap<String, ChunkMetadata>();
    }
  }

  public static String md5(byte[] input) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    md.update(input);
    byte[] hashbyte = md.digest();
    return String.format("%032x", new BigInteger(1, hashbyte)); // Ensure 32-char hash
  }

  public static int dMod(int base, int exp, int q) {
    int result = base % q;
    while (exp - 1 > 0) {
      result = result * (base % q);
      exp--;
    }
    return result % q;
  }

  public static List<Integer> computeBoundaries(byte[] input, int minChunk, int avgChunk, int maxChunk) {
    // m: min_chunk (window size), d: multiplier, q: avg_chunk (modulo), max: max_chunk

    if ((minChunk & (minChunk - 1)) != 0 || (avgChunk & (avgChunk - 1)) != 0 || (maxChunk & (maxChunk - 1)) != 0) {
      System.out.println("minChunk, avgChunk, maxChunk should be power of 2");
      System.exit(1);
    }

    List<Integer> boundaries = new ArrayList<Integer>(); // boundary = starting position of chunk
    boundaries.add(0); // create first boundary at index 0

    int dm1 = dMod(d, minChunk - 1, avgChunk); // precompute d^m-1
    int prev = 0;

    for (int s = 0; s + minChunk <= input.length; s++) {
      // create boundary if reaches max_chunk limit
      if ((s - boundaries.get(boundaries.size() - 1) + 1) >= maxChunk) {
        boundaries.add(s + 1);
        continue;
      }

      // calculate rfp
      int rfp = 0;
      if (s == 0 || s == boundaries.get(boundaries.size() - 1)) {
        for (int i = 0; i < minChunk; i++) {
          rfp += (input[s + i] * dMod(d, minChunk - i, avgChunk)) % avgChunk;
        }
      } else {
        rfp = d * (prev - dm1 * input[s - 1]) + input[s + minChunk - 1];
      }
      rfp = rfp % avgChunk;
      prev = rfp;

      // create boundary
      if ((rfp & (avgChunk - 1)) == 0 && s + minChunk < input.length) {
        boundaries.add(s + minChunk);
        s = s + minChunk - 1;
      }
    }

    return boundaries;
  }

  public static void main(String[] args)
      throws IOException, FileNotFoundException, ClassNotFoundException, NoSuchAlgorithmException {

    switch (args[0]) {

      case "upload":
        if (args.length < 5) {
          System.out.println("Not enough arguments.");
          System.exit(1);
        } else {
          int minChunk = Integer.parseInt(args[1]);
          int avgChunk = Integer.parseInt(args[2]);
          int maxChunk = Integer.parseInt(args[3]);

          File file = new File(args[4]);

          // Moved fileRecipes to here
          FileRecipes fileRecipes;
          File fileRecipesFile = new File("data/filerecipes.index");
          // load file recipes
          if (!fileRecipesFile.exists()) {
            fileRecipesFile.getParentFile().mkdirs();
            fileRecipesFile.createNewFile();
            fileRecipes = new FileRecipes();
          } else {
            FileInputStream finRecipes = new FileInputStream(fileRecipesFile);
            ObjectInputStream oinRecipes = new ObjectInputStream(finRecipes);
            fileRecipes = (FileRecipes) oinRecipes.readObject();
            oinRecipes.close();

            // Check if filename exists in the list of files
            for (String filename : fileRecipes.recipe.keySet()) {
              if (filename.contains(args[4])) {
                System.out.println("file already exists");
                System.exit(1);
              }
            }
          }

          FileInputStream fileToUpload = new FileInputStream(file);
          byte[] data = new byte[(int) file.length()];
          fileToUpload.read(data);
          fileToUpload.close();

          // separate files into chunks
          List<Integer> boundaries = computeBoundaries(data, minChunk, avgChunk, maxChunk);

          int numOfFiles;
          long logicalChunks;
          long uniqueChunks;
          long logicalBytes;
          long uniqueBytes;
          int containerNo;

          // load index
          FingerprintIndexing indexFile;
          File indexFileFile = new File("data/mydedup.index");
          if (!indexFileFile.exists()) {
            indexFileFile.getParentFile().mkdirs();
            indexFileFile.createNewFile();
            indexFile = new FingerprintIndexing();
            numOfFiles = 0;
            logicalChunks = 0;
            uniqueChunks = 0;
            logicalBytes = 0;
            uniqueBytes = 0;
            containerNo = 0;
          } else {
            FileInputStream finIndex = new FileInputStream(indexFileFile);
            ObjectInputStream oinIndex = new ObjectInputStream(finIndex);
            indexFile = (FingerprintIndexing) oinIndex.readObject();
            oinIndex.close();
            numOfFiles = indexFile.numOfFiles;
            logicalChunks = indexFile.logicalChunks;
            uniqueChunks = indexFile.uniqueChunks;
            logicalBytes = indexFile.logicalBytes;
            uniqueBytes = indexFile.uniqueBytes;
            containerNo = indexFile.containerNo;
          }

          // prepare container
          File dir = new File("data/");
          if (!dir.exists()) {
            dir.mkdirs();
          }
          File containerFile = new File("data/container_" + (containerNo + 1));
          if (!containerFile.exists()) {
            containerFile.createNewFile();
          }
          FileOutputStream containerOut = new FileOutputStream(containerFile);
          ByteArrayOutputStream container = new ByteArrayOutputStream();
          int currentContainerBytes = 0;

          List<ChunkMetadata> chunkList = new ArrayList<ChunkMetadata>();

          for (int i = 0; i < boundaries.size(); i++) {

            byte[] currentChunk = Arrays.copyOfRange(data, boundaries.get(i),
                (i == boundaries.size() - 1) ? data.length : boundaries.get(i + 1));
            String hash = md5(currentChunk);

            if (!indexFile.index.containsKey(hash)) { // unique chunk
              // System.out.println(containerFile.getName());
              // System.out.println(currentContainerBytes+currentChunk.length);
              if (currentContainerBytes + currentChunk.length > 1048576) {
                container.writeTo(containerOut);
                container.reset();
                currentContainerBytes = 0;
                containerNo++;
                containerFile = new File("data/container_" + (containerNo + 1));
                containerFile.createNewFile();
                containerOut = new FileOutputStream(containerFile);
              }
              ChunkMetadata currentChunkMetadata = new ChunkMetadata();
              currentChunkMetadata.containerId = containerFile.getName();
              currentChunkMetadata.offset = currentContainerBytes;
              currentChunkMetadata.chunkSize = currentChunk.length;
              currentChunkMetadata.refCount = 1;
              container.write(currentChunk);
              chunkList.add(currentChunkMetadata);
              indexFile.index.put(hash, currentChunkMetadata);
              currentContainerBytes += currentChunk.length;
              uniqueChunks += 1L;
              uniqueBytes += (long) currentChunk.length;

            } else {
              ChunkMetadata currentChunkMetadata = indexFile.index.get(hash);
              chunkList.add(currentChunkMetadata);
              currentChunkMetadata.refCount++;
            }

            if (i == boundaries.size() - 1 && currentContainerBytes > 0) { // tail container
              container.writeTo(containerOut);
            }

            logicalChunks += 1L;
            logicalBytes += (long) currentChunk.length;
          }
          containerOut.close();

          fileRecipes.recipe.put(args[4], chunkList);
          numOfFiles++;

          // clean empty containers
          File containerDir = new File("data/");
          if (containerDir.exists() && containerDir.isDirectory()) {
            for (File c : containerDir.listFiles()) {
              if (c.length() == 0) {
                c.delete();
              }
            }
          }
          // adjust containerNo to the max containerid
          containerNo = (int) Arrays.stream(containerDir.list()).filter(s -> s.startsWith("container_"))
              .map(s -> Integer.parseInt(s.substring(10))).max(Integer::compare).orElse(0);

          // update stat in index file
          indexFile.numOfFiles = numOfFiles;
          indexFile.logicalChunks = logicalChunks;
          indexFile.uniqueChunks = uniqueChunks;
          indexFile.logicalBytes = logicalBytes;
          indexFile.uniqueBytes = uniqueBytes;
          indexFile.containerNo = containerNo;

          // update index and file recipe
          FileOutputStream foutIndex = new FileOutputStream(indexFileFile, false);
          ObjectOutputStream ooutIndex = new ObjectOutputStream(foutIndex);
          ooutIndex.writeObject(indexFile);
          ooutIndex.close();

          FileOutputStream foutRecipes = new FileOutputStream(fileRecipesFile, false);
          ObjectOutputStream ooutRecipes = new ObjectOutputStream(foutRecipes);
          ooutRecipes.writeObject(fileRecipes);
          ooutRecipes.close();

          int totalContainers = 0;
          if (containerDir.exists() && containerDir.isDirectory()) {
            // count the number of containers start with "container_"
            totalContainers = (int) Arrays.stream(containerDir.list())
                .filter(s -> s.startsWith("container_")).count();
          }

          // report statistics
          System.out.println("Total number of files that have been stored: " + numOfFiles);
          System.out.println("Total number of pre-deduplicated chunks in storage: " + logicalChunks);
          System.out.println("Total number of unique chunks in storage: " + uniqueChunks);
          System.out.println("Total number of bytes of pre-deduplicated chunks in storage: " + logicalBytes);
          System.out.println("Total number of bytes of unique chunks in storage: " + uniqueBytes);
          System.out.println("Total number of containers in storage: " + totalContainers);
          System.out.println("Deduplication ratio: " + ((float) logicalBytes / uniqueBytes));
        }
        break;

      case "download":
        if (args.length < 3) {
          System.out.println("Not enough arguments.");
          System.exit(1);
        } else {
          FileInputStream finRecipe = new FileInputStream("data/filerecipes.index");
          ObjectInputStream oinRecipe = new ObjectInputStream(finRecipe);
          FileRecipes fileRecipes = (FileRecipes) oinRecipe.readObject();
          if (!fileRecipes.recipe.containsKey(args[1])) {
            System.out.println("Error: \"" + args[1] + "\" does not exist");
            System.exit(1);
          }
          List<ChunkMetadata> chunkList = fileRecipes.recipe.get(args[1]);
          oinRecipe.close();

          ByteArrayOutputStream data = new ByteArrayOutputStream();
          for (int i = 0; i < chunkList.size(); i++) {
            ChunkMetadata currentChunk = chunkList.get(i);
            FileInputStream finContainer = new FileInputStream("data/" + currentChunk.containerId);
            finContainer.skip(currentChunk.offset);
            byte[] containerData = new byte[currentChunk.chunkSize];
            finContainer.read(containerData);
            data.write(containerData);
            finContainer.close();
          }

          File fout = new File(args[2]);
          if (fout.getParentFile() != null) {
            fout.getParentFile().mkdirs();
          }
          if (!fout.exists()) {
            fout.createNewFile();
          }
          FileOutputStream newFile = new FileOutputStream(fout);
          data.writeTo(newFile);
          newFile.close();
        }
        break;

      case "delete": {
        if (args.length < 2) {
          System.out.println("Not enough arguments.");
          System.exit(1);
        }

        String filename = args[1];

        // load metadata
        FingerprintIndexing indexFile;
        FileRecipes fileRecipes;
        File indexFileFile = new File("data/mydedup.index");
        File fileRecipesFile = new File("data/filerecipes.index");

        if (!indexFileFile.exists() || !fileRecipesFile.exists()) {
          System.out.println("No metadata found. Nothing to delete.");
          System.exit(1);
        }

        FileInputStream finIndex = new FileInputStream(indexFileFile);
        ObjectInputStream oinIndex = new ObjectInputStream(finIndex);
        indexFile = (FingerprintIndexing) oinIndex.readObject();
        oinIndex.close();

        FileInputStream finRecipe = new FileInputStream(fileRecipesFile);
        ObjectInputStream oinRecipe = new ObjectInputStream(finRecipe);
        fileRecipes = (FileRecipes) oinRecipe.readObject();
        oinRecipe.close();

        // check if the file exist in FileRecipes
        if (!fileRecipes.recipe.containsKey(filename)) {
          System.out.println("Error: \"" + filename + "\" does not exist");
          System.exit(1);
        }

        // get the list of chunks for the file
        List<ChunkMetadata> chunkList = fileRecipes.recipe.get(filename);

        // Map to track how many times each chunk is referenced within the file
        Map<String, Integer> chunkReferenceCount = new HashMap<>();

        // Calculate the reference count for each chunk in the file
        for (ChunkMetadata chunk : chunkList) {
          String chunkHash = null;

          // Find the hash of the chunk in the index
          for (Map.Entry<String, ChunkMetadata> entry : indexFile.index.entrySet()) {
            if (entry.getValue().containerId.equals(chunk.containerId)
                && entry.getValue().offset == chunk.offset
                && entry.getValue().chunkSize == chunk.chunkSize) {
              chunkHash = entry.getKey();
              break;
            }
          }

          if (chunkHash != null) {
            chunkReferenceCount.put(chunkHash, chunkReferenceCount.getOrDefault(chunkHash, 0) + 1);
          }
        }

        // map to track rewritten containers
        Map<String, ByteArrayOutputStream> rewrittenContainers = new HashMap<>();
        Map<String, Integer> newOffsets = new HashMap<>(); // track updated offsets
        long deletedLogicalBytes = 0; // track bytes deleted
        long deletedLogicalChunks = 0; // track chunks deleted

        for (ChunkMetadata chunk : chunkList) {
          deletedLogicalChunks++;
          deletedLogicalBytes += chunk.chunkSize;

          String chunkHash = null;

          // find the hash of the chunk in the index
          for (Map.Entry<String, ChunkMetadata> entry : indexFile.index.entrySet()) {
            if (entry.getValue().containerId.equals(chunk.containerId)
                && entry.getValue().offset == chunk.offset
                && entry.getValue().chunkSize == chunk.chunkSize) {
              chunkHash = entry.getKey();
              break;
            }
          }

          if (chunkHash != null) {
            ChunkMetadata chunkIndex = indexFile.index.get(chunkHash);
            int referencesInFile = chunkReferenceCount.getOrDefault(chunkHash, 0);

            // Decrement the refCount by the number of references in the file
            chunkIndex.refCount -= referencesInFile;

            if (chunkIndex.refCount <= 0) {
              // Remove the chunk from the index if it's no longer referenced
              indexFile.index.remove(chunkHash);
              indexFile.uniqueChunks--;
              indexFile.uniqueBytes -= chunk.chunkSize;
            }
          }

          // rewrite the container to remove the deleted file's chunks
          String containerId = chunk.containerId;
          if (!rewrittenContainers.containsKey(containerId)) {
            rewrittenContainers.put(containerId, new ByteArrayOutputStream());
            newOffsets.put(containerId, 0);
          }

          ByteArrayOutputStream containerStream = rewrittenContainers.get(containerId);
          FileInputStream containerInputStream = new FileInputStream("data/" + containerId);

          // copy data from the container, skiping the deleted chunks
          byte[] containerData = new byte[chunk.chunkSize];
          containerInputStream.skip(chunk.offset);
          containerInputStream.read(containerData);
          containerInputStream.close();

          if (chunkReferenceCount.getOrDefault(chunkHash, 0) > 0) {
            // Copy the chunk (it's still in use)
            containerStream.write(containerData);

            // Update the offset for the chunk in metadata
            int newOffset = newOffsets.get(containerId);
            newOffsets.put(containerId, newOffset + chunk.chunkSize);

            // Update the chunk's offset in the index
            for (Map.Entry<String, ChunkMetadata> entry : indexFile.index.entrySet()) {
              ChunkMetadata indexChunk = entry.getValue();
              if (indexChunk.containerId.equals(containerId)
                  && indexChunk.offset == chunk.offset
                  && indexChunk.chunkSize == chunk.chunkSize) {
                indexChunk.offset = newOffset;
              }
            }
          }
        }

        // remove the file entry from file recipes
        fileRecipes.recipe.remove(filename);

        // write the updated containers back to disk
        for (Map.Entry<String, ByteArrayOutputStream> entry : rewrittenContainers.entrySet()) {
          String containerId = entry.getKey();
          ByteArrayOutputStream containerStream = entry.getValue();

          FileOutputStream containerOutputStream = new FileOutputStream("data/" + containerId);
          containerStream.writeTo(containerOutputStream);
          containerOutputStream.close();
        }

        // check for empty containers
        File containerDir = new File("data/");
        if (containerDir.exists() && containerDir.isDirectory()) {
          String[] containerFiles = containerDir.list((dir, name) -> name.startsWith("container_"));
          if (containerFiles != null) {
            for (String containerFileName : containerFiles) {
              File containerFile = new File(containerDir, containerFileName);

              // Check if any chunks of this container are still in use
              boolean isContainerUsed = false;
              for (ChunkMetadata chunk : indexFile.index.values()) {
                if (chunk.containerId.equals(containerFileName)) {
                  isContainerUsed = true;
                  break;
                }
              }

              if (!isContainerUsed) {
                // Delete the empty container
                containerFile.delete();
              }
            }
          }
        }

        // reset containerNo to max container id
        indexFile.containerNo = (int) Arrays.stream(containerDir.list()).filter(s -> s.startsWith("container_"))
            .map(s -> Integer.parseInt(s.substring(10))).max(Integer::compare).orElse(0);

        // update statistics in the index file
        indexFile.numOfFiles = fileRecipes.recipe.size();
        indexFile.logicalChunks -= deletedLogicalChunks;
        indexFile.logicalBytes -= deletedLogicalBytes;

        // save updated metadata
        FileOutputStream foutIndex = new FileOutputStream(indexFileFile, false);
        ObjectOutputStream ooutIndex = new ObjectOutputStream(foutIndex);
        ooutIndex.writeObject(indexFile);
        ooutIndex.close();

        FileOutputStream foutRecipe = new FileOutputStream(fileRecipesFile, false);
        ObjectOutputStream ooutRecipe = new ObjectOutputStream(foutRecipe);
        ooutRecipe.writeObject(fileRecipes);
        ooutRecipe.close();

        // report statistics
        System.out.println("File \"" + filename + "\" has been deleted.");
        System.out.println("Total number of files that remain stored: " + indexFile.numOfFiles);
        System.out.println("Total number of unique chunks in storage: " + indexFile.uniqueChunks);
        System.out.println("Total number of bytes of unique chunks in storage: " + indexFile.uniqueBytes);
      }
        break;

      default:
        System.out.println("Unknown command \"" + args[0] + "\"");
        System.exit(1);
    }
  }
}