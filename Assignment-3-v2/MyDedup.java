import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MyDedup {
  public static final int d = 257;

  public static class ChunkIndex implements Serializable { // struct to record the info about chunk
    public String containerID; // container that the chunk is located at
    public int offset; // offset of starting point of chunk
    public int chunkSize; // size of chunk
    public int refCount; // reference count

    public ChunkIndex() {
      this.containerID = new String();
      this.offset = 0;
      this.chunkSize = 0;
      this.refCount = 0;
    }
  }

  public static class FileRecipes implements Serializable {
    public HashMap<String, List<ChunkIndex>> recipe; // (filename, list of chunks)

    public FileRecipes() {
      this.recipe = new HashMap<String, List<ChunkIndex>>();
    }
  }

  public static class IndexFile implements Serializable {
    public int num_files;
    public long logical_chunks;
    public long unique_chunks;
    public long logical_bytes;
    public long unique_bytes;
    public int container_no;
    public HashMap<String, ChunkIndex> index; // (checksum, chunkIndex)

    public IndexFile() {
      this.num_files = 0;
      this.logical_chunks = 0L;
      this.unique_chunks = 0L;
      this.logical_bytes = 0L;
      this.unique_bytes = 0L;
      this.container_no = 0;

      this.index = new HashMap<String, ChunkIndex>();
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

  public static List<Integer> computeBoundaries(byte[] input, int m, int q, int max) {
    // m: min_chunk (window size), d: multiplier, q: avg_chunk (modulo), max:
    // max_chunk

    // m, q, max should be power of 2
    if ((m & (m - 1)) != 0 || (q & (q - 1)) != 0 || (max & (max - 1)) != 0) {
      System.out.println("minChunk, avgChunk, maxChunk should be power of 2");
      System.exit(1);
    }

    List<Integer> boundaries = new ArrayList<Integer>(); // boundary = starting position of chunk
    boundaries.add(0); // create first boundary at index 0

    int dm1 = dMod(d, m - 1, q); // precompute d^m-1
    int prev = 0;

    for (int s = 0; s + m <= input.length; s++) {
      // create boundary if reaches max_chunk limit
      if ((s - boundaries.get(boundaries.size() - 1) + 1) >= max) {
        boundaries.add(s + 1);
        continue;
      }

      // calculate rfp
      int rfp = 0;
      if (s == 0 || s == boundaries.get(boundaries.size() - 1)) {
        for (int i = 0; i < m; i++) {
          rfp += (input[s + i] * dMod(d, m - i, q)) % q;
        }
      } else {
        rfp = d * (prev - dm1 * input[s - 1]) + input[s + m - 1];
      }
      rfp = rfp % q;
      prev = rfp;

      // create boundary
      if ((rfp & (q - 1)) == 0 && s + m < input.length) {
        boundaries.add(s + m);
        s = s + m - 1;
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
          int m = Integer.parseInt(args[1]);
          int q = Integer.parseInt(args[2]);
          int max = Integer.parseInt(args[3]);

          File f = new File(args[4]);

          // Moved fileRecipes to here
          FileRecipes fileRecipes;
          File f_recipes = new File("data/filerecipes.index");
          // load file recipes
          if (!f_recipes.exists()) {
            f_recipes.getParentFile().mkdirs();
            f_recipes.createNewFile();
            fileRecipes = new FileRecipes();
          } else {
            FileInputStream fin_recipes = new FileInputStream(f_recipes);
            ObjectInputStream oin_recipes = new ObjectInputStream(fin_recipes);
            fileRecipes = (FileRecipes) oin_recipes.readObject();
            oin_recipes.close();

            // Check if filename exists in the list of files
            for (String filename : fileRecipes.recipe.keySet()) {
              if (filename.contains(args[4])) {
                System.out.println("file already exists");
                System.exit(1);
              }
            }
          }

          FileInputStream file_to_upload = new FileInputStream(f);
          byte[] data = new byte[(int) f.length()];
          file_to_upload.read(data);
          file_to_upload.close();

          // separate files into chunks
          List<Integer> boundaries = computeBoundaries(data, m, q, max);

          int num_files;
          long logical_chunks;
          long unique_chunks;
          long logical_bytes;
          long unique_bytes;
          int container_no;

          // load index
          IndexFile indexFile;
          File f_index = new File("data/mydedup.index");
          if (!f_index.exists()) {
            f_index.getParentFile().mkdirs();
            f_index.createNewFile();
            indexFile = new IndexFile();
            num_files = 0;
            logical_chunks = 0;
            unique_chunks = 0;
            logical_bytes = 0;
            unique_bytes = 0;
            container_no = 0;
          } else {
            FileInputStream fin_index = new FileInputStream(f_index);
            ObjectInputStream oin_index = new ObjectInputStream(fin_index);
            indexFile = (IndexFile) oin_index.readObject();
            oin_index.close();
            num_files = indexFile.num_files;
            logical_chunks = indexFile.logical_chunks;
            unique_chunks = indexFile.unique_chunks;
            logical_bytes = indexFile.logical_bytes;
            unique_bytes = indexFile.unique_bytes;
            container_no = indexFile.container_no;
          }

          // prepare container
          File dir = new File("data/");
          if (!dir.exists()) {
            dir.mkdirs();
          }
          File containerFile = new File("data/container_" + (container_no + 1));
          if (!containerFile.exists()) {
            containerFile.createNewFile();
          }
          FileOutputStream container_out = new FileOutputStream(containerFile);
          ByteArrayOutputStream container = new ByteArrayOutputStream();
          int currentContainerBytes = 0;

          List<ChunkIndex> chunkList = new ArrayList<ChunkIndex>();

          for (int i = 0; i < boundaries.size(); i++) {

            byte[] currentChunk = Arrays.copyOfRange(data, boundaries.get(i),
                (i == boundaries.size() - 1) ? data.length : boundaries.get(i + 1));
            String hash = md5(currentChunk);

            if (!indexFile.index.containsKey(hash)) { // unique chunk
              // System.out.println(containerFile.getName());
              // System.out.println(currentContainerBytes+currentChunk.length);
              if (currentContainerBytes + currentChunk.length > 1048576) {
                container.writeTo(container_out);
                container.reset();
                currentContainerBytes = 0;
                container_no++;
                containerFile = new File("data/container_" + (container_no + 1));
                containerFile.createNewFile();
                container_out = new FileOutputStream(containerFile);
              }
              ChunkIndex currentChunkIndex = new ChunkIndex();
              currentChunkIndex.containerID = containerFile.getName();
              currentChunkIndex.offset = currentContainerBytes;
              currentChunkIndex.chunkSize = currentChunk.length;
              currentChunkIndex.refCount = 1;
              container.write(currentChunk);
              chunkList.add(currentChunkIndex);
              indexFile.index.put(hash, currentChunkIndex);
              currentContainerBytes += currentChunk.length;
              unique_chunks += 1L;
              unique_bytes += (long) currentChunk.length;

            } else {
              ChunkIndex currentChunkIndex = indexFile.index.get(hash);
              chunkList.add(currentChunkIndex);
              currentChunkIndex.refCount++;
            }

            if (i == boundaries.size() - 1 && currentContainerBytes > 0) { // tail container
              container.writeTo(container_out);
            }

            logical_chunks += 1L;
            logical_bytes += (long) currentChunk.length;
          }
          container_out.close();

          fileRecipes.recipe.put(args[4], chunkList);
          num_files++;

          // clean empty containers
          File containerDir = new File("data/");
          if (containerDir.exists() && containerDir.isDirectory()) {
            for (File c : containerDir.listFiles()) {
              if (c.length() == 0) {
                c.delete();
              }
            }
          }
          // adjust container_no to the max containerid
          container_no = (int) Arrays.stream(containerDir.list()).filter(s -> s.startsWith("container_"))
              .map(s -> Integer.parseInt(s.substring(10))).max(Integer::compare).orElse(0);

          // update stat in index file
          indexFile.num_files = num_files;
          indexFile.logical_chunks = logical_chunks;
          indexFile.unique_chunks = unique_chunks;
          indexFile.logical_bytes = logical_bytes;
          indexFile.unique_bytes = unique_bytes;
          indexFile.container_no = container_no;

          // update index and file recipe
          FileOutputStream fout_index = new FileOutputStream(f_index, false);
          ObjectOutputStream oout_index = new ObjectOutputStream(fout_index);
          oout_index.writeObject(indexFile);
          oout_index.close();

          FileOutputStream fout_recipes = new FileOutputStream(f_recipes, false);
          ObjectOutputStream oout_recipes = new ObjectOutputStream(fout_recipes);
          oout_recipes.writeObject(fileRecipes);
          oout_recipes.close();

          int totalContainers = 0;
          if (containerDir.exists() && containerDir.isDirectory()) {
            // count the number of containers start with "container_"
            totalContainers = (int) Arrays.stream(containerDir.list())
                .filter(s -> s.startsWith("container_")).count();
          }

          // report statistics
          System.out.println("Total number of files that have been stored: " + num_files);
          System.out.println("Total number of pre-deduplicated chunks in storage: " + logical_chunks);
          System.out.println("Total number of unique chunks in storage: " + unique_chunks);
          System.out.println("Total number of bytes of pre-deduplicated chunks in storage: " + logical_bytes);
          System.out.println("Total number of bytes of unique chunks in storage: " + unique_bytes);
          System.out.println("Total number of containers in storage: " + totalContainers);
          System.out.println("Deduplication ratio: " + ((float) logical_bytes / unique_bytes));
        }
        break;

      case "download":
        if (args.length < 3) {
          System.out.println("Not enough arguments.");
          System.exit(1);
        } else {
          FileInputStream fin_recipe = new FileInputStream("data/filerecipes.index");
          ObjectInputStream oin_recipe = new ObjectInputStream(fin_recipe);
          FileRecipes fileRecipes = (FileRecipes) oin_recipe.readObject();
          if (!fileRecipes.recipe.containsKey(args[1])) {
            System.out.println("Error: \"" + args[1] + "\" does not exist");
            System.exit(1);
          }
          List<ChunkIndex> chunkList = fileRecipes.recipe.get(args[1]);
          oin_recipe.close();

          ByteArrayOutputStream data = new ByteArrayOutputStream();
          for (int i = 0; i < chunkList.size(); i++) {
            ChunkIndex currentChunk = chunkList.get(i);
            FileInputStream fin_container = new FileInputStream("data/" + currentChunk.containerID);
            fin_container.skip(currentChunk.offset);
            byte[] containerData = new byte[currentChunk.chunkSize];
            fin_container.read(containerData);
            data.write(containerData);
            fin_container.close();
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
        IndexFile indexFile;
        FileRecipes fileRecipes;
        File f_index = new File("data/mydedup.index");
        File f_recipe = new File("data/filerecipes.index");

        if (!f_index.exists() || !f_recipe.exists()) {
          System.out.println("No metadata found. Nothing to delete.");
          System.exit(1);
        }

        FileInputStream fin_index = new FileInputStream(f_index);
        ObjectInputStream oin_index = new ObjectInputStream(fin_index);
        indexFile = (IndexFile) oin_index.readObject();
        oin_index.close();

        FileInputStream fin_recipe = new FileInputStream(f_recipe);
        ObjectInputStream oin_recipe = new ObjectInputStream(fin_recipe);
        fileRecipes = (FileRecipes) oin_recipe.readObject();
        oin_recipe.close();

        // check if the file exist in FileRecipes
        if (!fileRecipes.recipe.containsKey(filename)) {
          System.out.println("Error: \"" + filename + "\" does not exist");
          System.exit(1);
        }

        // get the list of chunks for the file
        List<ChunkIndex> chunkList = fileRecipes.recipe.get(filename);

        // Map to track how many times each chunk is referenced within the file
        Map<String, Integer> chunkReferenceCount = new HashMap<>();

        // Calculate the reference count for each chunk in the file
        for (ChunkIndex chunk : chunkList) {
          String chunkHash = null;

          // Find the hash of the chunk in the index
          for (Map.Entry<String, ChunkIndex> entry : indexFile.index.entrySet()) {
            if (entry.getValue().containerID.equals(chunk.containerID)
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

        for (ChunkIndex chunk : chunkList) {
          deletedLogicalChunks++;
          deletedLogicalBytes += chunk.chunkSize;

          String chunkHash = null;

          // find the hash of the chunk in the index
          for (Map.Entry<String, ChunkIndex> entry : indexFile.index.entrySet()) {
            if (entry.getValue().containerID.equals(chunk.containerID)
                && entry.getValue().offset == chunk.offset
                && entry.getValue().chunkSize == chunk.chunkSize) {
              chunkHash = entry.getKey();
              break;
            }
          }

          if (chunkHash != null) {
            ChunkIndex chunkIndex = indexFile.index.get(chunkHash);
            int referencesInFile = chunkReferenceCount.getOrDefault(chunkHash, 0);

            // Decrement the refCount by the number of references in the file
            chunkIndex.refCount -= referencesInFile;

            if (chunkIndex.refCount <= 0) {
              // Remove the chunk from the index if it's no longer referenced
              indexFile.index.remove(chunkHash);
              indexFile.unique_chunks--;
              indexFile.unique_bytes -= chunk.chunkSize;
            }
          }

          // rewrite the container to remove the deleted file's chunks
          String containerId = chunk.containerID;
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
            for (Map.Entry<String, ChunkIndex> entry : indexFile.index.entrySet()) {
              ChunkIndex indexChunk = entry.getValue();
              if (indexChunk.containerID.equals(containerId)
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
              for (ChunkIndex chunk : indexFile.index.values()) {
                if (chunk.containerID.equals(containerFileName)) {
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

        // reset container_no to max container id
        indexFile.container_no = (int) Arrays.stream(containerDir.list()).filter(s -> s.startsWith("container_"))
            .map(s -> Integer.parseInt(s.substring(10))).max(Integer::compare).orElse(0);

        // update statistics in the index file
        indexFile.num_files = fileRecipes.recipe.size();
        indexFile.logical_chunks -= deletedLogicalChunks;
        indexFile.logical_bytes -= deletedLogicalBytes;

        // save updated metadata
        FileOutputStream fout_index = new FileOutputStream(f_index, false);
        ObjectOutputStream oout_index = new ObjectOutputStream(fout_index);
        oout_index.writeObject(indexFile);
        oout_index.close();

        FileOutputStream fout_recipe = new FileOutputStream(f_recipe, false);
        ObjectOutputStream oout_recipe = new ObjectOutputStream(fout_recipe);
        oout_recipe.writeObject(fileRecipes);
        oout_recipe.close();

        // report statistics
        System.out.println("File \"" + filename + "\" has been deleted.");
        System.out.println("Total number of files that remain stored: " + indexFile.num_files);
        System.out.println("Total number of unique chunks in storage: " + indexFile.unique_chunks);
        System.out.println("Total number of bytes of unique chunks in storage: " + indexFile.unique_bytes);
      }
        break;

      default:
        System.out.println("Unknown command \"" + args[0] + "\"");
        System.exit(1);
    }
  }
}