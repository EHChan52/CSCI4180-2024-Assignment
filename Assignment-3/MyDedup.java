import java.io.File;
import java.io.IOException;

class MyDedup {
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


        if("upload".equals(args[0])){
            System.out.println("upload");
        }
        else if("download".equals(args[0])){
            System.out.println("download");
        }

        //debug statement
        System.out.println("Number of arguments: " + args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.println("Argument " + (i + 1) + ": " + args[i]);
        }
    }
}
