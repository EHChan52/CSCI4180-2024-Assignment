class MyDedup {
    public static void main(String[] args) {

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
