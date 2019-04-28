package com.company;

public class Main {
    // Main class for the client side simulation, implements the ClientSocket class.
    public static void main(String[] args) {
        ClientSocket c;

        switch (args.length) {
            case 1:
                if (args[0].equals("-a")){
                    System.out.println("Missing option -a");
                    System.exit(0);
                } else {
                    System.out.println("Invalid option: " + args[0]);
                    System.exit(0);
                }

            case 2:
                if (args[0].equals("-a") && (args[1].equals("ff") || args[1].equals("bf") ||args[1].equals("wf"))){
                    c = new ClientSocket("127.0.0.1", 8096, args[1]);
                    break;
                } else {
                    System.out.println("Invalid input: " + args[1]);
                    System.exit(0);
                }

            //Defaults to allToLargest if no option provided.
            default:
                c = new ClientSocket("127.0.0.1", 8096, "atl");
                break;
        }
        c.runClient();
    }
}