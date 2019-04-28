package com.company;

/**
 * Main driving class for the ClientSocket class and handles command line parsing and creating of the scheduling
 * instance based on the inputs.
 *
 * @author Nicholas Mangano
 * @version 2.0
 */
public class Main {
    // Main class for the client side simulation, implements the ClientSocket class.

    /**
     * Function that parses the command line arguments and creates and instance of ClientSocket based on the user inputs
     * provided at runtime.
     *
     * @param args takes the form [-a] algorithm
     */
    public static void main(String[] args) {
        ClientSocket c;

        // Switch statement for handling command line arguments.
        switch (args.length) {

            // Case if option is provided with no input, or incorrect option provided.
            case 1:
                if (args[0].equals("-a")){
                    System.out.println("Missing option -a");
                    System.exit(0);
                } else {
                    System.out.println("Invalid option: " + args[0]);
                    System.exit(0);
                }

            // Case if option is provided with input correct and incorrect.
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