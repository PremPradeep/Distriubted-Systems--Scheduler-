package com.company;


public class Main {
    // Main class for the client side simulation, implements the ClientSocket class.
    public static void main(String[] args) {
        ClientSocket c = new ClientSocket("127.0.0.1", 8096);
        c.runClient();
    }
}