package com.company;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;

// Class for implementing socket communication capability for the scheduling client.
public class ClientSocket {
    //Host name and communication port for socket communication.
    private String hostName;
    private int port;
    private boolean running;

    // Data structures
    //XML STRUCT
    private ArrayList<String[]> resourceList;
    private ArrayList<String[]> serverJobList;


    // Java Socket communication object.
    private Socket client;

    // PrintWriter for parsing outgoing messages and BufferedReader for parsing incoming messages.
    private BufferedWriter outC;
    private BufferedReader inC;

    // Constructor that instantiates the input and output streams as well as connecting to the server.
    public ClientSocket (String hostName, int port){
        this.running = true;
        this.hostName = hostName;
        this.port = port;

        try {
            client = new Socket (hostName, port);
            outC = new BufferedWriter(new OutputStreamWriter(client.getOutputStream()));
            inC = new BufferedReader(new InputStreamReader(client.getInputStream()));
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    // Function for reading incoming messages. Prints the message to the terminal and returns a string
    // containing the message.
    private String readMessage(){
        try {
            String message = inC.readLine();
            System.out.println("RCVD " + message);
            return message;
        } catch(IOException e){
            e.printStackTrace();
        }
        return "message error";
    }

    // Function for sending messages. Prints the message to the terminal.
    private void sendMessage(String message){
        try {
            outC.write(message + "\n");
            outC.flush();
            System.out.println("SENT " + message);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    // Closes the open connection and readers/writers.
    private void stopConnection(){
        try {
            inC.close();
            outC.close();
            client.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public void runClient (){
        this.sendMessage("HELO");
        this.readMessage();

        this.sendMessage("AUTH Group_Two");

        //XML PARSING GOES HERE

        while (this.running){
            this.readMessage();
            this.sendMessage("REDY");

            this.jobSchedule();
        }

    }

    private void jobSchedule(){

        String[] jobInfo = this.readMessage().split("\\s+");

        if (jobInfo[0].equals("NONE")) {
            this.sendMessage("QUIT");
            if(this.readMessage().equals("QUIT")){
                this.stopConnection();
                this.running = false;
            }

        } else {
            if (jobInfo[0].equals("JOBN")) {
                this.sendMessage("RESC All");

                this.resourceList = createDataStruct();

                // ALGORITHM TO CHOOSE SERVER GOES HERE

                // needs "LSTJ <server type> <server number>"
                this.sendMessage("LSTJ large 0");

                this.serverJobList = createDataStruct();

                // ALGORITHM FOR SCHEDULING DECISION GOES HERE

                // needs "SCHD <job number> <server type> <server number>"
                this.sendMessage("SCHD " + jobInfo[2] + " large 0");
            }
        }
    }

    private ArrayList<String[]> createDataStruct() {
        ArrayList<String[]> result = new ArrayList<String[]>();

        if (this.readMessage().equals("DATA")) {
            this.sendMessage("OK");

            boolean transmitting = true;

            while (transmitting) {
                String temp = this.readMessage();

                if (temp.equals(".")) {
                    transmitting = false;
                } else {
                    result.add(temp.split("\\s+"));
                    this.sendMessage("OK");
                }
            }
        }
        return result;
    }
}