package com.company;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;

// Class for implementing the client side communication for the project.
public class ClientSocket {

    //Host name and communication port for socket communication.
    private String hostName;
    private int port;
    private String algorithm;
    private boolean running;

    // Data structures
    private ArrayList<String[]> resourceList;
    private ArrayList<String[]> resourcelistAll;
    private ArrayList<String[]> systemXML;

    // Java Socket communication object.
    private Socket client;

    // PrintWriter for parsing outgoing messages and BufferedReader for parsing incoming messages.
    private BufferedWriter outC;
    private BufferedReader inC;

    // Constructor that instantiates the input and output streams as well as connecting to the server.
    public ClientSocket (String hostName, int port, String algorithm){

        // Sets the client as running and specifying the hostname and port to connect over.
        this.running = true;
        this.hostName = hostName;
        this.port = port;
        this.algorithm = algorithm;

        // Opens the socket connection and created the input and output data streams.
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

    // Main function for running the client and scheduling jobs.
    public void runClient (){

        // Initial authentication protocol
        this.sendMessage("HELO");
        if (this.readMessage().equals("OK")){
            this.sendMessage("AUTH Group2");

            // Parses the system.xml into a data structure to allow for identification of server resources.
            this.readXML();

            // Loop to iterate over the job requests and parse them accordingly.
            while (this.running){

                // Reads the "OK" message from the server and responds with "REDY".
                if(this.readMessage().equals("OK")) {
                    this.sendMessage("REDY");
                    this.jobSchedule();
                }
            }
        }
    }

    // Job scheduling function responsible for parsing messages related to jobs.
    private void jobSchedule(){

        // Takes the inital job request and splits on whitespace into an array of strings.
        String[] jobInfo = this.readMessage().split("\\s+");

        // If the server has sent "NONE" responds with "QUIT" and closes the connection and terminates the client.
        if (jobInfo[0].equals("NONE")) {
            this.sendMessage("QUIT");
            if(this.readMessage().equals("QUIT")){
                this.stopConnection();
                this.running = false;
            }

        } else {
            if (jobInfo[0].equals("JOBN")) {
                String [] serverAllocation;
                switch (this.algorithm){
                    case "atl":
                        serverAllocation = allToLargest();
                        this.sendMessage("SCHD " + jobInfo[2] + " " + serverAllocation[0] + " " + serverAllocation[1]);
                        break;

                    case "ff":
                        //First fit code goes here
                        break;

                    case "bf":
                        serverAllocation = bestFit(jobInfo);
                        if (!serverAllocation[0].equals("NONE")) {
                            this.sendMessage("SCHD " + jobInfo[2] + " " + serverAllocation[0] + " " + serverAllocation[1]);
                        }
                        break;

                    case "wf":
                        //worst fit code goes here
                        break;

                }
            }
        }
    }

    // Parses data sent from the server into an arraylist of string arrays for use in algorithm.
    private ArrayList<String[]> createDataStruct(String message) {
        ArrayList<String[]> result = new ArrayList<String[]>();

        this.sendMessage(message);

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

    // Reads in the system.xml using the SAX parser and stores the information in an arraylist of string arrays.
    private void readXML(){
        systemXML = new ArrayList<String[]>();
        try {
            int index = 0;
            SAXParserFactory fact = SAXParserFactory.newInstance();
            SAXParser saxParser = fact.newSAXParser();

            DefaultHandler handle = new DefaultHandler() {

                @Override
                public void startElement(String uri, String localName,
                                         String qName, Attributes attributes) throws SAXException {

                    String[] temp = new String[7];

                    for (int i = 0; i < attributes.getLength(); i++) {
                        temp[i] = attributes.getValue(i);
                    }
                    if (qName.equals("server")){
                        systemXML.add(temp);
                    }
                }
            };

            saxParser.parse("system.xml", handle);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    // Algorithm to find the largest server by iterating over the system.xml file based on the server core count
    // (measurement of how large the server is) and returns the type of the largest server.
    private String[] allToLargest(){

        int currentSize = 0;
        String type = "";

        for (int i = 0; i < systemXML.size(); i++){
            if (Integer.parseInt(systemXML.get(i)[4]) > currentSize){
                currentSize = Integer.parseInt(systemXML.get(i)[4]);
                type = systemXML.get(i)[0];
            }
        }
        return new String[] {type, "0"};
    }

    private String[] bestFit(String[] jobN) {

        int bestfit = Integer.MAX_VALUE;
        int minAvail = Integer.MAX_VALUE;
        int fitness;
        boolean bestfound = false;

        String[] backupserver = new String[]{"", ""};
        String[] bestserver = new String[]{"", ""};

        this.resourceList = createDataStruct("RESC Avail " + jobN[4] + " " + jobN[5] + " " + jobN[6]);
        this.resourcelistAll = createDataStruct("RESC All");


        if(resourceList.size() > 0) {

            for (int i = 0; i < resourceList.size(); i++) {
                

                if ((Integer.parseInt(resourceList.get(i)[4]) >= Integer.parseInt(jobN[4])) &&
                        (Integer.parseInt(resourceList.get(i)[5]) >= Integer.parseInt(jobN[5])) &&
                        (Integer.parseInt(resourceList.get(i)[6]) >= Integer.parseInt(jobN[6]))) {

                    fitness = Integer.parseInt(jobN[4]) - Integer.parseInt(resourceList.get(i)[4]);


                    if ((fitness < bestfit) || (fitness == bestfit && (Integer.parseInt(resourceList.get(i)[3]) < minAvail))) {

                        bestfit = fitness;
                        minAvail = Integer.parseInt(resourceList.get(i)[3]);
                        bestserver = new String[]{resourceList.get(i)[0], resourceList.get(i)[1]};
                        bestfound = true;


                    }

                }

            }

        } else {

            for (int i = 0; i < resourcelistAll.size(); i++) {



                if ((Integer.parseInt(resourcelistAll.get(i)[4]) >= Integer.parseInt(jobN[4])) &&
                        (Integer.parseInt(resourcelistAll.get(i)[5]) >= Integer.parseInt(jobN[5])) &&
                        (Integer.parseInt(resourcelistAll.get(i)[6]) >= Integer.parseInt(jobN[6]))) {

                    fitness = Integer.parseInt(jobN[4]) - Integer.parseInt(resourcelistAll.get(i)[4]);


                    if ((fitness < bestfit) || (fitness == bestfit && (Integer.parseInt(resourcelistAll.get(i)[3]) < minAvail))) {

                        bestfit = fitness;
                        minAvail = Integer.parseInt(resourcelistAll.get(i)[3]);

                        if(Integer.parseInt(resourcelistAll.get(i)[2]) == 3) {
                            backupserver = new String[]{resourcelistAll.get(i)[0], resourcelistAll.get(i)[1]};
                        }

                    }

                }

            }


        }

        if (bestfound) {
            return bestserver;
        } else {
            return backupserver;
        }
    }

    }

