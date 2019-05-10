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
import java.util.Comparator;

/**
 * <h1>Client Side Simulator for Distributed Systems</h1>
 * <p>This class consists of methods that operate to implement a client side simulator for scheduling decisions for a
 * distributed network. It implements a number of basic socket communication functions including:
 * <ul>
 *     <li>Establishing a connection to a job server that is responsible for feeding information to the simulator.</li>
 *     <li>Sending messages over the socket.</li>
 *     <li>Receiving Messages over the socket.</li>
 * </ul>
 * <p>In addition to this it also implements a number of scheduling algorithms:
 * <ul>
 *     <li>AllToLargest - As the name implies simply allocates every job to the largest server available. This is
 *     considered the default mode of operation and if no other algorithm is specified will default to this.</li>
 *     <li>FirstFit - This algorithm allocates jobs to the smallest server that is capable of running the job, and in
 *     the event it cannot be allocated simply queues it to be executed on the smallest server that is capable of
 *     running it.</li>
 *     <li>BestFit - This Algorithm calculates a goodness of fit value specified by no. of cores required vs no
 *     available on the server and leverages this against the time that a server is available, and in the event it
 *     cannot be allocated simply queues it to be executed on the smallest server that is capable of running it. This
 *     aims to make use of a large number of small servers to accommodate every job.</li>
 *     <li>WorstFit - This Algorithm calculates a goodness of fit value specified by no. of cores required vs no
 *     available on the server and leverages this against the time that a server is available, and in the event it
 *     cannot be allocated simply queues it to be executed on the smallest server that is capable of running it. This
 *     aims to make use of a small number of large servers to accommodate every job.</li>
 * </ul>
 *
 * @author Nicholas Mangano
 * @version 2.1
 */
public class ClientSocket {

    //Host name and communication port for socket communication.
    private String hostName;
    private int port;
    private String algorithm;
    private boolean running;

    // Data structures
    private ArrayList<String[]> resourceList;
    private ArrayList<String[]> serverJobList;
    private ArrayList<String[]> systemXML;

    // Java Socket communication object.
    private Socket client;

    // PrintWriter for parsing outgoing messages and BufferedReader for parsing incoming messages.
    private BufferedWriter outC;
    private BufferedReader inC;

    /**
     * Class constructor that takes several parameters and sets up the initial socket connection and pre-defines the
     * algorithm to be used for the invocation of the class.
     *
     * @param hostName server host name.
     * @param port port to make the socket connection over.
     * @param algorithm algorithm to be used (atl, ff, bf or wf).
     *
     * @author Nicholas Mangano
     */
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

    /**
     * Socket function for reading incoming messages. Prints the received message to the terminal and returns a String
     * containing the contents of the message.
     *
     * @return message received from the server.
     *
     * @author Nicholas Mangano
     */
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

    /**
     * Socket function for sending messages to the server. Takes the message to be sent as an input and prints the
     * message to the terminal once it has been sent.
     *
     * @param message String to send to the server.
     *
     * @author Nicholas Mangano
     */
    private void sendMessage(String message){
        try {
            outC.write(message + "\n");
            outC.flush();
            System.out.println("SENT " + message);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Socket function for closing the open connection and terminating the readers and writers that are used to buffer
     * incoming and outgoing messages.
     *
     * @author Nicholas Mangano
     */
    private void stopConnection(){
        try {
            inC.close();
            outC.close();
            client.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Main function responsible for running the scheduler. Operates based on the protocol discussed in the
     * documentation provided and implements many of the socket communication protocols and helper functions for
     * running the algorithms.
     */
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

    /**
     * Main function for scheduling jobs, runs for every job request received from the server and implements one of
     * four algorithms described above.
     */
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
                        serverAllocation = firstFit(jobInfo);
                        this.sendMessage("SCHD " + jobInfo[2] + " " + serverAllocation[0] + " " + serverAllocation[1]);
                        break;

                    case "bf":
                        serverAllocation = bestFit(jobInfo);
                        this.sendMessage("SCHD " + jobInfo[2] + " " + serverAllocation[0] + " " + serverAllocation[1]);
                        break;

                    case "wf":
                        serverAllocation = worstFit(jobInfo);
                        this.sendMessage("SCHD " + jobInfo[2] + " " + serverAllocation[0] + " " + serverAllocation[1]);
                        break;

                }
            }
        }
    }

    /**
     * Function responsible for reacting data structures out of message requests for resources or resource availability
     * as defined in the communication protocol. Takes as input a message of the form "RESC" or "LSTJ" with their proper
     * fields and returns an array list of String arrays which contains either a resource list or a list of jobs running
     * on a server.
     *
     * @param message controls what data the server sends.
     * @return an array list of String arrays that contains requested data.
     *
     * @author Nicholas Mangano
     */
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

    /**
     * Function responsible for reading in the system.xml file. Makes use of a SAC parser and stores the data into the
     * class level variable systemXML as an array list of string arrays tokenised on whitespace.
     *
     * @author Nicholas Mangano
     */
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

    /**
     * Algorithm to find the largest server available and allocate all jobs to it. Iterates over the system.xml data
     * and determines size based on the servers core count. Returns the first largest servers type and index.
     *
     * @return largest server type and index.
     *
     * @author Nicholas Mangano
     */
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

    /**
     * One of the main algorithm functions for the client side simulator. Returns a a server scheduling decision for a
     * given job in the form of a String array of size two with the String at index 0 being the server type (e.g. "Small")
     * and the String at index two being a non-negative integer (e.g. 0, 1, ... n).
     * <p>
     * This function will attempt to allocate the job to the smallest server that the job can be run on, iterating over
     * decisions from smallest to largest. In the event the job cannot be run on any server schedules it to be preformed
     * on the smallest server whose initial resources meet the requirements of the job.
     *
     * @param jobN a job to be scheduled in the format "JOBN submit_time job_ID est_runtime cores memory disk".
     * @return schedulingDecision a string array that contains the server type and server index.
     *
     * @author Nicholas Mangano
     */
    private String[] firstFit(String[] jobN){

        // Creates variables to keep track of which server to allocate to in the event all servers are active and
        // cannot fit the job.
        String [] firstFitActiveServer = new String[] {"", ""};
        boolean initialRun = true;

        // Sorts systemXml in ascending order based on core count.
        Collections.sort(this.systemXML, new Comparator<String[]>() {
            public int compare(String[] string, String[] otherString) {
                return Integer.parseInt(string[4]) - Integer.parseInt(otherString[4]);
            }
        });

        // Iterates over all the server types from smallest to largest which is achievable as systemXML has been sorted
        // in ascending order.
        for (int i = 0; i < systemXML.size(); i++){

            // Checks if the job is capable of fitting on the server type assuming it has no other jobs running to pre
            // check what server types the job can fit onto to reduce calls to the server for resource information.
            if ((Integer.parseInt(systemXML.get(i)[4]) >= Integer.parseInt(jobN[4])) &&
                    (Integer.parseInt(systemXML.get(i)[5]) >= Integer.parseInt(jobN[5]))&&
                    (Integer.parseInt(systemXML.get(i)[6]) >= Integer.parseInt(jobN[6]))){

                // Sends a message to the server for the resource information for the given server size and creates an
                // array list of string arrays with each item in the array list being one server of type the queried
                // type.
                this.resourceList = createDataStruct("RESC Type " + systemXML.get(i)[0]);

                // Iterates over the resource list to attempt to fit the job onto a server.
                for (int k = 0; k < this.resourceList.size(); k++){

                    // Captures the smallest server that the job can be run on to be returned in the event all servers
                    // are currently active and cannot fit the job.
                    if (initialRun && Integer.parseInt(resourceList.get(k)[2]) == 3) {
                        firstFitActiveServer = new String[]{systemXML.get(i)[0], "0"};
                        initialRun = false;
                    }

                    if ((Integer.parseInt(resourceList.get(k)[4]) >= Integer.parseInt(jobN[4])) &&
                            (Integer.parseInt(resourceList.get(k)[5]) >= Integer.parseInt(jobN[5]))&&
                            (Integer.parseInt(resourceList.get(k)[6]) >= Integer.parseInt(jobN[6]))){

                        // Returns the server.
                        return new String[] {resourceList.get(k)[0], resourceList.get(k)[1]};
                    }
                }
            }
        }
        // If it cannot fit the job on a server allocates it to the first active server with the minimum initial
        // resources to run the job.
        return firstFitActiveServer;
    }

    /**
     * One of the main algorithm functions for the client side simulator. Returns a a server scheduling decision for a
     * given job in the form of a String array of size two with the String at index 0 being the server type (e.g. "Small")
     * and the String at index two being a non-negative integer (e.g. 0, 1, ... n).
     * <p>
     * This function will attempt to allocate the job to the the best fit server that the job can be run on, iterating
     * over decisions based on location in systemxml. In the event the job cannot be run on any server schedules it to
     * be performed on the best fit active server whose initial resources meet the requirements of the job.
     *
     * @param jobN a job to be scheduled in the format "JOBN submit_time job_ID est_runtime cores memory disk".
     * @return schedulingDecision a string array that contains the server type and server index.
     *
     * @author Nicholas Mangano
     */
    private String[] bestFit(String[] jobN){

        // Creates variables to keep track fitness and which server to allocate to in the event all servers are active
        // and cannot fit the job.
        int bestFitAlt = Integer.MAX_VALUE;
        int bestFit  = Integer.MAX_VALUE;
        int minAvail = Integer.MAX_VALUE;

        boolean bestFitFound = false;
        boolean initialRun = true;

        int fitnessAlt;
        int fitness;
        int avail;

        String [] bestFitServer = new String[] {"Error", "Error"};
        String [] bestFitActiveServer = new String[] {"Error", "Error"};

        // Iterates over all the server types in the order listed in system.XML.
        for (int i = 0; i < systemXML.size(); i++){
            initialRun = true;

            // Checks if the job is capable of fitting on the server type assuming it has no other jobs running to pre
            // check what server types the job can fit onto to reduce calls to the server for resource information.
            if ((Integer.parseInt(systemXML.get(i)[4]) >= Integer.parseInt(jobN[4])) &&
                    (Integer.parseInt(systemXML.get(i)[5]) >= Integer.parseInt(jobN[5]))&&
                    (Integer.parseInt(systemXML.get(i)[6]) >= Integer.parseInt(jobN[6]))){

                // Sends a message to the server for the resource information for the given server size and creates an
                // array list of string arrays with each item in the array list being one server of type the queried
                // type.
                this.resourceList = createDataStruct("RESC Type " + systemXML.get(i)[0]);

                // Iterates over the resource list to attempt to fit the job onto a server.
                for (int k = 0; k < this.resourceList.size(); k++){

                    // Determines the best fit active server in the event that we cannot fit the job on another server.
                    fitnessAlt = Integer.parseInt(systemXML.get(i)[4]) - Integer.parseInt(jobN[4]);
                    if (initialRun && (Integer.parseInt(resourceList.get(k)[2]) == 3) && (fitnessAlt < bestFitAlt)) {
                        bestFitAlt = fitnessAlt;
                        bestFitActiveServer = new String[]{resourceList.get(k)[0], resourceList.get(k)[1]};
                        initialRun = false;
                    }

                    // Checks if the job is capable of fitting on the specific server.
                    if ((Integer.parseInt(resourceList.get(k)[4]) >= Integer.parseInt(jobN[4])) &&
                            (Integer.parseInt(resourceList.get(k)[5]) >= Integer.parseInt(jobN[5]))&&
                            (Integer.parseInt(resourceList.get(k)[6]) >= Integer.parseInt(jobN[6]))){

                        // Calculates the fitness value and pulls out available time for easy comparision.
                        fitness = Integer.parseInt(resourceList.get(k)[4]) - Integer.parseInt(jobN[4]);
                        avail = Integer.parseInt(resourceList.get(k)[3]);

                        // If the server is the best fit, save the server info and update the comparators.
                        if ((fitness < bestFit) || ((fitness == bestFit) && (avail < minAvail))){
                            bestFit = fitness;
                            minAvail = avail;
                            bestFitFound = true;
                            bestFitServer = new String[] {this.resourceList.get(k)[0], this.resourceList.get(k)[1]};
                        }
                    }
                }
            }
        }
        // Return the best fit server or the best fit active server in that precedence.
        if (bestFitFound){
            return bestFitServer;

        } else {
            return bestFitActiveServer;

        }
    }


    /**
     * One of the main algorithm functions for the client side simulator. Returns a a server scheduling decision for a
     * given job in the form of a String array of size two with the String at index 0 being the server type (e.g. "Small")
     * and the String at index two being a non-negative integer (e.g. 0, 1, ... n).
     * <p>
     * This function will attempt to allocate the job to the the worst fit server that the job can be run on, iterating
     * over decisions based on location in systemxml. In the event the job cannot be run on any server schedules it to
     * be performed on the worst fit active server whose initial resources meet the requirements of the job.
     *
     * @param jobN a job to be scheduled in the format "JOBN submit_time job_ID est_runtime cores memory disk".
     * @return schedulingDecision a string array that contains the server type and server index.
     *
     * @author Nicholas Mangano
     */
    private String[] worstFit(String[] jobN){

        int worstFitAlt  = Integer.MIN_VALUE;
        int worstFit  = Integer.MIN_VALUE;
        int altFit = Integer.MIN_VALUE;

        boolean worstFitFound = false;
        boolean altFitFound = false;
        boolean initialRun = true;

        int fitnessAlt;
        int fitness;

        String [] worstFitServer = new String[] {"1", "1"};
        String [] altFitServer = new String[] {"2", "2"};
        String [] worstFitActiveServer = new String[] {"3", "3"};

        for (int i = 0; i < systemXML.size(); i++){
            initialRun = true;

            // Checks if the job is capable of fitting on the server type assuming it has no other jobs running to pre
            // check what server types the job can fit onto to reduce calls to the server for resource information.
            if ((Integer.parseInt(systemXML.get(i)[4]) >= Integer.parseInt(jobN[4])) &&
                    (Integer.parseInt(systemXML.get(i)[5]) >= Integer.parseInt(jobN[5]))&&
                    (Integer.parseInt(systemXML.get(i)[6]) >= Integer.parseInt(jobN[6]))){

                // Sends a message to the server for the resource information for the given server size and creates an
                // array list of string arrays with each item in the array list being one server of type the queried
                // type.
                this.resourceList = createDataStruct("RESC Type " + systemXML.get(i)[0]);

                // Iterates over the resource list to attempt to fit the job onto a server.
                for (int k = 0; k < this.resourceList.size(); k++){

                    // Determines the worst fit active server in the event that we cannot fit the job on another server.
                    fitnessAlt = Integer.parseInt(systemXML.get(i)[4]) - Integer.parseInt(jobN[4]);
                    if (initialRun && (Integer.parseInt(resourceList.get(k)[2]) == 3) && (fitnessAlt > worstFitAlt)) {
                        worstFitAlt = fitnessAlt;
                        worstFitActiveServer = new String[]{resourceList.get(k)[0], resourceList.get(k)[1]};
                        initialRun = false;
                    }

                    // Checks if the job is capable of fitting on the specific server.
                    if ((Integer.parseInt(resourceList.get(k)[4]) >= Integer.parseInt(jobN[4])) &&
                            (Integer.parseInt(resourceList.get(k)[5]) >= Integer.parseInt(jobN[5]))&&
                            (Integer.parseInt(resourceList.get(k)[6]) >= Integer.parseInt(jobN[6]))) {

                        //Filters out servers that are booting from the server lists.
                        if (Integer.parseInt(resourceList.get(k)[2]) != 1 ||
                                Integer.parseInt(resourceList.get(k)[2]) != 1) {

                            // Calculates the fitness value.
                            fitness = Integer.parseInt(resourceList.get(k)[4]) - Integer.parseInt(jobN[4]);

                            // If the server is the worst fit or the alt fit, save the server info and update the
                            // comparators.
                            if ((fitness > worstFit) &&
                                    ((Integer.parseInt(resourceList.get(k)[3]) == (Integer.parseInt(jobN[1])) ||
                                            (Integer.parseInt(resourceList.get(k)[3]) == -1)))) {
                                worstFit = fitness;
                                worstFitFound = true;
                                worstFitServer = new String[]{this.resourceList.get(k)[0], this.resourceList.get(k)[1]};
                            } else if ((fitness > altFit) && (Integer.parseInt(resourceList.get(k)[2]) < 3)) {
                                altFit = fitness;
                                altFitFound = true;
                                altFitServer = new String[]{this.resourceList.get(k)[0], this.resourceList.get(k)[1]};

                            }
                        }
                    }
                }
            }
        }
        // Return the worst fit server, the alt fit server or the worst fit active server in that precedence.
        if (worstFitFound){
            return worstFitServer;

        } else if (altFitFound){
            return altFitServer;

        } else {
            return worstFitActiveServer;

        }
    }
}