#Job Scheduling Simulator for Distributed Systems

The project has developed a client-side job scheduler that is capable of scheduling jobs across a variety of distributed resources based on the allocation of all jobs to the largest aviliable server.

##Pre-requisites
This program requires the Java Runtime Environment (JRE) to be executed. This can be obtained on a linux operating system by using the following commands:

    sudo apt update
    sudo apt install default-jre

You can check that you have this installed with the following:

    java -version

##Compilation
The compilation of this code can be done either by manually compiling by running the following at the command line:

    cd .../com/company
    javac *.java

Or by running the following:

    cd .../com/company
	compileClient.sh


##Usage
In order to run the simulator ensure that the com folder is located in the same directory as the ds-server application.  To start the simulation the following at the command line:

    cd .../ds-sim
	./ds-server -v brief -c <config file> -n 
	java com.company.Main

Simulation can be run with a specific algorithm using the -a option. Default operation uses the allToLargest algorithm, with first fit (ff), best fit (bf) and worst fit (wf) being specified as provided here:

	cd .../ds-sim
	./ds-server -v brief -c <config file> -n 
	java com.company.Main [-a] <algorithm>

Specifically for each algorithm you would execute the following commands, for allToLargest:

    cd .../ds-sim
	./ds-server -v brief -c <config file> -n
	java com.company.Main

For first-fit:

    cd .../ds-sim
	./ds-server -v brief -c <config file> -n
	java com.company.Main -a ff

For best-fit:

    cd .../ds-sim
	./ds-server -v brief -c <config file> -n
	java com.company.Main -a bf

For worst-fit:

    cd .../ds-sim
	./ds-server -v brief -c <config file> -n
	java com.company.Main -a wf

Or by running the following, which allows the parsing of the client through the testing script:

    cd .../ds-sim
	./ds-server -v brief -c <config file> -n
	runClient.sh <ff/bf/wf>
