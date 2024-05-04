----------------------------------------------------------------
This folder consists of the following files:
 - VC-CO21BTECH11008.cpp
 - SK-CO21BTECH11008.cpp
 - Report_Vector Clocks_CO21BTECH11008.pdf
 - readme.txt
 - inp-params.txt

-----------------------------------------------------------------
'inp-params.txt' is a sample input file. The file contains
the following information. 
The first line contains four numbers (seperated by space)
 - n: number of processes
 - lambda: exponential sleep time between events
 - alpha: ratio of no. of internal events to msg send events
 - m: number of messages to be sent by a processes
The following 'n' lines contain the adjacency list representation
of the graph topology. 

-----------------------------------------------------------------
In this code, I have used MPI for communication between processes
To compile .cpp files
 - mpic++ -o <program> <program-name>.cpp 
To run the executable
 - mpirun -np <n> <program>
Here <n> is the number of processes which should match the value 
of n in the input file "inp-params.txt"

After successful termination of the program the following log files are created

-total vector clock entries of each process along with the entriesfor all the processes 
-real and vector time stamps of individual events of each process. 
-space utilized by each process for vector clocks and also for sending messsages

-----------------------------------------------------------------
