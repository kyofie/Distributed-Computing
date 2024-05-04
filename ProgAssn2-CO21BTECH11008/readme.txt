----------------------------------------------------------------
This folder consists of the following files:
 - RC-CO21BTECH11008.cpp
 - MK-CO21BTECH11008.cpp
 - Report_ME_CO21BTECH11008.pdf
 - readme.txt
 - inp-params.txt

-----------------------------------------------------------------
'inp-params.txt' is a sample input file. The file contains
the following information. 
The first line contains four numbers (seperated by space)
 - n: number of processes
 - k: number of times each process have to enter CS
 - alpha: for local computation time(exponentially distributed with an average alpha)
 - beta: for CS time(exponentially distributed with an average beta)

-----------------------------------------------------------------
In this code, I have used MPI for communication between processes
To compile .cpp files
 - mpic++ -o <program> <program-name>.cpp 
To run the executable
 - mpirun -np <n> <program>
Here <n> is the number of processes which should match the value 
of n in the input file "inp-params.txt"

After successful termination of the program the following log files are created

-Log file containing the messages regarding entry and exit of CS ("output_RC.log", "output_MK.log")
-Another log file that contains statistics ("Statistics_RC.log", "Statistics_MK.log")

-----------------------------------------------------------------
