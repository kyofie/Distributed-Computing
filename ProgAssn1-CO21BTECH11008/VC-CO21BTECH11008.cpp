#include<bits/stdc++.h>
#include <mpi.h>

using namespace std;

int n,m;
double lambda, alpha;
int internalm;
MPI_Comm comm;
MPI_Status status;
int vClockEntries;


class Node{
    private:
        int id;
        vector<int> vClock;
        mutex mLock; //lock for vClock
        mutex mLogFile; //lock for Log file
        int sent_messages;
        int done_internal;
        vector<int> neighbors;
        unordered_map<int,bool> markers_received;

    public:
        Node(vector<vector<int>> AdjList, vector<vector<int>> markers_to_get, int rank) {
            id = rank;
            vClock.resize(n, 0);
            sent_messages = 0;
            done_internal = 0;
            vClockEntries = 0;
            for (auto it : AdjList[id]) {
                neighbors.push_back(it);
            }
            for(auto processNo : markers_to_get[id]){
                markers_received[processNo] = false;
            }
        }
        void internal_event(){
            done_internal++;

            mLock.lock();
            vClock[id]++;
            mLock.unlock();

            auto now = chrono::system_clock::now();
            auto now_c = chrono::system_clock::to_time_t(now);
            auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;
            // Writing to log file
            mLogFile.lock();
            ofstream logfile;
            logfile.open("output.log", ios::app);
            logfile << "Process" << id + 1 << " executes internal event e" << id+1 << done_internal << " at " 
                   << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << ", vc: ";
            for (int i = 0; i < n; ++i) {
                logfile << vClock[i] << " ";
            }
            logfile << endl;
            logfile.close();
            mLogFile.unlock();
        }
        void send(){

            mLock.lock();
            vClock[id]++;
            mLock.unlock();

            //randomly choose from neighbours to send msg to and get receiver_id
            int randomIndex = rand() % neighbors.size();
            int receiver_id = neighbors[randomIndex]-1;

            //create message with piggybacked vector clock
            int data[n+2];
            data[0] = id; //Sender ID
            for (int i = 1; i <= n; ++i) {
                data[i] = vClock[i-1];
            }
            vClockEntries += n;
            sent_messages++;

            data[n+1] = sent_messages;

            //send message
            MPI_Send(data, n + 2, MPI_INT, receiver_id, 0, comm);

            auto now = chrono::system_clock::now();
            auto now_c = chrono::system_clock::to_time_t(now);
            auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;
            // Writing to log file
            mLogFile.lock();
            ofstream logfile;
            logfile.open("output.log", ios::app);
            logfile << "Process" << id + 1 << " sends message m" << id + 1 << sent_messages << " to process" << receiver_id + 1 << " at "
                    << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << ", vc: ";
            for (int i = 0; i < n; ++i) {
                logfile << vClock[i] << " ";
            }
            logfile << endl;
            logfile.close();
            mLogFile.unlock();
        }
        void send_marker(){
            int data[n+2];
            data[0] = id; //Sender ID
            for (int i = 1; i <= n; ++i) {
                data[i] = vClock[i-1];
            }
            data[n+1]=-1; //indicates marker msg
            for(auto it : neighbors){
                // cout<<id<<" sending marker to "<<it-1<<endl;
                MPI_Send(data, n + 2, MPI_INT, it-1, 0, comm); //sents marker msg to all its neighbors
            }
            ofstream entries;
            entries.open("Vector Clock entries_VC.log", ios::app);
            entries << "Process" << id+1 << " sent a total of "<< vClockEntries  << " vector clock entries\n";
            entries.close();
        }
        void receive() {
            while (true) {
                int data[n + 2];
                // cout << "In receive thread " << id << endl;
                MPI_Recv(data, n + 2, MPI_INT, MPI_ANY_SOURCE, 0, comm, &status);

                int sender_id = data[0];

                if (data[n+1] == -1) {
                    // cout<<id<<" received marker from "<<sender_id<<endl;
                    // Mark the sender as received
                    markers_received[sender_id+1] = true;

                    // Check if all markers have been received
                    bool all_markers_received = true;
                    for (auto& kv : markers_received) {
                        if (!kv.second) {
                            all_markers_received = false;
                            break;
                        }
                    }

                    if (all_markers_received){
                        // cout<<id<<"receiver thread is exiting\n";
                        break;
                    }
                    continue;
                } else {
                    // After receiving the piggybacked vClock, update your own vClock
                    for (int i = 0; i < n; i++) {
                        mLock.lock();
                        vClock[i] = max(data[i + 1], vClock[i]);
                        mLock.unlock();
                        
                    }
                    mLock.lock();
                    vClock[id]++;
                    mLock.unlock();

                    int message_no = data[n + 1];
                    
                    auto now = chrono::system_clock::now();
                    auto now_c = chrono::system_clock::to_time_t(now);
                    auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;
                    // Writing to log file
                    mLogFile.lock();
                    ofstream logfile;
                    logfile.open("output.log", ios::app);
                    logfile << "Process" << id + 1 << " receives message m" << sender_id + 1 << message_no << " from process" << sender_id + 1 << " at "
                            << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << ", vc: ";
                    for (int i = 0; i < n; ++i) {
                        logfile << vClock[i] << " ";
                    }
                    logfile << endl;
                    logfile.close();
                    mLogFile.unlock();
                }
            }
        }
};
Node *Tnode;

void SendNInternal(){   

    random_device rd;
    mt19937 gen(rd());
    exponential_distribution<double> expRandObj(lambda);

    double sleepTime;

    //generate randomly..internal and msg sent
 
    vector<int> arr(m+internalm);

    for(int i=0; i<m; i++){
        arr[i] = 0; 
    }  
    for(int i=m; i<m+internalm; i++){
        arr[i] = 1;
    }

    // To obtain a time-based seed
    unsigned seed = 0;
 
    // Shuffling our array
    int ends = m+internalm;
    shuffle(arr.begin(), arr.end(), default_random_engine(seed));
 
    //The 0s in arr indicate message send event and the 1s in arr indicate internal event
    for(int i=0; i<arr.size(); i++){
        // cout<<arr[i]<<endl;
        if(arr[i]==0){
            //message send event
            Tnode->send();
        }
        if(arr[i]==1){
            //internal event
            Tnode->internal_event();
        }
        sleepTime = expRandObj(gen);
        // cout<<"thread going for sleep\n";
        this_thread::sleep_for(std::chrono::milliseconds(static_cast<long>(sleepTime)));
        // cout<<"thread came out of sleep\n";
    }

    //m messages has been sent 
    Tnode->send_marker(); // sending marker message before termination
    return;
}

void Receive(){
    Tnode->receive();
}

int main(){

    fstream input;
    input.open("inp-params.txt", ios::in);
    input >> n >> lambda >> alpha >> m;

    vector<vector<int>> AdjList(n);

    // Read adjacency list
    string line;
    getline(input, line); // Consume the newline after reading m
    for (int i = 0; i < n; ++i) {
        getline(input, line);
        stringstream ss(line);
        int node;
        while (ss >> node) {
            AdjList[i].push_back(node);
        }
    }
  
    // cout<<alpha<<m<<endl;
    internalm = alpha * m;
    // cout<<"Internalm = "<<internalm;
    // checking the input
    // cout << n << lambda << alpha << m << endl;
    // for(int i=0; i<n; i++){
    //     for(auto it : AdjList[i]){
    //         cout << it << " ";
    //     }
    //     cout<<endl;
    // }

    int rank; //the current process number
    vector<vector<int>> markers_to_get(n);
    for(int i=0; i<n; i++){
        for(auto it : AdjList[i]){
            markers_to_get[it-1].push_back(i+1);
        }
    }
    comm = MPI_COMM_WORLD;
    
    MPI_Init(NULL, NULL);
   
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Check MPI initialization
    if (rank == MPI_ERR_OTHER) {
        cerr << "Error: MPI initialization failed." << endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    //getting a Node for the process
    Tnode = new Node(AdjList,markers_to_get,rank);
    // cout<<"created a Tnode "<<rank<<endl;

    MPI_Barrier(MPI_COMM_WORLD); //wait until every process starts
    
    //each process has a sender thread and a reciever thread
    //the sender thread does message send and internal events
    
    thread sender_thread(SendNInternal);
    thread receiver_thread(Receive); 
   
    sender_thread.join();
    receiver_thread.join();

    MPI_Barrier(MPI_COMM_WORLD); //wait untill every process finishes

    ofstream space;
    space.open("Space Utilization_VC.log",ios::app);
    space<< "Space utilized by process "<<rank+1<<" is "<<n*sizeof(int)<<" Bytes\n";
    space.close();

    int totalEntries = vClockEntries;

    if(rank == 0) {
        int entrie;
        for(int i = 1; i < n; ++i) {
            // Receive entries from other processes
            MPI_Recv(&entrie, 1, MPI_INT, i, 0, comm, MPI_STATUS_IGNORE);
            totalEntries += entrie;
        }
        ofstream entries;
        entries.open("Vector Clock entries_VC.log", ios::app);
        entries << "Total vector clock entries across all processes: " << totalEntries << endl;
        entries.close();
        ofstream space;
        space.open("Space Utilization_VC.log",ios::app);
        space<<"Space utilized for sending messages is "<<totalEntries*sizeof(int)<<" Bytes\n";
        space.close();
    } else {
        // Send your entries to process of rank 0
        MPI_Send(&vClockEntries, 1, MPI_INT, 0, 0, comm);
    }

    MPI_Finalize();
    delete Tnode;
    return 0;
}