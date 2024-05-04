#include<bits/stdc++.h>
#include <mpi.h>

using namespace std;

int n,m;
double lambda, alpha;
int internalm;
MPI_Comm comm;
MPI_Status status;
int vClockEntries;


string intArrayToString(vector<int> arr) {
    string rv = "";
    for (auto it: arr) rv += to_string(it) + " ";
    return rv;
}

vector<int> stringToIntArray(string str) {
    vector<int> arr;
    istringstream iss(str);
    int num;
    while (iss >> num) {
        arr.push_back(num);
    }
    return arr;
}


class Node{
    private:
        int id;
        vector<int> vClock;
        vector<int> LS;
        vector<int> LU;
        mutex mLockv; //lock for vClock
        mutex mLockLU; //lock for LU
        mutex mLogFile; //lock for Log file
        int sent_messages;
        int done_internal;
        vector<int> neighbors;
        unordered_map<int,bool> markers_received;

    public:
        Node(vector<vector<int>> AdjList,vector<vector<int>> markers_to_get, int rank) {
            id = rank;
            vClock.resize(n, 0);
            LS.resize(n,0);
            LU.resize(n,0);
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

            mLockv.lock();
            vClock[id]++;
            mLockv.unlock();

            // Update LU[i]
            mLockLU.lock();
            LU[id] = vClock[id];
            mLockLU.unlock();

            
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
            mLockv.lock();
            vClock[id]++;
            mLockv.unlock();

            //randomly choose from neighbours to send msg to and get receiver_id
            int randomIndex = rand() % neighbors.size();
            int receiver_id = neighbors[randomIndex]-1;

            mLockLU.lock();
            LU[id] = vClock[id];
            mLockLU.unlock();

            // Create message with piggybacked vector clock differentials
            vector<pair<int, int>> differentials;
            for (int i = 0; i < n; ++i) {
                if (LS[receiver_id] < LU[i]) {
                    differentials.push_back({i, vClock[i]});
                }
            }

            sent_messages++;

            vClockEntries += differentials.size();

            // Add message number and sender ID
            differentials.push_back({-1, sent_messages}); // Message number
            differentials.push_back({-2, id});            // Sender ID

            vector<int> data;
            for(auto p : differentials){
                data.push_back(p.first);
                data.push_back(p.second);
            }
            
            // Send message
            string messageString = intArrayToString(data);
            // cout<<"message being sent="<<messageString<<endl;
            // cout<<"Message sent to "<<receiver_id<<endl;
            MPI_Send(messageString.c_str(), messageString.size()+1, MPI_CHAR, receiver_id, 0, comm);

            // Update LS[j]
            LS[receiver_id] = vClock[id];

            
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
            // Create message with piggybacked vector clock differentials
            vector<pair<int, int>> differentials;
            
            // Add marker flag
            differentials.push_back({-1, -1}); // Marker flag
            differentials.push_back({-2, id}); // Sender ID

            vector<int> data;
            for(auto p : differentials){
                data.push_back(p.first);
                data.push_back(p.second);
            }
            string messageString = intArrayToString(data);
            // cout<<messageString<<endl;
            // cout<<"marker Message sent\n";
            // Send marker to all neighbors
            for (auto neighbor : neighbors) {
                MPI_Send(messageString.c_str(), messageString.size()+1, MPI_CHAR, neighbor - 1, 0, comm);
                // cout<<id+1<<" sending marker to "<<neighbor-1<<endl;
            }
            ofstream entries;
            entries.open("Vector Clock entries_SK.log", ios::app);
            entries << "Process" << id+1 << " sent a total of "<< vClockEntries  << " vector clock entries\n";
            entries.close();
        }
        void receive() {
            while (true) {

                char received_data[1000];
                MPI_Recv(received_data, 1000, MPI_CHAR, MPI_ANY_SOURCE, 0, comm, &status);
                string message(received_data);
                // std::cout << message << "mmm\n";
                vector<int> Rdata = stringToIntArray(message);
                
                // for(int i=0; i<Rdata.size(); i++){
                //     cout<<Rdata[i]<<endl;
                // }
                // cout<<"message received\n";
            
                int sender_id;
                int message_no;
                
                unordered_map<int,int> PiggyClock;
                for(int i=0; i<Rdata.size(); i+=2){
                    // cout<<"Rdata\n";
                    // cout<<Rdata[i]<<" "<<Rdata[i+1]<<endl;
                    if(Rdata[i]==-1){
                        message_no = Rdata[i+1];
                    }
                    if(Rdata[i]==-2){
                        sender_id = Rdata[i+1];
                    }
                    if(Rdata[i]>-1){
                        // cout<<"Rdata\n";
                        // cout<<Rdata[i]<<" "<<Rdata[i+1]<<endl;
                        PiggyClock[Rdata[i]] = Rdata[i+1];
                    }
                }
                // cout<<message_no<<"message_n0 in "<<id<<endl;
                // cout<<sender_id<<"sender_id\n";
                if (message_no==-1) {
                    // Mark the sender as received
                    markers_received[sender_id+1] = true;
                    // cout<<id<<" received marker from "<<sender_id<<endl;

                    // Check if all markers have been received
                    bool all_markers_received = true;
                    for (auto& kv : markers_received) {
                        if (!kv.second) {
                            all_markers_received = false;
                            break;
                        }
                    }

                    if (all_markers_received){
                        // cout<<"Exiting="<<id<<endl;
                        break;
                    }
                    continue;
                } else {
                    // After receiving the piggybacked vClock, update your own vClock
                    for(auto it : PiggyClock){
                        // cout<<it.first<<endl;
                        vClock[it.first] = max(vClock[it.first],it.second);
                        LU[it.first] = vClock[it.first];
                    }
                    // cout<<"Above are \n";
                    mLockv.lock();
                    vClock[id]++;
                    mLockv.unlock();
                    
                    mLockLU.lock();
                    LU[id] = vClock[id];
                    mLockLU.unlock();

                    
                    auto now = chrono::system_clock::now();
                    auto now_c = chrono::system_clock::to_time_t(now);
                    auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;
                    // Writing to log file
                    mLogFile.lock();
                    ofstream logfile;
                    logfile.open("output_SK.log", ios::app);
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
// Node *Tnode;

void SendNInternal(Node *Tnode){   

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

void Receive(Node *Tnode){
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

    internalm = (int)(alpha * m);
   
    // //checking the input
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

    //getting a Node for the process
    Node *Tnode = new Node(AdjList,markers_to_get,rank);

    MPI_Barrier(MPI_COMM_WORLD); //wait until every process starts
    
    //each process has a sender thread and a reciever thread
    //the sender thread does message send and internal events
    
    thread sender_thread(SendNInternal, Tnode);
    thread receiver_thread(Receive, Tnode); 
   
    sender_thread.join();

    receiver_thread.join();
    // cout<<"Every thread joined="<<rank<<endl;

    MPI_Barrier(MPI_COMM_WORLD); //wait untill every process finishes

    ofstream space;
    space.open("Space Utilization_SK.log",ios::app);
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
        entries.open("Vector Clock entries_SK.log", ios::app);
        entries << "Total vector clock entries across all processes: " << totalEntries << endl;
        entries.close();
        ofstream space;
        space.open("Space Utilization_SK.log",ios::app);
        space<<"Space utilized for sending messages is "<<totalEntries*sizeof(int)<<" Bytes\n";
        space.close();
    } else {
        // Send your entries to process of rank 0
        MPI_Send(&vClockEntries, 1, MPI_INT, 0, 0, comm);
    }

    // cout << "Finalizing rank = " << rank << "\n";
    MPI_Finalize();
    delete Tnode;

    return 0;
}