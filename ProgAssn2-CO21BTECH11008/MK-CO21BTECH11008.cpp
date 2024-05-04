#include<bits/stdc++.h>
#include <mpi.h>

using namespace std;

typedef pair<int, int> pi; 

int n,k;
double alph, bta;

int timestamp = 0;
mutex TimeLock; //lock for timestamp

MPI_Comm comm;
MPI_Status status;

atomic<bool> inCS(false);
atomic<int> messagesExc(0);

vector<int> getQuorumMembers(int id){
    int p = sqrt(n);
    int row = id / p;
    int col = id % p;

    vector<int> Quorum;

    for(int i = 0; i < p; i++){
        if(i * p + col != id){
            Quorum.push_back(i*p + col);
        }
        if(row * p + i != id){
            Quorum.push_back(row * p + i);
        }
    }
    return Quorum;
}

void receive(int rnk, vector<int> quorum){

    int markers = 0;
    int reqGrants = quorum.size();
    vector<int> grantSet;
    vector<int> failedSet;
    priority_queue<pi, vector<pi>, greater<pi> > requests; 

    while(true){
        int recMessage[2];
        MPI_Recv(recMessage, 2, MPI_INT, MPI_ANY_SOURCE, 0, comm, &status);

        int msgTimestamp = recMessage[0];
        int msgType = recMessage[1];
        int sender = status.MPI_SOURCE;
        

        messagesExc.fetch_add(1);
        //update timestamp
        TimeLock.lock();
        timestamp = max(timestamp,msgTimestamp)+1;
        TimeLock.unlock();

        if(msgType == -1){
            //a REQUEST message
            // cout<<rnk<<" a REQ from"<<sender<<endl;

            if(requests.empty() ){
                //send grant msg
                TimeLock.lock();
                timestamp++;
                TimeLock.unlock();
                int message[2];
                message[0] = timestamp;
                message[1] = -2;
                MPI_Send(message, 2, MPI_INT, sender, 0, comm);
                messagesExc.fetch_add(1);
            }
            
            else{
                auto prevMsg = requests.top();
                if(make_pair(msgTimestamp,sender)<prevMsg){
                    //send INQUIRE message
                    TimeLock.lock();
                    timestamp++;
                    TimeLock.unlock();

                    int message[2];
                    message[0] = timestamp;
                    message[1] = -4;
                    int id = prevMsg.second;yie
                    MPI_Send(message, 2, MPI_INT, id, 0, comm);
                    messagesExc.fetch_add(1);
                }
                else{
                    //send FAILED message
                    TimeLock.lock();
                    timestamp++;
                    TimeLock.unlock();

                    int message[2];
                    message[0] = timestamp;
                    message[1] = -6;
                    int id = prevMsg.second;
                    MPI_Send(message, 2, MPI_INT, id, 0, comm);
                    messagesExc.fetch_add(1);
                }

            }
            //push into priority queue (timestamp,sender)
            requests.push(make_pair(msgTimestamp,sender));
        }
        else if(msgType == -2){
            // a GRANT message
            // cout<<rnk<<" a GRANT from"<<sender<<endl;
            grantSet.push_back(sender);

            //delete from failed set
            auto it = std::find(failedSet.begin(), failedSet.end(), sender);

            if (it != failedSet.end()) {
                failedSet.erase(it);
            } 
            
            bool gotall = true;
            for(auto qm : quorum){
                auto it = find(grantSet.begin(), grantSet.end(), qm);
                if(it == grantSet.end()){
                    gotall = false;
                }
            }
            if(gotall == true){
                inCS.store(true);
                grantSet.clear();
            }
            
        }
        else if(msgType == -3){
            // a RELEASE message
            // cout<<rnk<<" a REL from"<<sender<<endl;
            //send grant to the next higher prioirty process
            TimeLock.lock();
            timestamp++;
            TimeLock.unlock();

            int message[2];
            message[0] = timestamp;
            message[1] = -2;

            priority_queue<pi, vector<pi>, greater<pi>> updatedRequests;

            while (!requests.empty()) {
                if (requests.top().second != sender) {
                    updatedRequests.push(requests.top());
                }
                requests.pop();
            }
            
            requests = updatedRequests;

            //get highP from prioirty queue if empty do nothing
            if(!requests.empty()){
                int highP = requests.top().second;
                //send grant
                MPI_Send(message, 2, MPI_INT, highP, 0, comm);
                messagesExc.fetch_add(1);
            }
        }
        else if(msgType == -4){
            // an INQUIRE message
            cout<<rnk<<" a INQ from"<<sender<<endl;
            
            if(grantSet.size()<reqGrants || failedSet.size()>0){
                //send yield
                TimeLock.lock();
                timestamp++;
                TimeLock.unlock();
                int message[2];
                message[0] = timestamp;
                message[1] = -5;
                MPI_Send(message, 2, MPI_INT, sender, 0, comm);
                messagesExc.fetch_add(1);
            }
        }
        else if(msgType == -5){
            // an YIELD message
            cout<<rnk<<" a YIELD from"<<sender<<endl;
            //send grant to the higher prioirty process
            TimeLock.lock();
            timestamp++;
            TimeLock.unlock();
            int message[2];
            message[0] = timestamp;
            message[1] = -2;

            //get highP from prioirty queue
            if(!requests.empty()){
                int highP = requests.top().second;
                // cout<<"High P"<<highP<<endl;
                MPI_Send(message, 2, MPI_INT, highP, 0, comm);
                messagesExc.fetch_add(1);
            }
        }
        else if(msgType == -6){
            // a FAILED message
            cout<<rnk<<" a FAIL from"<<sender<<endl;
            failedSet.push_back(sender);
        }
        else{
            // a marker message
            markers++;
            // cout<<rnk<<" Received marker from "<<sender<<endl;
            if(markers == n){
                //exit
                return;
            }
        }
    }
}

void reqCS(int rnk, vector<int> quorum){

    //update timestamp
    TimeLock.lock();
    timestamp++;
    TimeLock.unlock();

    int Qmessage[2];
    Qmessage[0] = timestamp;
    Qmessage[1] = -1; // -1 indicates a REQUEST message

    //send request to all the quorum members

    for(auto id : quorum){
        MPI_Send(Qmessage, 2, MPI_INT, id, 0, comm);
        messagesExc.fetch_add(1);
    }

    while(!inCS.load()){

    }
    // cout<<rnk<<"enters CS\n";
    return;
}

void relCS(int rnk, vector<int> quorum){
    // cout<<rnk<<"exits CS\n";
    inCS.store(false);
    for(auto id : quorum){
        int message[2];
        message[0] = timestamp;
        message[1] = -3;
        //send release message
        MPI_Send(message, 2, MPI_INT, id, 0, comm);
        messagesExc.fetch_add(1);
    }
}

void working(int rnk, vector<int> quorum){

    random_device rd;
    mt19937 gen(rd());
    exponential_distribution<double> expRandObj1(alph);
    exponential_distribution<double> expRandObj2(bta);

    for(int i=0; i<k; i++){

        double outCSTime = expRandObj1(gen);
        double inCSTime = expRandObj2(gen);

        // Local Processing

        auto now = chrono::system_clock::now();
        auto now_c = chrono::system_clock::to_time_t(now);
        auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;

        ofstream logfile;
        logfile.open("output_MK.log", ios::app);
        logfile << "Process" << rnk + 1 << " is doing local computation at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << endl; 
        logfile.close();

        this_thread::sleep_for(std::chrono::milliseconds(static_cast<long>(outCSTime)));

        //Requesting the CS
        now = chrono::system_clock::now();
        now_c = chrono::system_clock::to_time_t(now);
        now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;

        logfile.open("output_MK.log", ios::app);
        logfile << "Process" << rnk + 1 << " requests to enter CS at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << " for the "<< i+1 << "th time" << endl; 
        logfile.close();

        reqCS(rnk, quorum);
    
        //Inside the CS
        now = chrono::system_clock::now();
        now_c = chrono::system_clock::to_time_t(now);
        now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;


        logfile.open("output_MK.log", ios::app);
        logfile << "Process" << rnk + 1 << " enters CS at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << endl; 
        logfile.close();

        this_thread::sleep_for(std::chrono::milliseconds(static_cast<long>(inCSTime)));

        //Releasing the CS
        now = chrono::system_clock::now();
        now_c = chrono::system_clock::to_time_t(now);
        now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;


        logfile.open("output_MK.log", ios::app);
        logfile << "Process" << rnk + 1 << " leaves CS at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << " for the "<< i+1 << "th time" << endl;  
        logfile.close();

        relCS(rnk, quorum);
        
    }
    
    //send marker msg 
    for(int id = 0; id < n; id++){
        TimeLock.lock();
        timestamp++;
        TimeLock.unlock();
        int Mmessage[2];
        Mmessage[0] = timestamp;
        Mmessage[1] = -7; // -7 indicates a Marker message
        MPI_Send(Mmessage, 2, MPI_INT, id, 0, comm);
    }
    return;
}

int main(){

    fstream input;
    input.open("inp-params.txt", ios::in);

    if (!input) {
        cout << "File couldn't be opened\n";
    }
    else{
        input >> n >> k >> alph >> bta;
        input.close();
    }  

    // Checking input
    // cout<< n << k << alph << bta << endl;

    int rank;

    comm = MPI_COMM_WORLD;

    // MPI_Init(NULL, NULL);

    int provided;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);


    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    vector<int> quorum = getQuorumMembers(rank);

    //checking quorum
    // for(auto it : quorum){
    //     cout<<it<<" ";
    // }
    // cout<<rank<<"checking quorum done\n";

    thread working_thread(working, rank, quorum);
    thread receive_thread(receive, rank, quorum);

    working_thread.join();
    receive_thread.join();

    ofstream StatsFile;
    StatsFile.open("Statistics_MK.log", ios::app);
    StatsFile<<"Process "<<rank<<" exchanged "<<messagesExc.load()<<endl;
    StatsFile.close();

    if(rank==0){
        int totalmessages = messagesExc.load();
        int Nmessages[1];
        for(int i=1; i<n; i++){
            MPI_Recv(Nmessages, 1, MPI_INT, i, 0, comm, MPI_STATUS_IGNORE);
            totalmessages += Nmessages[0];
        }
        ofstream StatsFile;
        StatsFile.open("Statistics_MK.log", ios::app);
        StatsFile<<"Average number of messages exchanged per critical section entry is "<<(double)totalmessages/(double)(n*k)<<endl;
        StatsFile.close();
    }
    else{
        int Nmessages[1] = {messagesExc.load()};
        MPI_Send(Nmessages, 1, MPI_INT, 0, 0, comm);
    }

    MPI_Finalize();


    return 0;
}