#include<bits/stdc++.h>
#include <mpi.h>

using namespace std;

int n,k;
double alph, bta;

int timestamp = 0;
int CSTimestamp = 0;
mutex TimeLock; //Lock for timestamp

MPI_Comm comm;
MPI_Status status;

vector<int> RD; // contains deferred requests
vector<int> SendReq; // processes we send requests to
vector<int> SentRep; // to reinitialize SendReq

atomic<bool> inCS(false);
atomic<bool> requesting(false);
atomic<int> messagesExc(0);

void receive(int rnk){

    int markers = 0;

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
            // REQUEST msg
            // cout<<rnk<<" received REQUEST from"<<sender<<endl;
            auto now = chrono::system_clock::now();
            auto now_c = chrono::system_clock::to_time_t(now);
            auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;

            ofstream logfile;
            logfile.open("output_RC.log", ios::app);
            logfile << "Process" << rnk + 1 << " receives Process" << sender+1 << "'s request to enter CS at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << endl; 
            logfile.close();

            if(inCS.load()){
                // cout<<rnk<<"deffered in CS"<<sender<<endl;
                RD[sender] = 1;
                
            }
            else if(requesting.load()){
                // cout<<rnk<<"in requesting else if\n";
                if(msgTimestamp < CSTimestamp){
                    
                    // if(SendReq[sender]==0){
                    //     // since it is in requesting phase send request
                    //     TimeLock.lock();
                    //     timestamp++;
                    //     TimeLock.unlock();
                    //     int Qmessage[2];
                    //     Qmessage[0] = timestamp;
                    //     Qmessage[1] = -1; // -2 indicates a REQUEST message

                    //     MPI_Send(Qmessage, 2, MPI_INT, sender, 0, comm);
                    //     SendReq[sender] = 1;
                    // }

                    // send REPLY
                    TimeLock.lock();
                    timestamp++;
                    TimeLock.unlock();

                    int Pmessage[2];
                    Pmessage[0] = timestamp;
                    Pmessage[1] = -2; // -2 indicates a REPLY message
                    // cout<<rnk<<"sending reply to"<<sender<<endl;
                    MPI_Send(Pmessage, 2, MPI_INT, sender, 0, comm);
                    messagesExc.fetch_add(1);
                    SentRep[sender] = 1;

                    auto now = chrono::system_clock::now();
                    auto now_c = chrono::system_clock::to_time_t(now);
                    auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;

                    ofstream logfile;
                    logfile.open("output_RC.log", ios::app);
                    logfile << "Process" << rnk + 1 << " reply to Process" << sender+1 << "'s request to enter CS at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << endl; 
                    logfile.close();

                }
                else if(msgTimestamp >= CSTimestamp){
                    if(msgTimestamp == CSTimestamp && rnk>sender){

                        // if(SendReq[sender]==0){
                        //     // since it is in requesting phase send request
                        //     TimeLock.lock();
                        //     timestamp++;
                        //     TimeLock.unlock();
                        //     int Qmessage[2];
                        //     Qmessage[0] = timestamp;
                        //     Qmessage[1] = -1; // -2 indicates a REQUEST message

                        //     MPI_Send(Qmessage, 2, MPI_INT, sender, 0, comm);
                        //     SendReq[sender] = 1;
                        // }

                        // send REPLY
                        TimeLock.lock();
                        timestamp++;
                        TimeLock.unlock();
                        int Pmessage[2];
                        Pmessage[0] = timestamp;
                        Pmessage[1] = -2; // -2 indicates a REPLY message
                        // cout<<rnk<<"sending reply to"<<sender<<endl;
                        MPI_Send(Pmessage, 2, MPI_INT, sender, 0, comm);
                        messagesExc.fetch_add(1);
                        SentRep[sender] = 1;

                        auto now = chrono::system_clock::now();
                        auto now_c = chrono::system_clock::to_time_t(now);
                        auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;

                        ofstream logfile;
                        logfile.open("output_RC.log", ios::app);
                        logfile << "Process" << rnk + 1 << " reply to Process" << sender+1 << "'s request to enter CS at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << endl; 
                        logfile.close();
                    }
                    else{
                        // cout<<rnk<<"deffered in requesting"<<sender<<endl;
                        RD[sender] = 1;
                    }
                }
            }
                    
            else{
               // send REPLY
                TimeLock.lock();
                timestamp++;
                TimeLock.unlock();

                int Pmessage[2];
                Pmessage[0] = timestamp;
                Pmessage[1] = -2; // -2 indicates a REPLY message
                // cout<<rnk<<"sending reply doesnt want CS to"<<sender<<endl;
                MPI_Send(Pmessage, 2, MPI_INT, sender, 0, comm);
                messagesExc.fetch_add(1);
                SentRep[sender] = 1;

                auto now = chrono::system_clock::now();
                auto now_c = chrono::system_clock::to_time_t(now);
                auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;

                ofstream logfile;
                logfile.open("output_RC.log", ios::app);
                logfile << "Process" << rnk + 1 << " reply to Process" << sender+1 << "'s request to enter CS at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << endl; 
                logfile.close(); 
            }
        }
        
        else if(msgType == -2){
            // REPLY msg
            // cout<<rnk<<"received REPLY from "<<sender<<endl;
            
            SendReq[sender] = 0;
            SentRep[sender] = 0;

            if(std::all_of(SendReq.begin(), SendReq.end(), [](int i){return i==0;})){
                
                inCS.store(true);
            }

        }
        else{
            // Marker msg
            markers++;
            // cout<<rnk<<" Received MARKER from "<<sender<<endl;
            if(markers == n){
                //exit
                // cout<<rnk<<"thread exiting\n";
                return;
            }
        }
    
    }
}

void reqCS(int rnk, int no_CS){

    //update timestamp
    TimeLock.lock();
    timestamp++;
    TimeLock.unlock();

    CSTimestamp = timestamp;

    int Qmessage[2];
    Qmessage[0] = timestamp;
    Qmessage[1] = -1; // -2 indicates a REQUEST message
                
    requesting.store(true);

    if(no_CS == 0){
        
        if(rnk == 0) {
            if(std::all_of(SentRep.begin(), SentRep.end(), [](int i){return i==0;})){
                // cout<<rnk<<"entering CS without any REQ\n";
                inCS.store(true);
            }
            else{
                for(int i = 0; i<n; i++){
                    if(SentRep[i] == 1){
                        MPI_Send(Qmessage, 2, MPI_INT, i, 0, comm);
                        messagesExc.fetch_add(1);
                        SendReq[i] = 1;
                        
                    }
                }
            }
        }
        else{
            for(int i = 0; i<n; i++){
                if(SendReq[i] == 1){
                    MPI_Send(Qmessage, 2, MPI_INT, i, 0, comm);
                    messagesExc.fetch_add(1);
                }
            }
            for(int i = 0; i<n; i++){
                if(SentRep[i] == 1){
                    MPI_Send(Qmessage, 2, MPI_INT, i, 0, comm);
                    messagesExc.fetch_add(1);
                    SendReq[i] = 1;
                }
            }
        }
    }
    else{
        // cout<<rnk<<"Request no. "<<no_CS<<endl;
        if(std::all_of(SentRep.begin(), SentRep.end(), [](int i){return i==0;})) {
            // cout<<rnk<<"entering CS without any REQ\n";
            inCS.store(true);
        }
        else{
            for(int i = 0; i<n; i++){
                if(SentRep[i] == 1){
                    MPI_Send(Qmessage, 2, MPI_INT, i, 0, comm);
                    messagesExc.fetch_add(1);
                    SendReq[i] = 1;
                }
            }
        }

    }

    while(!inCS.load()){
        // wait until others send REPLY messages
    }
    requesting.store(false);
    return;

}

void relCS(int rnk){

    // cout<<rnk<<"exists CS\n";
    
    inCS.store(false);
   
    for(int id = 0; id<RD.size(); id++){
        if(RD[id]==1){
            // send REPLY

            auto now = chrono::system_clock::now();
            auto now_c = chrono::system_clock::to_time_t(now);
            auto now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;

            ofstream logfile;
            logfile.open("output_RC.log", ios::app);
            logfile << "Process" << rnk + 1 << " reply to Process" << id+1 << "'s request to enter CS at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << endl; 
            logfile.close();

            // cout<<rnk<<"sending REPLY after CS to"<<id<<endl;

            TimeLock.lock();
            timestamp++;
            TimeLock.unlock();

            int Pmessage[2];
            Pmessage[0] = timestamp;
            Pmessage[1] = -2; // -2 indicates a REPLY message
           
            MPI_Send(Pmessage, 2, MPI_INT, id, 0, comm);
            messagesExc.fetch_add(1);
            
            RD[id] = 0;
            SentRep[id] = 1;
        }
    }

    return;
}

void working(int rnk){

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
        logfile.open("output_RC.log", ios::app);
        logfile << "Process" << rnk + 1 << " is doing local computation at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << endl; 
        logfile.close();

        this_thread::sleep_for(std::chrono::milliseconds(static_cast<long>(outCSTime)));

        //Requesting the CS
        
        now = chrono::system_clock::now();
        now_c = chrono::system_clock::to_time_t(now);
        now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;

        logfile.open("output_RC.log", ios::app);
        logfile << "Process" << rnk + 1 << " requests to enter CS at " << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << " for the "<< i+1 << "th time" << endl; 
        logfile.close();

        reqCS(rnk, i);
        
        // cout<<rnk<<"enters CS\n";
        //Inside the CS
        now = chrono::system_clock::now();
        now_c = chrono::system_clock::to_time_t(now);
        now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;


        logfile.open("output_RC.log", ios::app);
        logfile << "Process" << rnk + 1 << " enters CS at" << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << endl; 
        logfile.close();

        this_thread::sleep_for(std::chrono::milliseconds(static_cast<long>(inCSTime)));

        //Releasing the CS
        now = chrono::system_clock::now();
        now_c = chrono::system_clock::to_time_t(now);
        now_ms = chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()) % 1000;


        logfile.open("output_RC.log", ios::app);
        logfile << "Process" << rnk + 1 << " leaves CS at" << put_time(localtime(&now_c), "%Y-%m-%d %H:%M:%S") << "." << now_ms.count() << " for the "<< i+1 << "th time" << endl;  
        logfile.close();

        relCS(rnk);
        
    }
    

    for(int id = 0; id < n; id++){

        TimeLock.lock();
        timestamp++;
        TimeLock.unlock();
        int Mmessage[2];
        Mmessage[0] = timestamp;
        Mmessage[1] = -3; // -3 indicates a Marker message
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
    RD.resize(n);
    SendReq.resize(n);
    SentRep.resize(n);

    comm = MPI_COMM_WORLD;

    int provided;
    MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);


    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    for(int i=0; i<rank; i++){
        SendReq[i] = 1;
    }
    
    thread working_thread(working, rank);
    thread receive_thread(receive, rank);

    working_thread.join();
    receive_thread.join();

    ofstream StatsFile;
    StatsFile.open("Statistics_RC.log", ios::app);
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
        StatsFile.open("Statistics_RC.log", ios::app);
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