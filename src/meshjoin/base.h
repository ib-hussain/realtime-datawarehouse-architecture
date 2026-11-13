#ifndef BASE_H
#define BASE_H

#include <iostream> 
#include <sys/types.h>
#include <cstdlib>     // exit
#include <cerrno>       // errno
#include <cstring> 
#include <unistd.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <sstream>
#include <chrono>
#include <stdio.h>
#include <pthread.h>
#include <thread> 
#include <omp.h>
using namespace std;
#include <semaphore.h>
#include <semaphore>

// counting_semaphore<3> parkingSpots(3);// Counting semaphore with 3 available parking spots
// void parking(const string& employee, int carNumber) {
//     cout << employee << " (Car " << carNumber << ") wants to park.\n";
//     parkingSpots.acquire();  // wait if lot is full
//     cout << employee << " (Car " << carNumber << ") has been parked.\n";
//     // Simulate parking time progress
//     for (int percent = 20; percent <= 100; percent += 20) {
//         this_thread::sleep_for(chrono::milliseconds(300));
//         cout << employee << " parking (" << carNumber << "): " << percent << "%\n";
//     }
//     cout << employee << " (Car " << carNumber << ") is leaving the parking.\n";
//     parkingSpots.release();  // free a spot
// }


    // from <pthread.h>
    // pthread_t thread_id1, thread_id2, thread_id3, thread_id4, thread_id5;
    // int status1, status2, status3,status4, status5;

    // if (pthread_create(&thread_id1, NULL, AreaRect, &objRect) != 0) {
    //     fprintf(stderr, "Error creating thread\n");
    //     exit(1);
    // }
    // pthread_join(thread_id1, (void **)&status1);

    // function of <thread> lib
    // this_thread::sleep_for(chrono::milliseconds(300));

#endif