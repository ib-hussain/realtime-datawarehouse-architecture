#ifndef DISK_BUFFER_H
#define DISK_BUFFER_H

#include "base.h"
// for this file i was thinking how about we actually load make this a program and use it and upload it to ram 
// and we can control max ram allocation and we can compile and use this using cpython in python files??
// but unfortunately we need cross platform compatibility with this move which is hard because i had plans for 
// linux but windows threading is different and that can be handled by pyhton and also c but i wouldnt want to do that in C


    // pthread_t thread_id1, thread_id2, thread_id3, thread_id4, thread_id5;
    // int status1, status2, status3,status4, status5;

    // if (pthread_create(&thread_id1, NULL, AreaRect, &objRect) != 0) {
    //     fprintf(stderr, "Error creating thread\n");
    //     exit(1);
    // }
    // pthread_join(thread_id1, (void **)&status1);

#endif