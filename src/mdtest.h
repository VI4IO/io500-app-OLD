#ifndef _MDTEST_H
#define _MDTEST_H


typedef struct
{
    double entry[10];
    uint64_t items;
} table_t;

table_t * mdtest_run(int argc, char **argv, MPI_Comm world_com, FILE * out_logfile);

#endif
