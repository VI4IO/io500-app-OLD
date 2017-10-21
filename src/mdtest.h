#ifndef _MDTEST_H
#define _MDTEST_H


typedef struct
{
    double entry[10];
} table_t;

table_t * mdtest_run(int argc, char **argv);

#endif
