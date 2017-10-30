#ifndef _MDTEST_H
#define _MDTEST_H

typedef enum {
  MDTEST_DIR_CREATE_NUM = 0,
  MDTEST_DIR_STAT_NUM = 1,
  MDTEST_DIR_READ_NUM = 1,
  MDTEST_DIR_REMOVE_NUM = 3,
  MDTEST_FILE_CREATE_NUM = 4,
  MDTEST_FILE_STAT_NUM = 5,
  MDTEST_FILE_READ_NUM = 6,
  MDTEST_FILE_REMOVE_NUM = 7,
  MDTEST_TREE_CREATE_NUM = 8,
  MDTEST_TREE_REMOVE_NUM = 9,
  MDTEST_LAST_NUM
} mdtest_test_num_t;

typedef struct
{
    double rate[MDTEST_LAST_NUM];
    double time[MDTEST_LAST_NUM];
    uint64_t items[MDTEST_LAST_NUM];
    uint64_t stonewall_last_item[MDTEST_LAST_NUM];
} mdtest_results_t;

mdtest_results_t * mdtest_run(int argc, char **argv, MPI_Comm world_com, FILE * out_logfile);

#endif
