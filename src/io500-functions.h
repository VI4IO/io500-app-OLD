#ifndef _IO500_FUNCTIONS_H
#define _IO500_FUNCTIONS_H

#include <ior.h>
#include <mdtest.h>

#include "io500-types.h"

void io500_print_startup(int argc, char ** argv);


IOR_test_t * io500_io_hard_create(io500_options_t * options);
IOR_test_t * io500_io_hard_read(io500_options_t * options, IOR_test_t * create_read);
IOR_test_t * io500_io_easy_create(io500_options_t * options);
IOR_test_t * io500_io_easy_read(io500_options_t * options, IOR_test_t * create_read);

mdtest_results_t * io500_run_mdtest_easy(char mode, int maxfiles, int use_stonewall, const char * extra, char * suffix, int testID, io500_options_t * options);

mdtest_results_t * io500_md_easy_create(io500_options_t * options);
mdtest_results_t * io500_md_easy_read(io500_options_t * options, mdtest_results_t * create_read);
mdtest_results_t * io500_md_easy_stat(io500_options_t * options, mdtest_results_t * create_read);
mdtest_results_t * io500_md_easy_delete(io500_options_t * options, mdtest_results_t * create_read);

mdtest_results_t * io500_run_mdtest_hard(char mode, int maxfiles, int use_stonewall, const char * extra,  char * suffix, int testID, io500_options_t * options);
mdtest_results_t * io500_md_hard_create(io500_options_t * options);
mdtest_results_t * io500_md_hard_read(io500_options_t * options, mdtest_results_t * create_read);
mdtest_results_t * io500_md_hard_stat(io500_options_t * options, mdtest_results_t * create_read);
mdtest_results_t * io500_md_hard_delete(io500_options_t * options, mdtest_results_t * create_read);

io500_find_results_t * io500_find(FILE * out, io500_options_t * opt);



// IO functions TODO convert to aiori
void io500_touch(char * const filename);
void io500_create_workdir(io500_options_t * options);
int  io500_contains_workdir_tag(io500_options_t * options);
void io500_recursively_create(const char * dir, int touch);

io500_find_results_t * io500_parallel_find_or_delete(FILE * out, char * workdir, char * const filename_pattern, int delete, int stonewall_timer_s);
///////////////////////////////////

void io500_cleanup(io500_options_t* options);

void io500_print_find(io500_find_results_t * find);
void io500_print_bw(const char * prefix, int id, IOR_test_t * stat, int read);
void io500_print_md(const char * prefix, int id, mdtest_test_num_t pos, mdtest_results_t * stat);


#endif
