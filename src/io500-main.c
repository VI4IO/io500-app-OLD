#include <utilities.h>

#include "io500-options.h"
#include "io500-utils.h"
#include "io500-functions.h"

#include "io500-types.h"


int main(int argc, char ** argv){
  // output help with --help to enable running without mpiexec
  for(int i=0; i < argc; i++){
    if (strcmp(argv[i], "--help") == 0){
      argv[i][0] = 0;
      io500_rank = 0;
      io500_parse_args(argc, argv, 1);
      exit(0);
    }
  }

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, & io500_rank);

  io500_options_t * options = io500_parse_args(argc, argv, 0);

  FILE * out = stdout;
  if(io500_rank == 0 && options->write_output_to_log){
    out = io500_prepare_out("IO500-summary.txt", 0, options);
    options->output = out;
  }

  io500_print_startup(argc, argv, options);

  if(options->only_cleanup){
    // make sure there exists the file IO500_TIMESTAMP
    if(! io500_contains_workdir_tag(options)){
      io500_error("I will not delete the directory in parallel as the file"
        "IO500-testfile does not exist,\n"
        "Maybe it's the wrong directory! If you are sure create the file");
    }

    io500_cleanup(options);
    MPI_Finalize();
    exit(0);
  }

  if(io500_contains_workdir_tag(options)){
      if(io500_rank == 0){
        fprintf(options->output, "Error, the working directory contains IO500-testfile already, so I will clean that directory for you before I start!");
      }
      io500_cleanup(options);
  }

  if(io500_rank == 0){
    io500_create_workdir(options);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  IOR_test_t * io_easy_create = io500_io_easy_create(options);
  if(io500_rank == 0) io500_print_bw(out, "ior_easy_write", 1, io_easy_create, 0);

  mdtest_results_t *    md_easy_create = io500_md_easy_create(options);
  if(io500_rank == 0) io500_print_md(out, "mdtest_easy_create", 1, MDTEST_FILE_CREATE_NUM, md_easy_create);

  {
    char fname[4096];
    sprintf(fname, "%s/IO500_TIMESTAMP", options->workdir);
    io500_touch(fname);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  IOR_test_t * io_hard_create = io500_io_hard_create(options);
  if(io500_rank == 0) io500_print_bw(out, "ior_hard_write", 3, io_hard_create, 0);

  mdtest_results_t *    md_hard_create = io500_md_hard_create(options);
  if(io500_rank == 0) io500_print_md(out, "mdtest_hard_create", 5, MDTEST_FILE_CREATE_NUM, md_hard_create);

  // mdreal...
  if(io500_rank == 0){
    fprintf(out, "\n[Starting] find: %s", CurrentTimeString());
    fflush(out);
  }
  FILE * fout = io500_prepare_out("find", 1, options);
  io500_find_results_t* find = io500_find(options);
  fclose(fout);
  if(io500_rank == 0) io500_print_find(out, find);

  IOR_test_t * io_easy_read = io500_io_easy_read(options, io_easy_create);
  if(io500_rank == 0) io500_print_bw(out, "ior_easy_read", 2, io_easy_read, 1);

  //mdtest_results_t *    md_easy_read = io500_md_easy_read(options, md_easy_create);
  mdtest_results_t *    md_hard_stat = io500_md_hard_stat(options, md_hard_create);
  if(io500_rank == 0) io500_print_md(out, "mdtest_hard_stat",   7, MDTEST_FILE_STAT_NUM, md_hard_stat);

  IOR_test_t * io_hard_read = io500_io_hard_read(options, io_hard_create);
  mdtest_results_t *    md_hard_read = io500_md_hard_read(options, md_hard_create);
  if(io500_rank == 0) io500_print_md(out, "mdtest_hard_read",   6, MDTEST_FILE_READ_NUM, md_hard_read);

  mdtest_results_t *    md_easy_stat = io500_md_easy_stat(options, md_easy_create);
  if(io500_rank == 0) io500_print_md(out, "mdtest_easy_stat",   3, MDTEST_FILE_STAT_NUM, md_easy_stat);

  mdtest_results_t *    md_hard_delete = io500_md_hard_delete(options, md_hard_create);
  if(io500_rank == 0) io500_print_md(out, "mdtest_hard_delete", 8, MDTEST_FILE_REMOVE_NUM, md_hard_delete);

  mdtest_results_t *    md_easy_delete = io500_md_easy_delete(options, md_easy_create);
  if(io500_rank == 0) io500_print_md(out, "mdtest_easy_delete", 4, MDTEST_FILE_REMOVE_NUM, md_easy_delete);

  if(io500_rank == 0){
    fprintf(out, "\nIO500 complete: %s\n", CurrentTimeString());

    fprintf(out, "\n");
    fprintf(out, "=== IO-500 submission ===\n");

    io500_print_bw(out, "ior_easy_write", 1, io_easy_create, 0);
    io500_print_bw(out, "ior_easy_read", 2, io_easy_read, 1);
    io500_print_bw(out, "ior_hard_write", 3, io_hard_create, 0);
    io500_print_bw(out, "ior_hard_read", 4, io_hard_read, 1);

    io500_print_md(out, "mdtest_easy_create", 1, MDTEST_FILE_CREATE_NUM, md_easy_create);
    //io500_print_md(out, "mdtest_easy_read",   2, MDTEST_FILE_READ_NUM, md_easy_read);
    io500_print_md(out, "mdtest_easy_stat",   3, MDTEST_FILE_STAT_NUM, md_easy_stat);
    io500_print_md(out, "mdtest_easy_delete", 4, MDTEST_FILE_REMOVE_NUM, md_easy_delete);

    io500_print_md(out, "mdtest_hard_create", 5, MDTEST_FILE_CREATE_NUM, md_hard_create);
    io500_print_md(out, "mdtest_hard_read",   6, MDTEST_FILE_READ_NUM, md_hard_read);
    io500_print_md(out, "mdtest_hard_stat",   7, MDTEST_FILE_STAT_NUM, md_hard_stat);
    io500_print_md(out, "mdtest_hard_delete", 8, MDTEST_FILE_REMOVE_NUM, md_hard_delete);

    io500_print_find(out, find);
  }
  if(! options->stonewall_timer_delete){
    io500_cleanup(options);
  }
  MPI_Finalize();
  if(io500_rank == 0){
    fclose(out);
  }
  return 0;
}
