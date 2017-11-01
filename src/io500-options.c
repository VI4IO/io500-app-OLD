#include <stdio.h>
#include <getopt.h> // TODO replace with getoptList
#include <string.h>
#include <stdlib.h>
#include <mpi.h>

#include "io500-options.h"
#include "io500-utils.h"

static void io500_print_help(io500_options_t * res){
  printf("IO500 benchmark\nSynopsis:\n"
      "\t-w <DIR>: The working directory for the benchmarks = \"%s\"\n"
      "\t-r <DIR>: The result directory for the individual results = \"%s\"\n"
      "Optional flags\n"
      "\t-a <API>: API for I/O [POSIX|MPIIO|HDF5|HDFS|S3|S3_EMC|NCMPI] = %s\n"
      "\t-s <seconds>: Stonewall timer for all create/write phases = %d\n"
      "\t-S: Activate stonewall timer for all read phases (default off)\n\t\tuse -S -S to also activate stonewall timer for delete and skip final cleanup phase (useful when formating the partition)\n"
      "\t-e <IOR easy options>: additional acceptable IOR easy option = \"%s\"\n"
      "\t-E <IOR hard options>: additional acceptable IOR hard option = \"%s\"\n"
      "\t-I <N>: Max segments for ior_hard = %d\n"
      "\t-f <N>: Max number of files for mdtest_easy = %d\n"
      "\t-F <N>: Max number of files for mdtest_hard = %d\n"
      "\t-v: increase the verbosity, use multiple times to increase level = %d\n"
      "Useful utility flags\n"
      "\t-C: only parallel delete of files in the working directory, use to cleanup leftovers from aborted runs\n"
      "\t-l: Log all processes into individual result files, otherwise only rank 0 logs its output\n"
      "\t-L: Log output of main program to results-directory/IO500-summary.txt\n"
      "\t-h: prints the help\n"
      "\t--help: prints the help without initializing MPI\n",
      res->workdir,
      res->results_dir,
      res->backend_name,
      res->stonewall_timer,
      res->ior_easy_options,
      res->ior_hard_options,
      res->iorhard_max_segments,
      res->mdeasy_max_files,
      res->mdhard_max_files,
      res->verbosity
    );
}

io500_options_t * io500_parse_args(int argc, char ** argv, int force_print_help){
  io500_options_t * res = malloc(sizeof(io500_options_t));
  memset(res, 0, sizeof(io500_options_t));
  int print_help = force_print_help;

  res->backend_name = "POSIX";
  res->workdir = "./io500-run/";
  res->results_dir = "./io500-results/";
  res->verbosity = 0;

  res->ior_easy_options = strdup("-F -t 1m -b 1t");
  res->ior_hard_options = strdup("");
  res->mdtest_easy_options = strdup("-u -L");
  res->mdeasy_max_files = 100000000;
  res->mdhard_max_files = 100000000;
  res->stonewall_timer = 300;
  res->iorhard_max_segments = 100000000;
  res->output = stdout;

  int c;
  while (1) {
    c = getopt(argc, argv, "a:e:E:hvw:f:F:s:SI:ClLr:");
    if (c == -1) {
        break;
    }

    switch (c) {
    case 'a':
        res->backend_name = strdup(optarg); break;
    case 'C':
        res->only_cleanup = 1; break;
    case 'e':
        res->ior_easy_options = strdup(optarg); break;
    case 'E':
        res->ior_hard_options = strdup(optarg); break;
    case 'f':
      res->mdeasy_max_files = atol(optarg); break;
    case 'F':
      res->mdhard_max_files = atol(optarg); break;
    case 'h':
      print_help = 1; break;
    case 'I':
      res->iorhard_max_segments = atol(optarg); break;
    case 'l':
      res->log_all_procs = 1; break;
    case 'L':
      res->write_output_to_log = 1; break;
    case 'm':
        res->mdtest_easy_options = strdup(optarg); break;
    case 'r':
        res->results_dir = strdup(optarg); break;
    case 's':
      res->stonewall_timer = atol(optarg);
      break;
    case 'S':
      if(res->stonewall_timer_reads){
        res->stonewall_timer_delete = 1;
      }
      res->stonewall_timer_reads = 1; break;
    case 'v':
      res->verbosity++; break;
    case 'w':
      res->workdir = strdup(optarg); break;
    }
  }
  if(print_help){
    io500_print_help(res);
    int init;
    MPI_Initialized( & init);
    if(init){
      MPI_Finalize();
    }
    exit(0);
  }
  io500_replace_str(res->ior_easy_options);
  io500_replace_str(res->ior_hard_options);
  io500_replace_str(res->mdtest_easy_options);
  return res;
}
