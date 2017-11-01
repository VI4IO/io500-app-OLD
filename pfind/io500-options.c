#include <stdio.h>
#include <getopt.h> // TODO replace with getoptList
#include <string.h>
#include <stdlib.h>
#include <mpi.h>

#include "io500-options.h"
#include "io500-utils.h"

static void io500_print_help(io500_options_t * res){
  printf("pfind \nSynopsis:\n"
      "\t-w <DIR>: The working directory, that must contain the file IO500_TIMESTAMP = \"%s\"\n"
      "\t-s <seconds>: Stonewall timer for find = %d\n"
      "\t-v: increase the verbosity, use multiple times to increase level = %d\n"
      "Useful utility flags\n"
      "\t-h: prints the help\n"
      "\t--help: prints the help without initializing MPI\n",
      res->workdir,
      res->stonewall_timer,
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
