#include <stdio.h>

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
  MPI_Barrier(MPI_COMM_WORLD);

  if(io500_rank == 0){
    fprintf(out, "\n[Starting] find: %s", CurrentTimeString());
    fflush(out);
  }
  io500_find_results_t* find = io500_find(options);
  if(io500_rank == 0) io500_print_find(out, find);

  io500_print_find(out, find);
  MPI_Finalize();
  if(io500_rank == 0){
    fclose(out);
  }
  return 0;
}
