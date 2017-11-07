#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

#include <ior.h>
#include <utilities.h>

#include "io500-utils.h"

int io500_rank;

void io500_replace_str(char * str){
  for( ; *str != 0 ; str++ ){
    if(*str == ' '){
      *str = '\n';
    }
  }
}

char ** io500_str_to_arr_prep_exec(char * str, int * out_count){
  // str is separated on "\n"
  int cnt = 1;
  for(int i=0; str[i] != 0; i++){
    if(str[i] == '\n'){
      cnt++;
    }
  }
  char ** out_arr = malloc(sizeof(void*) * cnt);
  *out_count = cnt;

  int pos = 0;
  out_arr[pos] = & str[0];
  if(io500_rank == 0)
    printf("[Exec]:");
  for(int i=0; str[i] != 0; i++){
    if(str[i] == '\n'){
      pos++;
      out_arr[pos] = & str[i+1];
      str[i] = 0;
      if(io500_rank == 0)
        printf(" \"%s\"", out_arr[pos - 1]);
    }
  }
  if(io500_rank == 0)
    printf(" \"%s\"\n", out_arr[pos]);
  return out_arr;
}

void io500_error(char * const str){
  if(io500_rank == 0){
    printf("\nIO500 critical error: %s\nError timestamp: %s\n", str, CurrentTimeString());
  }
  MPI_Abort(MPI_COMM_WORLD, 1);
}


FILE * io500_prepare_out(char * suffix, int testID, io500_options_t * options){
  if(io500_rank == 0 || options->log_all_procs){
    char out[10000];
    if(options->log_all_procs){
      sprintf(out, "%s/%s-%d-%d.log", options->results_dir, suffix, io500_rank, testID);
    }else{
      sprintf(out, "%s/%s-%d.log", options->results_dir, suffix, testID);
    }
    if(testID == 0){
      sprintf(out, "%s/%s", options->results_dir, suffix);
    }
    fprintf(options->output, "[Output] %s\n", out);
    fflush(options->output);

    // open an output file
    FILE * ret = fopen(out, "w");
    if (ret == NULL){
      io500_error("Could not open output file, aborting!");
    }
    return ret;
  }else{
    // messages from other processes are usually critical or verbose, let them through...
    FILE * null = fopen("/dev/null", "w");
    return null;
  }
}
