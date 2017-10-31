/*
 * License: MIT license
 */
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>

#include <utilities.h>

#include "io500-functions.h"
#include "io500-utils.h"
#include "io500-options.h"

#define IOR_HARD_OPTIONS "ior -C -Q 1 -g -G 27 -k -e -t 47008 -b 47008"
#define IOR_EASY_OPTIONS "ior -k"
#define MDTEST_EASY_OPTIONS "mdtest -F"
#define MDTEST_HARD_OPTIONS "mdtest -w 3900 -e 3900 -t -F"

static IOR_test_t * io500_run_ior_really(char * args, char * suffix, int testID, io500_options_t * options){
  int argc_count;
  char ** args_array;
  FILE * out;

  if(io500_rank == 0){
    printf("\n[Starting] %s: %s", suffix, CurrentTimeString());
  }

  args_array = io500_str_to_arr_prep_exec(args, & argc_count);
  out = io500_prepare_out(suffix, testID, options);
  IOR_test_t * res = ior_run(argc_count, args_array, MPI_COMM_WORLD, out);
  fclose(out);
  free(args_array);
  return res;
}

IOR_test_t * io500_io_hard_create(io500_options_t * options){
  //generic array holding the arguments to the subtests
  char args[10000];
  int pos;
  pos = sprintf(args, IOR_HARD_OPTIONS" -w");
  pos += sprintf(& args[pos], " -a %s", options->backend_name);
  for(int i=0; i < options->verbosity; i++){
    pos += sprintf(& args[pos], " -v");
  }
  pos += sprintf(& args[pos], " -D %d -O stoneWallingWearOut=1", options->stonewall_timer);
  pos += sprintf(& args[pos], " -s %d", options->iorhard_max_segments);

  io500_replace_str(args); // make sure workdirs with space works
  pos += sprintf(& args[pos], "\n-o\n%s/ior_hard/file", options->workdir);
  pos += sprintf(& args[pos], "\n%s", options->ior_hard_options);

  return io500_run_ior_really(args, "ior_hard_create", 1, options);
}


IOR_test_t * io500_io_hard_read(io500_options_t * options, IOR_test_t * create_read){
  //generic array holding the arguments to the subtests
  char args[10000];
  int pos;
  pos = sprintf(args, IOR_HARD_OPTIONS" -R");
  pos += sprintf(& args[pos], " -a %s", options->backend_name);
  for(int i=0; i < options->verbosity; i++){
    pos += sprintf(& args[pos], " -v");
  }
  pos += sprintf(& args[pos], " -O stoneWallingWearOutIterations=%zu", create_read->results->pairs_accessed);
  if (options->stonewall_timer_reads){
    pos += sprintf(& args[pos], " -D %d -O stoneWallingWearOut=1", options->stonewall_timer);
  }
  pos += sprintf(& args[pos], " -s %d", options->iorhard_max_segments);

  io500_replace_str(args); // make sure workdirs with space works
  pos += sprintf(& args[pos], "\n-o\n%s/ior_hard/file", options->workdir);
  pos += sprintf(& args[pos], "\n%s", options->ior_hard_options);

  return io500_run_ior_really(args, "ior_hard_read", 1, options);
}


IOR_test_t * io500_io_easy_create(io500_options_t * options){
  //generic array holding the arguments to the subtests
  char args[10000];
  int pos;
  pos = sprintf(args, IOR_EASY_OPTIONS" -w");
  pos += sprintf(& args[pos], " -a %s", options->backend_name);
  for(int i=0; i < options->verbosity; i++){
    pos += sprintf(& args[pos], " -v");
  }
  pos += sprintf(& args[pos], " -D %d -O stoneWallingWearOut=1", options->stonewall_timer);

  io500_replace_str(args); // make sure workdirs with space works
  pos += sprintf(& args[pos], "\n-o\n%s/ior_easy/file", options->workdir);
  pos += sprintf(& args[pos], "\n%s", options->ior_easy_options);

  return io500_run_ior_really(args, "ior_easy_create", 1, options);
}

IOR_test_t * io500_io_easy_read(io500_options_t * options, IOR_test_t * create_read){
  //generic array holding the arguments to the subtests
  char args[10000];
  int pos;
  pos = sprintf(args, IOR_EASY_OPTIONS" -r");
  pos += sprintf(& args[pos], " -a %s", options->backend_name);
  for(int i=0; i < options->verbosity; i++){
    pos += sprintf(& args[pos], " -v");
  }
  pos += sprintf(& args[pos], " -O stoneWallingWearOutIterations=%zu", create_read->results->pairs_accessed);
  if (options->stonewall_timer_reads){
    pos += sprintf(& args[pos], " -D %d -O stoneWallingWearOut=1", options->stonewall_timer);
  }

  io500_replace_str(args); // make sure workdirs with space works
  pos += sprintf(& args[pos], "\n-o\n%s/ior_easy/file", options->workdir);
  pos += sprintf(& args[pos], "\n%s", options->ior_easy_options);

  return io500_run_ior_really(args, "ior_easy_read", 1, options);
}

static mdtest_results_t * io500_run_mdtest_really(char * args, char * suffix, int testID, io500_options_t * options){
  int argc_count;
  char ** args_array;
  mdtest_results_t * table;
  FILE * out;

  if(io500_rank == 0){
    printf("\n[Starting] %s: %s", suffix, CurrentTimeString());
  }

  args_array = io500_str_to_arr_prep_exec(args, & argc_count);
  out = io500_prepare_out(suffix, testID, options);
  table = mdtest_run(argc_count, args_array, MPI_COMM_WORLD, out);
  fclose(out);
  free(args_array);
  return table;
}

mdtest_results_t * io500_run_mdtest_easy(char mode, int maxfiles, int use_stonewall, const char * extra, char * suffix, int testID, io500_options_t * options){
  char args[10000];
  memset(args, 0, 10000);
  if(maxfiles == 0){
    io500_error("Error, mdtest does not support 0 files.");
  }

  int pos;
  pos = sprintf(args, MDTEST_EASY_OPTIONS" -%c", mode);
  pos += sprintf(& args[pos], " -a %s", options->backend_name);
  pos += sprintf(& args[pos], " -n %d", maxfiles);
  if(use_stonewall){
    pos += sprintf(& args[pos], " -W %d", options->stonewall_timer);
  }
  for(int i=0; i < options->verbosity; i++){
    pos += sprintf(& args[pos], " -v");
  }
  pos += sprintf(& args[pos], "%s", extra);
  io500_replace_str(args);
  pos += sprintf(& args[pos], "\n-d\n%s/mdtest_easy", options->workdir);
  pos += sprintf(& args[pos], "\n%s", options->mdtest_easy_options);

  return io500_run_mdtest_really(args, suffix, testID, options);
}

mdtest_results_t * io500_md_easy_create(io500_options_t * options){
  mdtest_results_t * res = io500_run_mdtest_easy('C', options->mdeasy_max_files, 1, "", "mdtest_easy_create", 1, options);
  if(res->items == 0){
    io500_error("Stonewalling returned 0 created files, that is wrong.");
  }
  return res;
}

mdtest_results_t * io500_md_easy_read(io500_options_t * options, mdtest_results_t * create_read){
  return io500_run_mdtest_easy('E', create_read->stonewall_last_item[MDTEST_FILE_CREATE_NUM], options->stonewall_timer_reads, "", "mdtest_easy_read", 1, options);
}

mdtest_results_t * io500_md_easy_stat(io500_options_t * options, mdtest_results_t * create_read){
  return io500_run_mdtest_easy('T', create_read->stonewall_last_item[MDTEST_FILE_CREATE_NUM], options->stonewall_timer_reads, "", "mdtest_easy_stat", 1, options);
}


mdtest_results_t * io500_md_easy_delete(io500_options_t * options, mdtest_results_t * create_read){
  return io500_run_mdtest_easy('r', create_read->stonewall_last_item[MDTEST_FILE_CREATE_NUM], options->stonewall_timer_delete, "", "mdtest_easy_delete", 1, options);
}


mdtest_results_t * io500_run_mdtest_hard(char mode, int maxfiles, int use_stonewall, const char * extra,  char * suffix, int testID, io500_options_t * options){
  char args[10000];
  int pos;
  pos = sprintf(args, MDTEST_HARD_OPTIONS" -%c", mode);
  pos += sprintf(& args[pos], " -a %s", options->backend_name);
  pos += sprintf(& args[pos], " -n %d", maxfiles);
  if(use_stonewall){
    pos += sprintf(& args[pos], " -W %d", options->stonewall_timer);
  }
  for(int i=0; i < options->verbosity; i++){
    pos += sprintf(& args[pos], " -v");
  }
  pos += sprintf(& args[pos], "%s", extra);
  io500_replace_str(args);
  pos += sprintf(& args[pos], "\n-d\n%s/mdtest_hard", options->workdir);

  return io500_run_mdtest_really(args, suffix, testID, options);
}

mdtest_results_t * io500_md_hard_create(io500_options_t * options){
  mdtest_results_t * res = io500_run_mdtest_hard('C', options->mdhard_max_files, 1, "", "mdtest_hard_create", 1, options);
  if(res->items == 0){
    io500_error("Stonewalling returned 0 created files, that is wrong.");
  }
  return res;
}

mdtest_results_t * io500_md_hard_read(io500_options_t * options, mdtest_results_t * create_read){
  return io500_run_mdtest_hard('E', create_read->stonewall_last_item[MDTEST_FILE_CREATE_NUM], options->stonewall_timer_reads, "","mdtest_hard_read", 1, options);
}

mdtest_results_t * io500_md_hard_stat(io500_options_t * options, mdtest_results_t * create_read){
  return io500_run_mdtest_hard('T', create_read->stonewall_last_item[MDTEST_FILE_CREATE_NUM], options->stonewall_timer_reads, "","mdtest_hard_stat", 1, options);
}

mdtest_results_t * io500_md_hard_delete(io500_options_t * options, mdtest_results_t * create_read){
  return io500_run_mdtest_hard('r', create_read->stonewall_last_item[MDTEST_FILE_CREATE_NUM], options->stonewall_timer_delete, "", "mdtest_hard_delete", 1, options);
}

void io500_touch(char * const filename){
  if(io500_rank != 0){
    return;
  }
  int fd = open(filename, O_CREAT | O_WRONLY, S_IWUSR | S_IRUSR);
  if(fd < 0){
    printf("%s ", strerror(errno));
    io500_error("Could not write file, verify permissions");
  }
  close(fd);
}

void io500_cleanup(io500_options_t* options){
  if(io500_rank == 0){
    printf("\nCleaning files from working directory: %s", CurrentTimeString());
  }
  io500_parallel_find_or_delete(stdout, options->workdir, NULL, 1, 0);
  if(io500_rank == 0){
    printf("Done: %s", CurrentTimeString());
    printf("  If you want to use the same directory with different numbers of ranks,\n"
           "  remove the directory structure with rm -r, too\n");
  }
}

void io500_recursively_create(const char * dir, int touch){
  char tmp[10000]; // based on https://stackoverflow.com/questions/2336242/recursive-mkdir-system-call-on-unix
  char *p = NULL;
  size_t len;
  snprintf(tmp, sizeof(tmp),"%s",dir);
  len = strlen(tmp);
  if(tmp[len - 1] == '/'){
    tmp[len - 1] = 0;
  }
  for(p = tmp + 1; *p; p++){
    if(*p == '/') {
      *p = 0;
      io500_recursively_create(tmp, 0);
      *p = '/';
    }
  }
  mkdir(tmp, S_IRWXU);

  if(touch){
    char tmp2[10000];
    snprintf(tmp2, sizeof(tmp2), "%s/%s", dir, "IO500-testfile");
    io500_touch(tmp2);
  }
}

int io500_contains_workdir_tag(io500_options_t * options){
    char fname[4096];
    sprintf(fname, "%s/IO500-testfile", options->workdir);
    int fd = open(fname, O_RDONLY);
    int ret = (fd != -1);
    close(fd);
    return ret;
}

void io500_create_workdir(io500_options_t * options){
  // todo, ensure that the working directory contains no legacy stuff
  char dir[10000];

  sprintf(dir, "%s/", options->results_dir);
  io500_recursively_create(dir, 0);

  sprintf(dir, "%s/ior_hard/", options->workdir);
  io500_recursively_create(dir, 1);
  sprintf(dir, "%s/ior_easy/", options->workdir);
  io500_recursively_create(dir, 1);
  sprintf(dir, "%s/mdtest_easy/", options->workdir);
  io500_recursively_create(dir, 1);
  sprintf(dir, "%s/mdtest_hard/", options->workdir);
  io500_recursively_create(dir, 1);

  sprintf(dir, "%s/IO500-testfile", options->workdir);
  io500_touch(dir);
}

void io500_print_bw(const char * prefix, int id, IOR_test_t * stat, int read){
  double timer = read ? stat->results->readTime[0] : stat->results->writeTime[0];
  double gibsize = stat->results->aggFileSizeFromXfer[0] / 1024.0 / 1024.0 / 1024.0;
  printf("[Result] IOR %s bw: %.3f GiB/s time: %.1fs size: %.1f GiB",
  prefix, gibsize / timer, timer, gibsize );
  if(stat->results->stonewall_min_data_accessed != 0){
    printf(" (perf at stonewall min: %.3f GiB/s avg: %.3f GiB/s)",
      stat->results->stonewall_min_data_accessed / stat->results->stonewall_time / 1024.0 / 1024.0 / 1024.0,
      stat->results->stonewall_avg_data_accessed / stat->results->stonewall_time / 1024.0 / 1024.0 / 1024.0);
  }
  printf("\n");
}

void io500_print_md(const char * prefix, int id, mdtest_test_num_t pos, mdtest_results_t * stat){
  double val = stat->rate[pos] / 1000;
  double tim = stat->time[pos];
  //for(int i=0; i < 10; i++){
  //  if(stat->entry[i] != 0){
  //    printf("%d %f\n", i, stat->entry[i]);
  //  }
  //}
  printf("[Result] mdtest %s rate: %.3f kiops time: %.1fs", prefix, val, tim);
  if(stat->stonewall_item_sum[pos] != 0){
    printf(" (perf at stonewall min: %.1f kiops avg: %.1f kiops)", stat->stonewall_item_min[pos] / 1000.0 / stat->stonewall_time[pos],
    stat->stonewall_item_sum[pos] / 1000.0 / stat->stonewall_time[pos]);
  }
  printf("\n");
}

void io500_print_find(io500_find_results_t * find){
    printf("[Result] find rate: %.3f kiops time: %.1fs err: %ld found: %ld (scanned %ld files)\n",  find->rate / 1000, find->runtime, find->errors, find->found_files, find->total_files);
}

void io500_print_startup(int argc, char ** argv){
  int size;
  MPI_Comm_size(MPI_COMM_WORLD, & size);
  int nodes = CountTasksPerNode(size, MPI_COMM_WORLD);

  char * procName = NULL;
  if(size < 1000){
    procName = (char *) malloc(MPI_MAX_PROCESSOR_NAME * size);
    int res;
    MPI_Get_processor_name(procName, & res);

    if(io500_rank != 0){
      MPI_Gather(procName, MPI_MAX_PROCESSOR_NAME, MPI_CHAR, NULL, 0, MPI_CHAR, 0, MPI_COMM_WORLD);
      free(procName);
      return;
    }
  }else if (io500_rank != 0){
    return;
  }
  printf("IO500 starting: %s\n", CurrentTimeString());
  printf("Arguments: %s", argv[0]);
  for(int i=1 ; i < argc; i++){
    printf(" \"%s\"", argv[i]);
  }
  printf("\n");
  printf("Runtime:\n");
  printf("NODES=%d\n", nodes);
  printf("NPROC=%d\n", size);

  if(size < 1000){
    MPI_Gather(procName, MPI_MAX_PROCESSOR_NAME, MPI_CHAR, procName, MPI_MAX_PROCESSOR_NAME, MPI_CHAR, 0, MPI_COMM_WORLD);
    printf("RANK_MAP=");

    char * curP = procName;
    printf("%d:%s", 0, curP);
    for(int i=1; i < size; i++){
      printf(",%d:%s", i, curP);
      curP += strlen(curP) + 1;
    }
    printf("\n");
    free(procName);
  }
}
