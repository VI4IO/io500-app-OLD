#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>
#include <dirent.h>
#include <limits.h>
#include <sys/stat.h>

#include <aiori.h>
#include <libcircle.h>

// this version is based on mpistat
#include <utilities.h>

#include "io500-types.h"
#include "io500-functions.h"
#include "io500-utils.h"

// parallel recursive find

static struct stat compare_time_newer;
static off_t glob_expected_size;
static char * glob_compare_str;

static int glob_verbosity = 0;
static int glob_delete;
static int glob_stonewall_timer;
static double glob_endtime;

static io500_find_results_t * res = NULL;

io500_find_results_t* io500_find(io500_options_t * opt){
  if(rank == 0){
    fprintf(opt->output, "[Running] find: %s\n", CurrentTimeString());
  }

  glob_expected_size = 3900; // TODO make that adjustable
  glob_verbosity = opt->verbosity;
  char fname[4096];
  {
    sprintf(fname, "%s/IO500_TIMESTAMP", opt->workdir);
    if(lstat(fname, & compare_time_newer) != 0) {
      printf("Timestamp file: %s\n", fname);
      io500_error("Could not read timestamp file!");
    }
  }

  sprintf(fname, "%s/mdtest_easy", opt->workdir);

  //ior_aiori_t * backend = aiori_select(opt->backend_name);
  double start = GetTimeStamp();
  io500_find_results_t * res = io500_parallel_find_or_delete(out_logfile, fname, "01", 0, opt->stonewall_timer_reads ? opt->stonewall_timer : 0 );
  double end = GetTimeStamp();
  res->runtime = end - start;

  double runtime = res->runtime;
  long long found = res->found_files;
  long long total_files = res->total_files;
  MPI_Reduce(& runtime, & res->runtime, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(& found, & res->found_files, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(& total_files, & res->total_files, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  res->rate = res->total_files / res->runtime;

  return res;
}


io500_find_results_t* io500_find_hard(io500_options_t * opt){
  if(rank == 0){
    fprintf(opt->output, "[Running] find: %s\n", CurrentTimeString());
  }

  glob_expected_size = 3900; // TODO make that adjustable
  glob_verbosity = opt->verbosity;

  char fname[4096];
  {
    sprintf(fname, "%s/IO500_TIMESTAMP", opt->workdir);
    if(lstat(fname, & compare_time_newer) != 0) {
      printf("Timestamp file: %s\n", fname);
      io500_error("Could not read timestamp file!");
    }
  }
  sprintf(fname, "%s/mdtest_hard", opt->workdir);

  //ior_aiori_t * backend = aiori_select(opt->backend_name);
  double start = GetTimeStamp();
  io500_find_results_t * res = io500_parallel_find_or_delete(out_logfile, fname, "01", 0, opt->stonewall_timer_reads ? opt->stonewall_timer : 0 );
  double end = GetTimeStamp();
  res->runtime = end - start;

  double runtime = res->runtime;
  long long found = res->found_files;
  long long total_files = res->total_files;
  MPI_Reduce(& runtime, & res->runtime, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(& found, & res->found_files, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(& total_files, & res->total_files, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  res->rate = res->total_files / res->runtime;

  return res;
}

// globals
static char start_dir[8192]; // absolute path of start directory
static char item_buf[8192]; // buffer to construct type / path combos for queue items

static char  find_file_type(unsigned char c) {
    switch (c) {
        case DT_BLK :
            return 'b';
        case DT_CHR :
            return 'c';
        case DT_DIR :
            return 'd';
        case DT_FIFO :
            return 'F';
        case DT_LNK :
            return 'l';
        case DT_REG :
            return 'f';
        case DT_SOCK :
            return 's';
        default :
            return 'u';
    }
}

static void find_do_lstat(char *path) {
  static struct stat buf;
  // filename comparison has been done already
  if(glob_verbosity >= 2){
    fprintf(out_logfile, "STAT: %s\n", path);
  }

  if (lstat(path+1, & buf) == 0) {
    // compare values
    if(buf.st_size != glob_expected_size){
      if(glob_verbosity >= 2){
        fprintf(out_logfile, "Size does not match: %s has %zu bytes\n", path, (size_t) buf.st_size);
      }
      return;
    }
    if( buf.st_ctime < compare_time_newer.st_ctime ){
      if(glob_verbosity >= 2){
        fprintf(out_logfile, "Timestamp too small: %s\n", path);
      }
      return;
    }

    if(glob_verbosity >= 2){
      fprintf(out_logfile, "Found acceptable file: %s\n", path);
    }
    res->found_files++;
  } else {
    res->errors++;
    if(glob_verbosity >= 1){
      fprintf(out_logfile, "Error stating file: %s\n", path);
    }
  }
}

static void find_do_readdir(char *path, CIRCLE_handle *handle) {
    int path_len = strlen(path+1);
    DIR *d = opendir(path+1);
    if (!d) {
        fprintf (stderr, "Cannot open '%s': %s\n", path+1, strerror (errno));
        return;
    }
    int fd = dirfd(d);
    while (1) {
        struct dirent *entry;
        entry = readdir(d);
        if (glob_stonewall_timer && GetTimeStamp() >= glob_endtime ){
          break;
        }
        if (entry==0) {
            break;
        }
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        char typ = find_file_type(entry->d_type);
        if (typ == 'u'){
          // sometimes the filetype is not provided by readdir.

          static struct stat buf;
          if (fstatat(fd, entry->d_name, & buf, 0 )) {
            res->errors++;
            if(glob_verbosity >= 1){
              fprintf(out_logfile, "Error stating file: %s\n", path);
            }
            continue;
          }
          typ = S_ISDIR(buf.st_mode) ? 'd' : 'f';

          if(! glob_delete && typ == 'f'){ // since we have done the stat already, it would be a waste to do it again
            res->total_files++;
            // compare values
            if(glob_compare_str != NULL && strstr(entry->d_name, glob_compare_str) == NULL){
              continue;
            }else if(buf.st_size != glob_expected_size){
              if(glob_verbosity >= 2){
                fprintf(out_logfile, "Size does not match: %s/%s has %zu bytes\n", path + 1, entry->d_name, (size_t) buf.st_size);
              }
              continue;
            }else if( buf.st_ctime < compare_time_newer.st_ctime ){
              if(glob_verbosity >= 2){
                fprintf(out_logfile, "Timestamp too small: %s/%s\n", path + 1, entry->d_name);
              }
              continue;
            }else{
              if(glob_verbosity >= 2){
                fprintf(out_logfile, "Found acceptable file: %s/%s\n", path + 1, entry->d_name);
              }
              res->found_files++;
              continue;
            }
          }
        }

        if (typ == 'f'){
          res->total_files++;
          // compare file name
          if( glob_compare_str != NULL && strstr(entry->d_name, glob_compare_str) == NULL){
            continue;
          }
        }
        char *tmp=(char*) malloc(path_len+strlen(entry->d_name)+3);
        *tmp = typ;
        strcpy(tmp+1, path+1);
        *(tmp+path_len+1)='/';
        strcpy(tmp + path_len+2, entry->d_name);
        handle->enqueue(tmp);
    }
    closedir(d);
}

// create work callback
// this is called once at the start on rank 0
// use to seed rank 0 with the initial dir to start
// searching from
static void find_create_work(CIRCLE_handle *handle) {
    handle->enqueue(item_buf);
}

// process work callback
static void find_process_work(CIRCLE_handle *handle)
{
    // dequeue the next item
    handle->dequeue(item_buf);
    if (*item_buf == 'd') {
      find_do_readdir(item_buf, handle);
    }else{
      if(! glob_delete){
        find_do_lstat(item_buf);
      }else{
        unlink(& item_buf[1]); // strip type
      }
    }
}

// arguments :
// first argument is data directory to store the lstat files
// second argument is directory to start lstating from
io500_find_results_t * io500_parallel_find_or_delete(FILE * logfile, char * workdir, char * const filename_pattern, int delete, int stonewall_timer_s) {
  out_logfile = logfile;

  char * err = realpath(workdir, start_dir);
  glob_compare_str = filename_pattern;

  res = malloc(sizeof(io500_find_results_t));
  memset(res, 0, sizeof(*res));

  glob_delete = delete;
  glob_stonewall_timer = stonewall_timer_s;
  glob_endtime = GetTimeStamp() + glob_stonewall_timer;

  DIR * sd=opendir(start_dir);
  if (err == NULL || ! sd) {
      fprintf (stderr, "Cannot open directory '%s': %s\n", start_dir, strerror (errno));
      exit (EXIT_FAILURE);
  }
  memset(item_buf, 0, sizeof(item_buf));
  sprintf(item_buf, "%c%s", 'd', start_dir);

	// initialise MPI and the libcircle stuff
  int argc = 1;
  char *argv[] = {"test"};

	CIRCLE_init(argc, argv, CIRCLE_SPLIT_RANDOM);

  CIRCLE_enable_logging(CIRCLE_LOG_FATAL);

	// set the create work callback
  CIRCLE_cb_create(& find_create_work);

	// set the process work callback
	CIRCLE_cb_process(& find_process_work);

	// enter the processing loop
	CIRCLE_begin();

	// wait for all processing to finish and then clean up
	CIRCLE_finalize();

	return res;
}
