/* -*- mode: c; c-basic-offset: 8; indent-tabs-mode: nil; -*-
 * vim:expandtab:shiftwidth=8:tabstop=8:
 */
/******************************************************************************\
*                                                                              *
*        Copyright (c) 2003, The Regents of the University of California       *
*      See the file COPYRIGHT for a complete copyright notice and license.     *
*                                                                              *
\******************************************************************************/

#ifdef HAVE_CONFIG_H
# include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>              /* tolower() */
#include <errno.h>
#include <math.h>
#include <mpi.h>
#include <string.h>
#include <sys/stat.h>           /* struct stat */
#include <time.h>

#ifndef _WIN32
# include <sys/time.h>           /* gettimeofday() */
# include <sys/utsname.h>        /* uname() */
#endif

#include <assert.h>

#include "ior.h"
#include "aiori.h"
#include "utilities.h"
#include "parse_options.h"

/* file scope globals */
extern char **environ;

static const ior_aiori_t *backend;
static int totalErrorCount = 0;


static void DestroyTests(IOR_test_t *tests_head);
static void DisplayUsage(char **);
static void GetTestFileName(char *, IOR_param_t *);
static char *PrependDir(IOR_param_t *, char *);
static char **ParseFileName(char *, int *);
static void PrintEarlyHeader();
static void PrintHeader(int argc, char **argv);
static IOR_test_t *SetupTests(int, char **);
static void ShowTestInfo(IOR_param_t *);
static void ShowSetup(IOR_param_t *params);
static void ShowTest(IOR_param_t *);
static void PrintLongSummaryAllTests(IOR_test_t *tests_head);
static void TestIoSys(IOR_test_t *);
static void ValidateTests(IOR_param_t *);
static IOR_offset_t WriteOrRead(IOR_param_t * test, IOR_results_t * results, void *fd, int access, IOR_io_buffers* ioBuffers);
static void WriteTimes(IOR_param_t *, double **, int, int);

IOR_test_t * ior_run(int argc, char **argv, MPI_Comm world_com, FILE * world_out){
        IOR_test_t *tests_head;
        IOR_test_t *tptr;
        out_logfile = world_out;
        mpi_comm_world = world_com;

        MPI_CHECK(MPI_Comm_size(mpi_comm_world, &numTasksWorld), "cannot get number of tasks");
        MPI_CHECK(MPI_Comm_rank(mpi_comm_world, &rank), "cannot get rank");
        PrintEarlyHeader();

        /* Sanity check, we were compiled with SOME backend, right? */
        if (0 == aiori_count ()) {
                ERR("No IO backends compiled into ior.  "
                    "Run 'configure --with-<backend>', and recompile.");
        }

        /* setup tests, and validate parameters */
        tests_head = SetupTests(argc, argv);
        verbose = tests_head->params.verbose;
        tests_head->params.testComm = world_com;

        /* check for commandline 'help' request */
        if (rank == 0 && tests_head->params.showHelp == TRUE) {
                DisplayUsage(argv);
        }

        PrintHeader(argc, argv);

        /* perform each test */
        for (tptr = tests_head; tptr != NULL; tptr = tptr->next) {
                verbose = tptr->params.verbose;
                if (rank == 0 && verbose >= VERBOSE_0) {
                        ShowTestInfo(&tptr->params);
                }
                if (rank == 0 && verbose >= VERBOSE_3) {
                        ShowTest(&tptr->params);
                }

                TestIoSys(tptr);

                if(rank == 0 && tptr->params.stoneWallingWearOut){
                  fprintf(out_logfile, "Pairs deadlineForStonewallingaccessed: %lld\n", (long long) tptr->results->pairs_accessed);
                }
                tptr->results->errors = totalErrorCount;
                totalErrorCount = 0;
        }

        PrintLongSummaryAllTests(tests_head);

        /* display finish time */
        if (rank == 0 && verbose >= VERBOSE_0) {
                fprintf(out_logfile, "\n");
                fprintf(out_logfile, "Finished: %s", CurrentTimeString());
        }
        return tests_head;
}


/********************************** M A I N ***********************************/

int main(int argc, char **argv)
{
        int i;
        IOR_test_t *tests_head;
        IOR_test_t *tptr;
        out_logfile = stdout;

        /*
         * check --help option from commandline without starting MPI;
         * if the help option is requested in a script file (showHelp=TRUE),
         * the help output will be displayed in the MPI job
         */
        for (i = 1; i < argc; i++) {
                if (strcmp(argv[i], "--help") == 0) {
                        DisplayUsage(argv);
                        return (0);
                }
        }

#ifdef USE_S3_AIORI
        /* This is supposed to be done before *any* threads are created.
         * Could MPI_Init() create threads (or call multi-threaded
         * libraries)?  We'll assume so. */
        AWS4C_CHECK( aws_init() );
#endif

        /* start the MPI code */
        MPI_CHECK(MPI_Init(&argc, &argv), "cannot initialize MPI");

        mpi_comm_world = MPI_COMM_WORLD;
        MPI_CHECK(MPI_Comm_size(mpi_comm_world, &numTasksWorld),
                  "cannot get number of tasks");
        MPI_CHECK(MPI_Comm_rank(mpi_comm_world, &rank), "cannot get rank");
        PrintEarlyHeader();

        /* set error-handling */
        /*MPI_CHECK(MPI_Errhandler_set(mpi_comm_world, MPI_ERRORS_RETURN),
           "cannot set errhandler"); */

        /* Sanity check, we were compiled with SOME backend, right? */
        if (0 == aiori_count ()) {
                ERR("No IO backends compiled into ior.  "
                    "Run 'configure --with-<backend>', and recompile.");
        }

        /* setup tests, and validate parameters */
        tests_head = SetupTests(argc, argv);
        verbose = tests_head->params.verbose;
        tests_head->params.testComm = mpi_comm_world;

        /* check for commandline 'help' request */
        if (tests_head->params.showHelp == TRUE) {
                if( rank == 0 ){
                  DisplayUsage(argv);
                }
                MPI_Finalize();
                exit(0);
        }

        PrintHeader(argc, argv);

        /* perform each test */
        for (tptr = tests_head; tptr != NULL; tptr = tptr->next) {
                verbose = tptr->params.verbose;
                if (rank == 0 && verbose >= VERBOSE_0) {
                        ShowTestInfo(&tptr->params);
                }
                if (rank == 0 && verbose >= VERBOSE_3) {
                        ShowTest(&tptr->params);
                }

                // This is useful for trapping a running MPI process.  While
                // this is sleeping, run the script 'testing/hdfs/gdb.attach'
                if (verbose >= VERBOSE_4) {
                        fprintf(out_logfile, "\trank %d: sleeping\n", rank);
                        sleep(5);
                        fprintf(out_logfile, "\trank %d: awake.\n", rank);
                }
                TestIoSys(tptr);

                if(rank == 0 && tptr->params.stoneWallingWearOut){
                  fprintf(out_logfile, "Pairs deadlineForStonewallingaccessed: %lld\n", (long long) tptr->results->pairs_accessed);
                }
        }

        if (verbose < 0)
                /* always print final summary */
                verbose = 0;
        PrintLongSummaryAllTests(tests_head);

        /* display finish time */
        if (rank == 0 && verbose >= VERBOSE_0) {
                fprintf(out_logfile, "\n");
                fprintf(out_logfile, "Finished: %s", CurrentTimeString());
        }

        DestroyTests(tests_head);

        MPI_CHECK(MPI_Finalize(), "cannot finalize MPI");

#ifdef USE_S3_AIORI
        /* done once per program, after exiting all threads.
         * NOTE: This fn doesn't return a value that can be checked for success. */
        aws_cleanup();
#endif

        return (totalErrorCount);
}

/***************************** F U N C T I O N S ******************************/

/*
 * Initialize an IOR_param_t structure to the defaults
 */
void init_IOR_Param_t(IOR_param_t * p)
{
        const char *default_aiori = aiori_default ();

        assert (NULL != default_aiori);

        memset(p, 0, sizeof(IOR_param_t));

        p->mode = IOR_IRUSR | IOR_IWUSR | IOR_IRGRP | IOR_IWGRP;
        p->openFlags = IOR_RDWR | IOR_CREAT;

        strncpy(p->api, default_aiori, MAX_STR);
        strncpy(p->platform, "HOST(OSTYPE)", MAX_STR);
        strncpy(p->testFileName, "testFile", MAXPATHLEN);

        p->nodes = 1;
        p->tasksPerNode = 1;
        p->repetitions = 1;
        p->repCounter = -1;
        p->open = WRITE;
        p->taskPerNodeOffset = 1;
        p->segmentCount = 1;
        p->blockSize = 1048576;
        p->transferSize = 262144;
        p->randomSeed = -1;
        p->incompressibleSeed = 573;
        p->testComm = mpi_comm_world;
        p->setAlignment = 1;
        p->lustre_start_ost = -1;

        strncpy(p->hdfs_user, getenv("USER"), MAX_STR);
        p->hdfs_name_node      = "default";
        p->hdfs_name_node_port = 0; /* ??? */
        p->hdfs_fs = NULL;
        p->hdfs_replicas = 0;   /* invokes the default */
        p->hdfs_block_size = 0;

        // p->curl       = NULL;
        p->URI = NULL;
        p->curl_flags = 0;
        p->io_buf = NULL;
        p->etags = NULL;
        p->part_number = 0;

        p->beegfs_numTargets = -1;
        p->beegfs_chunkSize = -1;
}

static void AioriBind(char* api, IOR_param_t* param)
{
        backend = aiori_select (api);
        if (NULL != backend) {
                if (! strncmp(api, "S3", 2)) {
                        if (! strcmp(api, "S3_EMC")) {
                                param->curl_flags |= IOR_CURL_S3_EMC_EXT;
                        } else {
                                param->curl_flags &= ~(IOR_CURL_S3_EMC_EXT);
                        }
                }
        } else {
                ERR("unrecognized IO API");
        }
}

static void
DisplayOutliers(int numTasks,
                double timerVal,
                char *timeString, int access, int outlierThreshold)
{
        char accessString[MAX_STR];
        double sum, mean, sqrDiff, var, sd;

        /* for local timerVal, don't compensate for wall clock delta */
        timerVal += wall_clock_delta;

        MPI_CHECK(MPI_Allreduce
                  (&timerVal, &sum, 1, MPI_DOUBLE, MPI_SUM, testComm),
                  "MPI_Allreduce()");
        mean = sum / numTasks;
        sqrDiff = pow((mean - timerVal), 2);
        MPI_CHECK(MPI_Allreduce
                  (&sqrDiff, &var, 1, MPI_DOUBLE, MPI_SUM, testComm),
                  "MPI_Allreduce()");
        var = var / numTasks;
        sd = sqrt(var);

        if (access == WRITE) {
                strcpy(accessString, "write");
        } else {                /* READ */
                strcpy(accessString, "read");
        }
        if (fabs(timerVal - mean) > (double)outlierThreshold) {
                fprintf(out_logfile, "WARNING: for task %d, %s %s is %f\n",
                        rank, accessString, timeString, timerVal);
                fprintf(out_logfile, "         (mean=%f, stddev=%f)\n", mean, sd);
                fflush(out_logfile);
        }
}

/*
 * Check for outliers in start/end times and elapsed create/xfer/close times.
 */
static void CheckForOutliers(IOR_param_t * test, double **timer, int rep,
                             int access)
{
        int shift;

        if (access == WRITE) {
                shift = 0;
        } else {                /* READ */
                shift = 6;
        }

        DisplayOutliers(test->numTasks, timer[shift + 0][rep],
                        "start time", access, test->outlierThreshold);
        DisplayOutliers(test->numTasks,
                        timer[shift + 1][rep] - timer[shift + 0][rep],
                        "elapsed create time", access, test->outlierThreshold);
        DisplayOutliers(test->numTasks,
                        timer[shift + 3][rep] - timer[shift + 2][rep],
                        "elapsed transfer time", access,
                        test->outlierThreshold);
        DisplayOutliers(test->numTasks,
                        timer[shift + 5][rep] - timer[shift + 4][rep],
                        "elapsed close time", access, test->outlierThreshold);
        DisplayOutliers(test->numTasks, timer[shift + 5][rep], "end time",
                        access, test->outlierThreshold);

}

/*
 * Check if actual file size equals expected size; if not use actual for
 * calculating performance rate.
 */
static void CheckFileSize(IOR_test_t *test, IOR_offset_t dataMoved, int rep)
{
        IOR_param_t *params = &test->params;
        IOR_results_t *results = test->results;

        MPI_CHECK(MPI_Allreduce(&dataMoved, &results->aggFileSizeFromXfer[rep],
                                1, MPI_LONG_LONG_INT, MPI_SUM, testComm),
                  "cannot total data moved");

        if (strcmp(params->api, "HDF5") != 0 && strcmp(params->api, "NCMPI") != 0) {
                if (verbose >= VERBOSE_0 && rank == 0) {
                        if ((params->expectedAggFileSize
                             != results->aggFileSizeFromXfer[rep])
                            || (results->aggFileSizeFromStat[rep]
                                != results->aggFileSizeFromXfer[rep])) {
                                fprintf(out_logfile,
                                        "WARNING: Expected aggregate file size       = %lld.\n",
                                        (long long) params->expectedAggFileSize);
                                fprintf(out_logfile,
                                        "WARNING: Stat() of aggregate file size      = %lld.\n",
                                        (long long) results->aggFileSizeFromStat[rep]);
                                fprintf(out_logfile,
                                        "WARNING: Using actual aggregate bytes moved = %lld.\n",
                                        (long long) results->aggFileSizeFromXfer[rep]);
                                if(params->deadlineForStonewalling){
                                  fprintf(out_logfile,
                                        "WARNING: maybe caused by deadlineForStonewalling\n");
                                }
                        }
                }
        }
        results->aggFileSizeForBW[rep] = results->aggFileSizeFromXfer[rep];
}

/*
 * Compare buffers after reading/writing each transfer.  Displays only first
 * difference in buffers and returns total errors counted.
 */
static size_t
CompareBuffers(void *expectedBuffer,
               void *unknownBuffer,
               size_t size,
               IOR_offset_t transferCount, IOR_param_t *test, int access)
{
        char testFileName[MAXPATHLEN];
        char bufferLabel1[MAX_STR];
        char bufferLabel2[MAX_STR];
        size_t i, j, length, first, last;
        size_t errorCount = 0;
        int inError = 0;
        unsigned long long *goodbuf = (unsigned long long *)expectedBuffer;
        unsigned long long *testbuf = (unsigned long long *)unknownBuffer;

        if (access == WRITECHECK || access == READCHECK) {
                strcpy(bufferLabel1, "Expected: ");
                strcpy(bufferLabel2, "Actual:   ");
        } else {
                ERR("incorrect argument for CompareBuffers()");
        }

        length = size / sizeof(IOR_size_t);
        first = -1;
        if (verbose >= VERBOSE_3) {
                fprintf(out_logfile,
                        "[%d] At file byte offset %lld, comparing %llu-byte transfer\n",
                        rank, test->offset, (long long)size);
        }
        for (i = 0; i < length; i++) {
                if (testbuf[i] != goodbuf[i]) {
                        errorCount++;
                        if (verbose >= VERBOSE_2) {
                                fprintf(out_logfile,
                                        "[%d] At transfer buffer #%lld, index #%lld (file byte offset %lld):\n",
                                        rank, transferCount - 1, (long long)i,
                                        test->offset +
                                        (IOR_size_t) (i * sizeof(IOR_size_t)));
                                fprintf(out_logfile, "[%d] %s0x", rank, bufferLabel1);
                                fprintf(out_logfile, "%016llx\n", goodbuf[i]);
                                fprintf(out_logfile, "[%d] %s0x", rank, bufferLabel2);
                                fprintf(out_logfile, "%016llx\n", testbuf[i]);
                        }
                        if (!inError) {
                                inError = 1;
                                first = i;
                                last = i;
                        } else {
                                last = i;
                        }
                } else if (verbose >= VERBOSE_5 && i % 4 == 0) {
                        fprintf(out_logfile,
                                "[%d] PASSED offset = %lld bytes, transfer %lld\n",
                                rank,
                                ((i * sizeof(unsigned long long)) +
                                 test->offset), transferCount);
                        fprintf(out_logfile, "[%d] GOOD %s0x", rank, bufferLabel1);
                        for (j = 0; j < 4; j++)
                                fprintf(out_logfile, "%016llx ", goodbuf[i + j]);
                        fprintf(out_logfile, "\n[%d] GOOD %s0x", rank, bufferLabel2);
                        for (j = 0; j < 4; j++)
                                fprintf(out_logfile, "%016llx ", testbuf[i + j]);
                        fprintf(out_logfile, "\n");
                }
        }
        if (inError) {
                inError = 0;
                GetTestFileName(testFileName, test);
                fprintf(out_logfile,
                        "[%d] FAILED comparison of buffer containing %d-byte ints:\n",
                        rank, (int)sizeof(unsigned long long int));
                fprintf(out_logfile, "[%d]   File name = %s\n", rank, testFileName);
                fprintf(out_logfile, "[%d]   In transfer %lld, ", rank,
                        transferCount);
                fprintf(out_logfile,
                        "%lld errors between buffer indices %lld and %lld.\n",
                        (long long)errorCount, (long long)first,
                        (long long)last);
                fprintf(out_logfile, "[%d]   File byte offset = %lld:\n", rank,
                        ((first * sizeof(unsigned long long)) + test->offset));

                fprintf(out_logfile, "[%d]     %s0x", rank, bufferLabel1);
                for (j = first; j < length && j < first + 4; j++)
                        fprintf(out_logfile, "%016llx ", goodbuf[j]);
                if (j == length)
                        fprintf(out_logfile, "[end of buffer]");
                fprintf(out_logfile, "\n[%d]     %s0x", rank, bufferLabel2);
                for (j = first; j < length && j < first + 4; j++)
                        fprintf(out_logfile, "%016llx ", testbuf[j]);
                if (j == length)
                        fprintf(out_logfile, "[end of buffer]");
                fprintf(out_logfile, "\n");
                if (test->quitOnError == TRUE)
                        ERR("data check error, aborting execution");
        }
        return (errorCount);
}

/*
 * Count all errors across all tasks; report errors found.
 */
static int CountErrors(IOR_param_t * test, int access, int errors)
{
        int allErrors = 0;

        if (test->checkWrite || test->checkRead) {
                MPI_CHECK(MPI_Reduce(&errors, &allErrors, 1, MPI_INT, MPI_SUM,
                                     0, testComm), "cannot reduce errors");
                MPI_CHECK(MPI_Bcast(&allErrors, 1, MPI_INT, 0, testComm),
                          "cannot broadcast allErrors value");
                if (allErrors != 0) {
                        totalErrorCount += allErrors;
                        test->errorFound = TRUE;
                }
                if (rank == 0 && allErrors != 0) {
                        if (allErrors < 0) {
                                WARN("overflow in errors counted");
                                allErrors = -1;
                        }
                        fprintf(out_logfile, "WARNING: incorrect data on %s (%d errors found).\n",
                                access == WRITECHECK ? "write" : "read", allErrors);
                        fprintf(out_logfile,
                                "Used Time Stamp %u (0x%x) for Data Signature\n",
                                test->timeStampSignatureValue,
                                test->timeStampSignatureValue);
                }
        }
        return (allErrors);
}

/*
 * Count the number of tasks that share a host.
 *
 * This function employees the gethostname() call, rather than using
 * MPI_Get_processor_name().  We are interested in knowing the number
 * of tasks that share a file system client (I/O node, compute node,
 * whatever that may be).  However on machines like BlueGene/Q,
 * MPI_Get_processor_name() uniquely identifies a cpu in a compute node,
 * not the node where the I/O is function shipped to.  gethostname()
 * is assumed to identify the shared filesystem client in more situations.
 *
 * NOTE: This also assumes that the task count on all nodes is equal
 * to the task count on the host running MPI task 0.
 */
int CountTasksPerNode(int numTasks, MPI_Comm comm)
{
        char localhost[MAX_STR];
        char hostname0[MAX_STR];
        static int firstPass = TRUE;
        unsigned count;
        unsigned flag;
        int rc;

        rc = gethostname(localhost, MAX_STR);
        if (rc == -1) {
                /* This node won't match task 0's hostname...except in the
                   case where ALL gethostname() calls fail, in which
                   case ALL nodes will appear to be on the same node.
                   We'll handle that later. */
                localhost[0] = '\0';
                if (rank == 0)
                        perror("gethostname() failed");
        }

        if (verbose >= VERBOSE_2 && firstPass) {
                char tmp[MAX_STR];
                sprintf(tmp, "task %d on %s", rank, localhost);
                OutputToRoot(numTasks, comm, tmp);
                firstPass = FALSE;
        }

        /* send task 0's hostname to all tasks */
        if (rank == 0)
                strcpy(hostname0, localhost);
        MPI_CHECK(MPI_Bcast(hostname0, MAX_STR, MPI_CHAR, 0, comm),
                  "broadcast of task 0's hostname failed");
        if (strcmp(hostname0, localhost) == 0)
                flag = 1;
        else
                flag = 0;

        /* count the tasks share the same host as task 0 */
        MPI_Allreduce(&flag, &count, 1, MPI_UNSIGNED, MPI_SUM, comm);

        if (hostname0[0] == '\0')
                count = 1;

        return (int)count;
}

/*
 * Allocate a page-aligned (required by O_DIRECT) buffer.
 */
static void *aligned_buffer_alloc(size_t size)
{
        size_t pageSize;
        size_t pageMask;
        char *buf, *tmp;
        char *aligned;

        pageSize = getpagesize();
        pageMask = pageSize - 1;
        buf = malloc(size + pageSize + sizeof(void *));
        if (buf == NULL)
                ERR("out of memory");
        /* find the alinged buffer */
        tmp = buf + sizeof(char *);
        aligned = tmp + pageSize - ((size_t) tmp & pageMask);
        /* write a pointer to the original malloc()ed buffer into the bytes
           preceding "aligned", so that the aligned buffer can later be free()ed */
        tmp = aligned - sizeof(void *);
        *(void **)tmp = buf;

        return (void *)aligned;
}

/*
 * Free a buffer allocated by aligned_buffer_alloc().
 */
static void aligned_buffer_free(void *buf)
{
        free(*(void **)((char *)buf - sizeof(char *)));
}

void AllocResults(IOR_test_t *test)
{
        int reps;
        if (test->results != NULL)
                return;

        reps = test->params.repetitions;
        test->results = (IOR_results_t *) malloc(sizeof(IOR_results_t));
        memset(test->results, 0, sizeof(IOR_results_t));
        if (test->results == NULL)
                ERR("malloc of IOR_results_t failed");

        test->results->writeTime = (double *)malloc(reps * sizeof(double));
        if (test->results->writeTime == NULL)
                ERR("malloc of writeTime array failed");
        memset(test->results->writeTime, 0, reps * sizeof(double));

        test->results->readTime = (double *)malloc(reps * sizeof(double));
        if (test->results->readTime == NULL)
                ERR("malloc of readTime array failed");
        memset(test->results->readTime, 0, reps * sizeof(double));

        test->results->aggFileSizeFromStat =
                (IOR_offset_t *)malloc(reps * sizeof(IOR_offset_t));
        if (test->results->aggFileSizeFromStat == NULL)
                ERR("malloc of aggFileSizeFromStat failed");

        test->results->aggFileSizeFromXfer =
                (IOR_offset_t *)malloc(reps * sizeof(IOR_offset_t));
        if (test->results->aggFileSizeFromXfer == NULL)
                ERR("malloc of aggFileSizeFromXfer failed");

        test->results->aggFileSizeForBW =
                (IOR_offset_t *)malloc(reps * sizeof(IOR_offset_t));
        if (test->results->aggFileSizeForBW == NULL)
                ERR("malloc of aggFileSizeForBW failed");

}

void FreeResults(IOR_test_t *test)
{
        if (test->results != NULL) {
                free(test->results->aggFileSizeFromStat);
                free(test->results->aggFileSizeFromXfer);
                free(test->results->aggFileSizeForBW);
                free(test->results->readTime);
                free(test->results->writeTime);
                free(test->results);
        }
}


/*
 * Create new test for list of tests.
 */
IOR_test_t *CreateTest(IOR_param_t *init_params, int test_num)
{
        IOR_test_t *newTest = NULL;

        newTest = (IOR_test_t *) malloc(sizeof(IOR_test_t));
        if (newTest == NULL)
                ERR("malloc() of IOR_test_t failed");
        newTest->params = *init_params;
        GetPlatformName(newTest->params.platform);
        newTest->params.nodes = init_params->numTasks / tasksPerNode;
        newTest->params.tasksPerNode = tasksPerNode;
        newTest->params.id = test_num;
        newTest->next = NULL;
        newTest->results = NULL;
        return newTest;
}

static void DestroyTest(IOR_test_t *test)
{
        FreeResults(test);
        free(test);
}

static void DestroyTests(IOR_test_t *tests_head)
{
        IOR_test_t *tptr, *next;

        for (tptr = tests_head; tptr != NULL; tptr = next) {
                next = tptr->next;
                DestroyTest(tptr);
        }
}

/*
 * Sleep for 'delay' seconds.
 */
static void DelaySecs(int delay)
{
        if (rank == 0 && delay > 0) {
                if (verbose >= VERBOSE_1)
                        fprintf(out_logfile, "delaying %d seconds . . .\n", delay);
                sleep(delay);
        }
}

/*
 * Display freespace (df).
 */
static void DisplayFreespace(IOR_param_t * test)
{
        char fileName[MAX_STR] = { 0 };
        int i;
        int directoryFound = FALSE;

        /* get outfile name */
        GetTestFileName(fileName, test);

        /* get directory for outfile */
        i = strlen(fileName);
        while (i-- > 0) {
                if (fileName[i] == '/') {
                        fileName[i] = '\0';
                        directoryFound = TRUE;
                        break;
                }
        }

        /* if no directory/, use '.' */
        if (directoryFound == FALSE) {
                strcpy(fileName, ".");
        }

        ShowFileSystemSize(fileName);

        return;
}

/*
 * Display usage of script file.
 */
static void DisplayUsage(char **argv)
{
        char *opts[] = {
                "OPTIONS:",
                " -a S  api --  API for I/O [POSIX|MPIIO|HDF5|HDFS|S3|S3_EMC|NCMPI]",
                " -A N  refNum -- user supplied reference number to include in the summary",
                " -b N  blockSize -- contiguous bytes to write per task  (e.g.: 8, 4k, 2m, 1g, 1t, 1p)",
                " -B    useO_DIRECT -- uses O_DIRECT for POSIX, bypassing I/O buffers",
                " -c    collective -- collective I/O",
                " -C    reorderTasks -- changes task ordering to n+1 ordering for readback",
                " -d N  interTestDelay -- delay between reps in seconds",
                " -D N  deadlineForStonewalling -- seconds before stopping write or read phase",
                " -O stoneWallingWearOut=1 -- once the stonewalling timout is over, all process finish to access the amount of data",
                " -O stoneWallingWearOutIterations=N -- stop after processing this number of iterations, needed for reading data back written with stoneWallingWearOut",
                " -e    fsync -- perform fsync upon POSIX write close",
                " -E    useExistingTestFile -- do not remove test file before write access",
                " -f S  scriptFile -- test script name",
                " -F    filePerProc -- file-per-process",
                " -g    intraTestBarriers -- use barriers between open, write/read, and close",
                " -G N  setTimeStampSignature -- set value for time stamp signature/random seed",
                " -h    showHelp -- displays options and help",
                " -H    showHints -- show hints",
                " -i N  repetitions -- number of repetitions of test",
                " -I    individualDataSets -- datasets not shared by all procs [not working]",
                " -j N  outlierThreshold -- warn on outlier N seconds from mean",
                " -J N  setAlignment -- HDF5 alignment in bytes (e.g.: 8, 4k, 2m, 1g)",
                " -k    keepFile -- don't remove the test file(s) on program exit",
                " -K    keepFileWithError  -- keep error-filled file(s) after data-checking",
                " -l    datapacket type-- type of packet that will be created [offset|incompressible|timestamp|o|i|t]",
                " -m    multiFile -- use number of reps (-i) for multiple file count",
                " -M N  memoryPerNode -- hog memory on the node  (e.g.: 2g, 75%)",
                " -n    noFill -- no fill in HDF5 file creation",
                " -N N  numTasks -- number of tasks that should participate in the test",
                " -o S  testFile -- full name for test",
                " -O S  string of IOR directives (e.g. -O checkRead=1,lustreStripeCount=32)",
                " -p    preallocate -- preallocate file size",
                " -P    useSharedFilePointer -- use shared file pointer [not working]",
                " -q    quitOnError -- during file error-checking, abort on error",
                " -Q N  taskPerNodeOffset for read tests use with -C & -Z options (-C constant N, -Z at least N)",
                " -r    readFile -- read existing file",
                " -R    checkRead -- verify that the output of read matches the expected signature (used with -G)",
                " -s N  segmentCount -- number of segments",
                " -S    useStridedDatatype -- put strided access into datatype [not working]",
                " -t N  transferSize -- size of transfer in bytes (e.g.: 8, 4k, 2m, 1g)",
                " -T N  maxTimeDuration -- max time in minutes for each test",
                " -u    uniqueDir -- use unique directory name for each file-per-process",
                " -U S  hintsFileName -- full name for hints file",
                " -v    verbose -- output information (repeating flag increases level)",
                " -V    useFileView -- use MPI_File_set_view",
                " -w    writeFile -- write file",
                " -W    checkWrite -- check read after write",
                " -x    singleXferAttempt -- do not retry transfer if incomplete",
                " -X N  reorderTasksRandomSeed -- random seed for -Z option",
                " -Y    fsyncPerWrite -- perform fsync after each POSIX write",
                " -z    randomOffset -- access is to random, not sequential, offsets within a file",
                " -Z    reorderTasksRandom -- changes task ordering to random ordering for readback",
                " ",
                "         NOTE: S is a string, N is an integer number.",
                " ",
                ""
        };
        int i = 0;

        fprintf(out_logfile, "Usage: %s [OPTIONS]\n\n", *argv);
        for (i = 0; strlen(opts[i]) > 0; i++)
                fprintf(out_logfile, "%s\n", opts[i]);

        return;
}

/*
 * Distribute IOR_HINTs to all tasks' environments.
 */
void DistributeHints(void)
{
        char hint[MAX_HINTS][MAX_STR], fullHint[MAX_STR], hintVariable[MAX_STR];
        int hintCount = 0, i;

        if (rank == 0) {
                for (i = 0; environ[i] != NULL; i++) {
                        if (strncmp(environ[i], "IOR_HINT", strlen("IOR_HINT"))
                            == 0) {
                                hintCount++;
                                if (hintCount == MAX_HINTS) {
                                        WARN("exceeded max hints; reset MAX_HINTS and recompile");
                                        hintCount = MAX_HINTS;
                                        break;
                                }
                                /* assume no IOR_HINT is greater than MAX_STR in length */
                                strncpy(hint[hintCount - 1], environ[i],
                                        MAX_STR - 1);
                        }
                }
        }

        MPI_CHECK(MPI_Bcast(&hintCount, sizeof(hintCount), MPI_BYTE,
                            0, mpi_comm_world), "cannot broadcast hints");
        for (i = 0; i < hintCount; i++) {
                MPI_CHECK(MPI_Bcast(&hint[i], MAX_STR, MPI_BYTE,
                                    0, mpi_comm_world),
                          "cannot broadcast hints");
                strcpy(fullHint, hint[i]);
                strcpy(hintVariable, strtok(fullHint, "="));
                if (getenv(hintVariable) == NULL) {
                        /* doesn't exist in this task's environment; better set it */
                        if (putenv(hint[i]) != 0)
                                WARN("cannot set environment variable");
                }
        }
}

/*
 * Fill buffer, which is transfer size bytes long, with known 8-byte long long
 * int values.  In even-numbered 8-byte long long ints, store MPI task in high
 * bits and timestamp signature in low bits.  In odd-numbered 8-byte long long
 * ints, store transfer offset.  If storeFileOffset option is used, the file
 * (not transfer) offset is stored instead.
 */

static void
FillIncompressibleBuffer(void* buffer, IOR_param_t * test)

{
        size_t i;
        unsigned long long hi, lo;
        unsigned long long *buf = (unsigned long long *)buffer;

        for (i = 0; i < test->transferSize / sizeof(unsigned long long); i++) {
                hi = ((unsigned long long) rand_r(&test->incompressibleSeed) << 32);
                lo = (unsigned long long) rand_r(&test->incompressibleSeed);
                buf[i] = hi | lo;
        }

}

unsigned int reseed_incompressible_prng = TRUE;

static void
FillBuffer(void *buffer,
           IOR_param_t * test, unsigned long long offset, int fillrank)
{
        size_t i;
        unsigned long long hi, lo;
        unsigned long long *buf = (unsigned long long *)buffer;

        if(test->dataPacketType == incompressible ) { /* Make for some non compressable buffers with randomish data */

                /* In order for write checks to work, we have to restart the psuedo random sequence */
                if(reseed_incompressible_prng == TRUE) {
                        test->incompressibleSeed = test->setTimeStampSignature + rank; /* We copied seed into timestampSignature at initialization, also add the rank to add randomness between processes */
                        reseed_incompressible_prng = FALSE;
                }
                FillIncompressibleBuffer(buffer, test);
        }

        else {
                hi = ((unsigned long long)fillrank) << 32;
                lo = (unsigned long long)test->timeStampSignatureValue;
                for (i = 0; i < test->transferSize / sizeof(unsigned long long); i++) {
                        if ((i % 2) == 0) {
                                /* evens contain MPI rank and time in seconds */
                                buf[i] = hi | lo;
                        } else {
                                /* odds contain offset */
                                buf[i] = offset + (i * sizeof(unsigned long long));
                        }
                }
        }
}

/*
 * Return string describing machine name and type.
 */
void GetPlatformName(char *platformName)
{
        char nodeName[MAX_STR], *p, *start, sysName[MAX_STR];
        struct utsname name;

        if (uname(&name) != 0) {
                EWARN("cannot get platform name");
                sprintf(sysName, "%s", "Unknown");
                sprintf(nodeName, "%s", "Unknown");
        } else {
                sprintf(sysName, "%s", name.sysname);
                sprintf(nodeName, "%s", name.nodename);
        }

        start = nodeName;
        if (strlen(nodeName) == 0) {
                p = start;
        } else {
                /* point to one character back from '\0' */
                p = start + strlen(nodeName) - 1;
        }
        /*
         * to cut off trailing node number, search backwards
         * for the first non-numeric character
         */
        while (p != start) {
                if (*p < '0' || *p > '9') {
                        *(p + 1) = '\0';
                        break;
                } else {
                        p--;
                }
        }

        sprintf(platformName, "%s(%s)", nodeName, sysName);
}

/*
 * Return test file name to access.
 * for single shared file, fileNames[0] is returned in testFileName
 */
static void GetTestFileName(char *testFileName, IOR_param_t * test)
{
        char **fileNames;
        char   initialTestFileName[MAXPATHLEN];
        char   testFileNameRoot[MAX_STR];
        char   tmpString[MAX_STR];
        int count;

        /* parse filename for multiple file systems */
        strcpy(initialTestFileName, test->testFileName);
        fileNames = ParseFileName(initialTestFileName, &count);
        if (count > 1 && test->uniqueDir == TRUE)
                ERR("cannot use multiple file names with unique directories");
        if (test->filePerProc) {
                strcpy(testFileNameRoot,
                       fileNames[((rank +
                                   rankOffset) % test->numTasks) % count]);
        } else {
                strcpy(testFileNameRoot, fileNames[0]);
        }

        /* give unique name if using multiple files */
        if (test->filePerProc) {
                /*
                 * prepend rank subdirectory before filename
                 * e.g., /dir/file => /dir/<rank>/file
                 */
                if (test->uniqueDir == TRUE) {
                        strcpy(testFileNameRoot,
                               PrependDir(test, testFileNameRoot));
                }
                sprintf(testFileName, "%s.%08d", testFileNameRoot,
                        (rank + rankOffset) % test->numTasks);
        } else {
                strcpy(testFileName, testFileNameRoot);
        }

        /* add suffix for multiple files */
        if (test->repCounter > -1) {
                sprintf(tmpString, ".%d", test->repCounter);
                strcat(testFileName, tmpString);
        }
        free (fileNames);
}

/*
 * Convert IOR_offset_t value to human readable string.  This routine uses a
 * statically-allocated buffer internally and so is not re-entrant.
 */
static char *HumanReadable(IOR_offset_t value, int base)
{
        static char valueStr[MAX_STR];
        IOR_offset_t m = 0, g = 0, t = 0;
        char m_str[8], g_str[8], t_str[8];

        if (base == BASE_TWO) {
                m = MEBIBYTE;
                g = GIBIBYTE;
                t = GIBIBYTE * 1024llu;
                strcpy(m_str, "MiB");
                strcpy(g_str, "GiB");
                strcpy(t_str, "TiB");
        } else if (base == BASE_TEN) {
                m = MEGABYTE;
                g = GIGABYTE;
                t = GIGABYTE * 1000llu;
                strcpy(m_str, "MB");
                strcpy(g_str, "GB");
                strcpy(t_str, "TB");
        }

        if (value >= t) {
                if (value % t) {
                        snprintf(valueStr, MAX_STR-1, "%.2f %s",
                                (double)((double)value / t), t_str);
                } else {
                        snprintf(valueStr, MAX_STR-1, "%d %s", (int)(value / t), t_str);
                }
        }else if (value >= g) {
                if (value % g) {
                        snprintf(valueStr, MAX_STR-1, "%.2f %s",
                                (double)((double)value / g), g_str);
                } else {
                        snprintf(valueStr, MAX_STR-1, "%d %s", (int)(value / g), g_str);
                }
        } else if (value >= m) {
                if (value % m) {
                        snprintf(valueStr, MAX_STR-1, "%.2f %s",
                                (double)((double)value / m), m_str);
                } else {
                        snprintf(valueStr, MAX_STR-1, "%d %s", (int)(value / m), m_str);
                }
        } else if (value >= 0) {
                snprintf(valueStr, MAX_STR-1, "%d bytes", (int)value);
        } else {
                snprintf(valueStr, MAX_STR-1, "-");
        }
        return valueStr;
}

/*
 * Parse file name.
 */
static char **ParseFileName(char *name, int *count)
{
        char **fileNames, *tmp, *token;
        char delimiterString[3] = { FILENAME_DELIMITER, '\n', '\0' };
        int i = 0;

        *count = 0;
        tmp = name;

        /* pass one */
        /* if something there, count the first item */
        if (*tmp != '\0') {
                (*count)++;
        }
        /* count the rest of the filenames */
        while (*tmp != '\0') {
                if (*tmp == FILENAME_DELIMITER) {
                        (*count)++;
                }
                tmp++;
        }

        fileNames = (char **)malloc((*count) * sizeof(char **));
        if (fileNames == NULL)
                ERR("out of memory");

        /* pass two */
        token = strtok(name, delimiterString);
        while (token != NULL) {
                fileNames[i] = token;
                token = strtok(NULL, delimiterString);
                i++;
        }
        return (fileNames);
}

/*
 * Pretty Print a Double.  The First parameter is a flag determining if left
 * justification should be used.  The third parameter a null-terminated string
 * that should be appended to the number field.
 */
static void PPDouble(int leftjustify, double number, char *append)
{
        char format[16];
        int width = 10;
        int precision;

        if (number < 0) {
                fprintf(out_logfile, "   -      %s", append);
                return;
        }

        if (number < 1)
                precision = 6;
        else if (number < 3600)
                precision = 2;
        else
                precision = 0;

        sprintf(format, "%%%s%d.%df%%s",
                leftjustify ? "-" : "",
                width, precision);

        fprintf(out_logfile, format, number, append);
}

/*
 * From absolute directory, insert rank as subdirectory.  Allows each task
 * to write to its own directory.  E.g., /dir/file => /dir/<rank>/file.
 */
static char *PrependDir(IOR_param_t * test, char *rootDir)
{
        char *dir;
        char fname[MAX_STR + 1];
        char *p;
        int i;

        dir = (char *)malloc(MAX_STR + 1);
        if (dir == NULL)
                ERR("out of memory");

        /* get dir name */
        strcpy(dir, rootDir);
        i = strlen(dir) - 1;
        while (i > 0) {
                if (dir[i] == '\0' || dir[i] == '/') {
                        dir[i] = '/';
                        dir[i + 1] = '\0';
                        break;
                }
                i--;
        }

        /* get file name */
        strcpy(fname, rootDir);
        p = fname;
        while (i > 0) {
                if (fname[i] == '\0' || fname[i] == '/') {
                        p = fname + (i + 1);
                        break;
                }
                i--;
        }

        /* create directory with rank as subdirectory */
        sprintf(dir, "%s%d", dir, (rank + rankOffset) % test->numTasks);

        /* dir doesn't exist, so create */
        if (access(dir, F_OK) != 0) {
                if (mkdir(dir, S_IRWXU) < 0) {
                        ERR("cannot create directory");
                }

                /* check if correct permissions */
        } else if (access(dir, R_OK) != 0 || access(dir, W_OK) != 0 ||
                   access(dir, X_OK) != 0) {
                ERR("invalid directory permissions");
        }

        /* concatenate dir and file names */
        strcat(dir, "/");
        strcat(dir, p);

        return dir;
}

/******************************************************************************/
/*
 * Reduce test results, and show if verbose set.
 */

static void ReduceIterResults(IOR_test_t *test, double **timer, int rep,
                              int access)
{
        double reduced[12] = { 0 };
  double diff[6];
  double *diff_subset;
  double totalTime;
  double bw;
        enum { RIGHT, LEFT };
        int i;
        MPI_Op op;

  assert(access == WRITE || access == READ);

        /* Find the minimum start time of the even numbered timers, and the
           maximum finish time for the odd numbered timers */
        for (i = 0; i < 12; i++) {
                op = i % 2 ? MPI_MAX : MPI_MIN;
                MPI_CHECK(MPI_Reduce(&timer[i][rep], &reduced[i], 1, MPI_DOUBLE,
                                     op, 0, testComm), "MPI_Reduce()");
        }

        if (rank != 0) {
    /* Only rank 0 tallies and prints the results. */
    return;
  }

  /* Calculate elapsed times and throughput numbers */
  for (i = 0; i < 6; i++) {
    diff[i] = reduced[2 * i + 1] - reduced[2 * i];
  }
  if (access == WRITE) {
    totalTime = reduced[5] - reduced[0];
    test->results->writeTime[rep] = totalTime;
    diff_subset = &diff[0];
  } else { /* READ */
    totalTime = reduced[11] - reduced[6];
    test->results->readTime[rep] = totalTime;
    diff_subset = &diff[3];
  }

        if (verbose < VERBOSE_0) {
    return;
  }

  fprintf(out_logfile, "%-10s", access == WRITE ? "write" : "read");
  bw = (double)test->results->aggFileSizeForBW[rep] / totalTime;
  PPDouble(LEFT, bw / MEBIBYTE, " ");
  PPDouble(LEFT, (double)test->params.blockSize / KIBIBYTE, " ");
  PPDouble(LEFT, (double)test->params.transferSize / KIBIBYTE, " ");
  PPDouble(LEFT, diff_subset[0], " ");
  PPDouble(LEFT, diff_subset[1], " ");
  PPDouble(LEFT, diff_subset[2], " ");
  PPDouble(LEFT, totalTime, " ");
  fprintf(out_logfile, "%-4d\n", rep);

  fflush(out_logfile);
}

static void PrintRemoveTiming(double start, double finish, int rep)
{
        if (rank != 0 || verbose < VERBOSE_0)
    return;

        fprintf(out_logfile, "remove    -          -          -          -          -          -          ");
        PPDouble(1, finish-start, " ");
        fprintf(out_logfile, "%-4d\n", rep);
}

/*
 * Check for file(s), then remove all files if file-per-proc, else single file.
 *
 */
static void RemoveFile(char *testFileName, int filePerProc, IOR_param_t * test)
{
        int tmpRankOffset = 0;
        if (filePerProc) {
                /* in random tasks, delete own file */
                if (test->reorderTasksRandom == TRUE) {
                        tmpRankOffset = rankOffset;
                        rankOffset = 0;
                        GetTestFileName(testFileName, test);
                }
                if (access(testFileName, F_OK) == 0) {
                        backend->delete(testFileName, test);
                }
                if (test->reorderTasksRandom == TRUE) {
                        rankOffset = tmpRankOffset;
                        GetTestFileName(testFileName, test);
                }
        } else {
                // BUG: "access()" assumes a POSIX filesystem.  Maybe use
                //      backend->get_file_size(), instead, (and catch
                //      errors), or extend the aiori struct to include
                //      something to safely check for existence of the
                //      "file".
                //
                if ((rank == 0) && (access(testFileName, F_OK) == 0)) {
                        backend->delete(testFileName, test);
                }
        }
}

/*
 * Setup tests by parsing commandline and creating test script.
 * Perform a sanity-check on the configured parameters.
 */
static IOR_test_t *SetupTests(int argc, char **argv)
{
        IOR_test_t *tests, *testsHead;

        /* count the tasks per node */
        tasksPerNode = CountTasksPerNode(numTasksWorld, mpi_comm_world);

        testsHead = tests = ParseCommandLine(argc, argv);
        /*
         * Since there is no guarantee that anyone other than
         * task 0 has the environment settings for the hints, pass
         * the hint=value pair to everyone else in mpi_comm_world
         */
        DistributeHints();

        /* check validity of tests and create test queue */
        while (tests != NULL) {
                ValidateTests(&tests->params);
                tests = tests->next;
        }

        init_clock();

        /* seed random number generator */
        SeedRandGen(mpi_comm_world);

        return (testsHead);
}

/*
 * Setup transfer buffers, creating and filling as needed.
 */
static void XferBuffersSetup(IOR_io_buffers* ioBuffers, IOR_param_t* test,
                             int pretendRank)
{
        ioBuffers->buffer = aligned_buffer_alloc(test->transferSize);

        FillBuffer(ioBuffers->buffer, test, 0, pretendRank);

        if (test->checkWrite || test->checkRead) {
                ioBuffers->checkBuffer = aligned_buffer_alloc(test->transferSize);
        }
        if (test->checkRead) {
                ioBuffers->readCheckBuffer = aligned_buffer_alloc(test->transferSize);
        }

        return;
}

/*
 * Free transfer buffers.
 */
static void XferBuffersFree(IOR_io_buffers* ioBuffers, IOR_param_t* test)

{
        aligned_buffer_free(ioBuffers->buffer);

        if (test->checkWrite || test->checkRead) {
                aligned_buffer_free(ioBuffers->checkBuffer);
        }
        if (test->checkRead) {
                aligned_buffer_free(ioBuffers->readCheckBuffer);
        }

        return;
}


/*
 * Message to print immediately after MPI_Init so we know that
 * ior has started.
 */
static void PrintEarlyHeader()
{
        if (rank != 0)
                return;

        fprintf(out_logfile, "IOR-" META_VERSION ": MPI Coordinated Test of Parallel I/O\n");
        fprintf(out_logfile, "\n");
        fflush(out_logfile);
}

static void PrintHeader(int argc, char **argv)
{
        struct utsname unamebuf;
        int i;

        if (rank != 0)
                return;

        fprintf(out_logfile, "Began: %s", CurrentTimeString());
        fprintf(out_logfile, "Command line used: %s", argv[0]);
        for (i = 1; i < argc; i++) {
                fprintf(out_logfile, " \"%s\"", argv[i]);
        }
        fprintf(out_logfile, "\n");
        if (uname(&unamebuf) != 0) {
                EWARN("uname failed");
                fprintf(out_logfile, "Machine: Unknown");
        } else {
                fprintf(out_logfile, "Machine: %s %s", unamebuf.sysname,
                        unamebuf.nodename);
                if (verbose >= VERBOSE_2) {
                        fprintf(out_logfile, " %s %s %s", unamebuf.release,
                                unamebuf.version, unamebuf.machine);
                }
        }
        fprintf(out_logfile, "\n");
#ifdef _NO_MPI_TIMER
        if (verbose >= VERBOSE_2)
                fprintf(out_logfile, "Using unsynchronized POSIX timer\n");
#else                           /* not _NO_MPI_TIMER */
        if (MPI_WTIME_IS_GLOBAL) {
                if (verbose >= VERBOSE_2)
                        fprintf(out_logfile, "Using synchronized MPI timer\n");
        } else {
                if (verbose >= VERBOSE_2)
                        fprintf(out_logfile, "Using unsynchronized MPI timer\n");
        }
#endif                          /* _NO_MPI_TIMER */
        if (verbose >= VERBOSE_1) {
                fprintf(out_logfile, "Start time skew across all tasks: %.02f sec\n",
                        wall_clock_deviation);
        }
        if (verbose >= VERBOSE_3) {     /* show env */
                fprintf(out_logfile, "STARTING ENVIRON LOOP\n");
                for (i = 0; environ[i] != NULL; i++) {
                        fprintf(out_logfile, "%s\n", environ[i]);
                }
                fprintf(out_logfile, "ENDING ENVIRON LOOP\n");
        }
        fflush(out_logfile);
}

/*
 * Print header information for test output.
 */
static void ShowTestInfo(IOR_param_t *params)
{
        fprintf(out_logfile, "\n");
        fprintf(out_logfile, "Test %d started: %s", params->id, CurrentTimeString());
        if (verbose >= VERBOSE_1) {
                /* if pvfs2:, then skip */
                if (Regex(params->testFileName, "^[a-z][a-z].*:") == 0) {
                        DisplayFreespace(params);
                }
        }
        fflush(out_logfile);
}

/*
 * Show simple test output with max results for iterations.
 */
static void ShowSetup(IOR_param_t *params)
{

        if (strcmp(params->debug, "") != 0) {
                fprintf(out_logfile, "\n*** DEBUG MODE ***\n");
                fprintf(out_logfile, "*** %s ***\n\n", params->debug);
        }
        fprintf(out_logfile, "Summary:\n");
        fprintf(out_logfile, "\tapi                = %s\n", params->apiVersion);
        fprintf(out_logfile, "\ttest filename      = %s\n", params->testFileName);
        fprintf(out_logfile, "\taccess             = ");
        fprintf(out_logfile, params->filePerProc ? "file-per-process" : "single-shared-file");
        if (verbose >= VERBOSE_1 && strcmp(params->api, "POSIX") != 0) {
                fprintf(out_logfile, params->collective == FALSE ? ", independent" : ", collective");
        }
        fprintf(out_logfile, "\n");
        if (verbose >= VERBOSE_1) {
                if (params->segmentCount > 1) {
                        fprintf(out_logfile,
                                "\tpattern            = strided (%d segments)\n",
                                (int)params->segmentCount);
                } else {
                        fprintf(out_logfile,
                                "\tpattern            = segmented (1 segment)\n");
                }
        }
        fprintf(out_logfile, "\tordering in a file =");
        if (params->randomOffset == FALSE) {
                fprintf(out_logfile, " sequential offsets\n");
        } else {
                fprintf(out_logfile, " random offsets\n");
        }
        fprintf(out_logfile, "\tordering inter file=");
        if (params->reorderTasks == FALSE && params->reorderTasksRandom == FALSE) {
                fprintf(out_logfile, " no tasks offsets\n");
        }
        if (params->reorderTasks == TRUE) {
                fprintf(out_logfile, " constant task offsets = %d\n",
                        params->taskPerNodeOffset);
        }
        if (params->reorderTasksRandom == TRUE) {
                fprintf(out_logfile, " random task offsets >= %d, seed=%d\n",
                        params->taskPerNodeOffset, params->reorderTasksRandomSeed);
        }
        fprintf(out_logfile, "\tclients            = %d (%d per node)\n",
                params->numTasks, params->tasksPerNode);
        if (params->memoryPerTask != 0)
                fprintf(out_logfile, "\tmemoryPerTask      = %s\n",
                       HumanReadable(params->memoryPerTask, BASE_TWO));
        if (params->memoryPerNode != 0)
                fprintf(out_logfile, "\tmemoryPerNode      = %s\n",
                       HumanReadable(params->memoryPerNode, BASE_TWO));
        fprintf(out_logfile, "\trepetitions        = %d\n", params->repetitions);
        fprintf(out_logfile, "\txfersize           = %s\n",
                HumanReadable(params->transferSize, BASE_TWO));
        fprintf(out_logfile, "\tblocksize          = %s\n",
                HumanReadable(params->blockSize, BASE_TWO));
        fprintf(out_logfile, "\taggregate filesize = %s\n",
                HumanReadable(params->expectedAggFileSize, BASE_TWO));
#ifdef HAVE_LUSTRE_LUSTRE_USER_H
        if (params->lustre_set_striping) {
                fprintf(out_logfile, "\tLustre stripe size = %s\n",
                       ((params->lustre_stripe_size == 0) ? "Use default" :
                        HumanReadable(params->lustre_stripe_size, BASE_TWO)));
                if (params->lustre_stripe_count == 0) {
                        fprintf(out_logfile, "\t      stripe count = %s\n", "Use default");
                } else {
                        fprintf(out_logfile, "\t      stripe count = %d\n",
                               params->lustre_stripe_count);
                }
        }
#endif /* HAVE_LUSTRE_LUSTRE_USER_H */
        if (params->deadlineForStonewalling > 0) {
                fprintf(out_logfile, "\tUsing stonewalling = %d second(s)%s\n",
                        params->deadlineForStonewalling, params->stoneWallingWearOut ? " with phase out" : "");
        }
        fflush(out_logfile);
}

/*
 * Show test description.
 */
static void ShowTest(IOR_param_t * test)
{
        const char* data_packets[] = {"g", "t","o","i"};

        fprintf(out_logfile, "TEST:\t%s=%d\n", "id", test->id);
        fprintf(out_logfile, "\t%s=%d\n", "refnum", test->referenceNumber);
        fprintf(out_logfile, "\t%s=%s\n", "api", test->api);
        fprintf(out_logfile, "\t%s=%s\n", "platform", test->platform);
        fprintf(out_logfile, "\t%s=%s\n", "testFileName", test->testFileName);
        fprintf(out_logfile, "\t%s=%s\n", "hintsFileName", test->hintsFileName);
        fprintf(out_logfile, "\t%s=%d\n", "deadlineForStonewall",
                test->deadlineForStonewalling);
        fprintf(out_logfile, "\t%s=%d\n", "stoneWallingWearOut", test->stoneWallingWearOut);
        fprintf(out_logfile, "\t%s=%d\n", "maxTimeDuration", test->maxTimeDuration);
        fprintf(out_logfile, "\t%s=%d\n", "outlierThreshold",
                test->outlierThreshold);
        fprintf(out_logfile, "\t%s=%s\n", "options", test->options);
        fprintf(out_logfile, "\t%s=%d\n", "nodes", test->nodes);
        fprintf(out_logfile, "\t%s=%lu\n", "memoryPerTask", (unsigned long) test->memoryPerTask);
        fprintf(out_logfile, "\t%s=%lu\n", "memoryPerNode", (unsigned long) test->memoryPerNode);
        fprintf(out_logfile, "\t%s=%d\n", "tasksPerNode", tasksPerNode);
        fprintf(out_logfile, "\t%s=%d\n", "repetitions", test->repetitions);
        fprintf(out_logfile, "\t%s=%d\n", "multiFile", test->multiFile);
        fprintf(out_logfile, "\t%s=%d\n", "interTestDelay", test->interTestDelay);
        fprintf(out_logfile, "\t%s=%d\n", "fsync", test->fsync);
        fprintf(out_logfile, "\t%s=%d\n", "fsYncperwrite", test->fsyncPerWrite);
        fprintf(out_logfile, "\t%s=%d\n", "useExistingTestFile",
                test->useExistingTestFile);
        fprintf(out_logfile, "\t%s=%d\n", "showHints", test->showHints);
        fprintf(out_logfile, "\t%s=%d\n", "uniqueDir", test->uniqueDir);
        fprintf(out_logfile, "\t%s=%d\n", "showHelp", test->showHelp);
        fprintf(out_logfile, "\t%s=%d\n", "individualDataSets",
                test->individualDataSets);
        fprintf(out_logfile, "\t%s=%d\n", "singleXferAttempt",
                test->singleXferAttempt);
        fprintf(out_logfile, "\t%s=%d\n", "readFile", test->readFile);
        fprintf(out_logfile, "\t%s=%d\n", "writeFile", test->writeFile);
        fprintf(out_logfile, "\t%s=%d\n", "filePerProc", test->filePerProc);
        fprintf(out_logfile, "\t%s=%d\n", "reorderTasks", test->reorderTasks);
        fprintf(out_logfile, "\t%s=%d\n", "reorderTasksRandom",
                test->reorderTasksRandom);
        fprintf(out_logfile, "\t%s=%d\n", "reorderTasksRandomSeed",
                test->reorderTasksRandomSeed);
        fprintf(out_logfile, "\t%s=%d\n", "randomOffset", test->randomOffset);
        fprintf(out_logfile, "\t%s=%d\n", "checkWrite", test->checkWrite);
        fprintf(out_logfile, "\t%s=%d\n", "checkRead", test->checkRead);
        fprintf(out_logfile, "\t%s=%d\n", "preallocate", test->preallocate);
        fprintf(out_logfile, "\t%s=%d\n", "useFileView", test->useFileView);
        fprintf(out_logfile, "\t%s=%lld\n", "setAlignment", test->setAlignment);
        fprintf(out_logfile, "\t%s=%d\n", "storeFileOffset", test->storeFileOffset);
        fprintf(out_logfile, "\t%s=%d\n", "useSharedFilePointer",
                test->useSharedFilePointer);
        fprintf(out_logfile, "\t%s=%d\n", "useO_DIRECT", test->useO_DIRECT);
        fprintf(out_logfile, "\t%s=%d\n", "useStridedDatatype",
                test->useStridedDatatype);
        fprintf(out_logfile, "\t%s=%d\n", "keepFile", test->keepFile);
        fprintf(out_logfile, "\t%s=%d\n", "keepFileWithError",
                test->keepFileWithError);
        fprintf(out_logfile, "\t%s=%d\n", "quitOnError", test->quitOnError);
        fprintf(out_logfile, "\t%s=%d\n", "verbose", verbose);
        fprintf(out_logfile, "\t%s=%s\n", "data packet type", data_packets[test->dataPacketType]);
        fprintf(out_logfile, "\t%s=%d\n", "setTimeStampSignature/incompressibleSeed",
                test->setTimeStampSignature); /* Seed value was copied into setTimeStampSignature as well */
        fprintf(out_logfile, "\t%s=%d\n", "collective", test->collective);
        fprintf(out_logfile, "\t%s=%lld", "segmentCount", test->segmentCount);
#ifdef HAVE_GPFS_FCNTL_H
        fprintf(out_logfile, "\t%s=%d\n", "gpfsHintAccess", test->gpfs_hint_access);
        fprintf(out_logfile, "\t%s=%d\n", "gpfsReleaseToken", test->gpfs_release_token);
#endif
        if (strcmp(test->api, "HDF5") == 0) {
                fprintf(out_logfile, " (datasets)");
        }
        fprintf(out_logfile, "\n");
        fprintf(out_logfile, "\t%s=%lld\n", "transferSize", test->transferSize);
        fprintf(out_logfile, "\t%s=%lld\n", "blockSize", test->blockSize);
}

static double mean_of_array_of_doubles(double *values, int len)
{
        double tot = 0.0;
        int i;

        for (i = 0; i < len; i++) {
                tot += values[i];
        }
        return tot / len;

}

struct results {
        double min;
        double max;
        double mean;
        double var;
        double sd;
        double sum;
        double *val;
};

static struct results *bw_values(int reps, IOR_offset_t *agg_file_size, double *vals)
{
        struct results *r;
        int i;

        r = (struct results *)malloc(sizeof(struct results)
                                     + (reps * sizeof(double)));
        if (r == NULL)
                ERR("malloc failed");
        r->val = (double *)&r[1];

        for (i = 0; i < reps; i++) {
                r->val[i] = (double)agg_file_size[i] / vals[i];
                if (i == 0) {
                        r->min = r->val[i];
                        r->max = r->val[i];
                        r->sum = 0.0;
                }
                r->min = MIN(r->min, r->val[i]);
                r->max = MAX(r->max, r->val[i]);
                r->sum += r->val[i];
        }
        r->mean = r->sum / reps;
        r->var = 0.0;
        for (i = 0; i < reps; i++) {
                r->var += pow((r->mean - r->val[i]), 2);
        }
        r->var = r->var / reps;
        r->sd = sqrt(r->var);

        return r;
}

/*
 * Summarize results
 *
 * operation is typically "write" or "read"
 */
static void PrintLongSummaryOneOperation(IOR_test_t *test, double *times, char *operation)
{
        IOR_param_t *params = &test->params;
        IOR_results_t *results = test->results;
        struct results *bw;
        int reps;

        if (rank != 0 || verbose < VERBOSE_0)
                return;

        reps = params->repetitions;

        bw = bw_values(reps, results->aggFileSizeForBW, times);

        fprintf(out_logfile, "%-9s ", operation);
        fprintf(out_logfile, "%10.2f ", bw->max / MEBIBYTE);
        fprintf(out_logfile, "%10.2f ", bw->min / MEBIBYTE);
        fprintf(out_logfile, "%10.2f ", bw->mean / MEBIBYTE);
        fprintf(out_logfile, "%10.2f ", bw->sd / MEBIBYTE);
        fprintf(out_logfile, "%10.5f ",
                mean_of_array_of_doubles(times, reps));
        fprintf(out_logfile, "%d ", params->id);
        fprintf(out_logfile, "%d ", params->numTasks);
        fprintf(out_logfile, "%d ", params->tasksPerNode);
        fprintf(out_logfile, "%d ", params->repetitions);
        fprintf(out_logfile, "%d ", params->filePerProc);
        fprintf(out_logfile, "%d ", params->reorderTasks);
        fprintf(out_logfile, "%d ", params->taskPerNodeOffset);
        fprintf(out_logfile, "%d ", params->reorderTasksRandom);
        fprintf(out_logfile, "%d ", params->reorderTasksRandomSeed);
        fprintf(out_logfile, "%lld ", params->segmentCount);
        fprintf(out_logfile, "%lld ", params->blockSize);
        fprintf(out_logfile, "%lld ", params->transferSize);
        fprintf(out_logfile, "%lld ", results->aggFileSizeForBW[0]);
        fprintf(out_logfile, "%s ", params->api);
        fprintf(out_logfile, "%d", params->referenceNumber);
        fprintf(out_logfile, "\n");
        fflush(out_logfile);

        free(bw);
}

static void PrintLongSummaryOneTest(IOR_test_t *test)
{
        IOR_param_t *params = &test->params;
        IOR_results_t *results = test->results;

        if (params->writeFile)
                PrintLongSummaryOneOperation(test, results->writeTime, "write");
        if (params->readFile)
                PrintLongSummaryOneOperation(test, results->readTime, "read");
}

static void PrintLongSummaryHeader()
{
        if (rank != 0 || verbose < VERBOSE_0)
                return;

        fprintf(out_logfile, "\n");
        fprintf(out_logfile, "%-9s %10s %10s %10s %10s %10s",
                "Operation", "Max(MiB)", "Min(MiB)", "Mean(MiB)", "StdDev",
                "Mean(s)");
        fprintf(out_logfile, " Test# #Tasks tPN reps fPP reord reordoff reordrand seed"
                " segcnt blksiz xsize aggsize API RefNum\n");
}

static void PrintLongSummaryAllTests(IOR_test_t *tests_head)
{
        IOR_test_t *tptr;

        if (rank != 0 || verbose < VERBOSE_0)
                return;

        fprintf(out_logfile, "\n");
        fprintf(out_logfile, "Summary of all tests:");
        PrintLongSummaryHeader();

        for (tptr = tests_head; tptr != NULL; tptr = tptr->next) {
                PrintLongSummaryOneTest(tptr);
        }
}

static void PrintShortSummary(IOR_test_t * test)
{
        IOR_param_t *params = &test->params;
        IOR_results_t *results = test->results;
        double max_write = 0.0;
        double max_read = 0.0;
        double bw;
        int reps;
        int i;

        if (rank != 0 || verbose < VERBOSE_0)
                return;

        reps = params->repetitions;

        max_write = results->writeTime[0];
        max_read = results->readTime[0];
        for (i = 0; i < reps; i++) {
                bw = (double)results->aggFileSizeForBW[i]/results->writeTime[i];
                max_write = MAX(bw, max_write);
                bw = (double)results->aggFileSizeForBW[i]/results->readTime[i];
                max_read = MAX(bw, max_read);
        }

        fprintf(out_logfile, "\n");
        if (params->writeFile) {
                fprintf(out_logfile, "Max Write: %.2f MiB/sec (%.2f MB/sec)\n",
                        max_write/MEBIBYTE, max_write/MEGABYTE);
        }
        if (params->readFile) {
                fprintf(out_logfile, "Max Read:  %.2f MiB/sec (%.2f MB/sec)\n",
                        max_read/MEBIBYTE, max_read/MEGABYTE);
        }
}

/*
 * malloc a buffer, touching every page in an attempt to defeat lazy allocation.
 */
static void *malloc_and_touch(size_t size)
{
        size_t page_size;
        char *buf;
        char *ptr;

        if (size == 0)
                return NULL;

        page_size = sysconf(_SC_PAGESIZE);

        buf = (char *)malloc(size);
        if (buf == NULL)
                return NULL;

        for (ptr = buf; ptr < buf+size; ptr += page_size) {
                *ptr = (char)1;
        }

        return (void *)buf;
}

static void file_hits_histogram(IOR_param_t *params)
{
        int *rankoffs = NULL;
        int *filecont = NULL;
        int *filehits = NULL;
        int ifile;
        int jfile;

        if (rank == 0) {
                rankoffs = (int *)malloc(params->numTasks * sizeof(int));
                filecont = (int *)malloc(params->numTasks * sizeof(int));
                filehits = (int *)malloc(params->numTasks * sizeof(int));
        }

        MPI_CHECK(MPI_Gather(&rankOffset, 1, MPI_INT, rankoffs,
                             1, MPI_INT, 0, mpi_comm_world),
                  "MPI_Gather error");

        if (rank != 0)
                return;

        memset((void *)filecont, 0, params->numTasks * sizeof(int));
        for (ifile = 0; ifile < params->numTasks; ifile++) {
                filecont[(ifile + rankoffs[ifile]) % params->numTasks]++;
        }
        memset((void *)filehits, 0, params->numTasks * sizeof(int));
        for (ifile = 0; ifile < params->numTasks; ifile++)
                for (jfile = 0; jfile < params->numTasks; jfile++) {
                        if (ifile == filecont[jfile])
                                filehits[ifile]++;
                }
        fprintf(out_logfile, "#File Hits Dist:");
        jfile = 0;
        ifile = 0;
        while (jfile < params->numTasks && ifile < params->numTasks) {
                fprintf(out_logfile, " %d", filehits[ifile]);
                jfile += filehits[ifile], ifile++;
        }
        fprintf(out_logfile, "\n");
        free(rankoffs);
        free(filecont);
        free(filehits);
}


int test_time_elapsed(IOR_param_t *params, double startTime)
{
        double endTime;

        if (params->maxTimeDuration == 0)
                return 0;

        endTime = startTime + (params->maxTimeDuration * 60);

        return GetTimeStamp() >= endTime;
}

/*
 * hog some memory as a rough simulation of a real application's memory use
 */
static void *HogMemory(IOR_param_t *params)
{
        size_t size;
        void *buf;

        if (params->memoryPerTask != 0) {
                size = params->memoryPerTask;
        } else if (params->memoryPerNode != 0) {
                if (verbose >= VERBOSE_3)
                        fprintf(out_logfile, "This node hogging %ld bytes of memory\n",
                                params->memoryPerNode);
                size = params->memoryPerNode / params->tasksPerNode;
        } else {
                return NULL;
        }

        if (verbose >= VERBOSE_3)
                fprintf(out_logfile, "This task hogging %ld bytes of memory\n", size);

        buf = malloc_and_touch(size);
        if (buf == NULL)
                ERR("malloc of simulated applciation buffer failed");

        return buf;
}

/*
 * Using the test parameters, run iteration(s) of single test.
 */
static void TestIoSys(IOR_test_t *test)
{
        IOR_param_t *params = &test->params;
        IOR_results_t *results = test->results;
        char testFileName[MAX_STR];
        double *timer[12];
        double startTime;
        int pretendRank;
        int i, rep;
        void *fd;
        MPI_Group orig_group, new_group;
        int range[3];
        IOR_offset_t dataMoved; /* for data rate calculation */
        void *hog_buf;
        IOR_io_buffers ioBuffers;

        /* set up communicator for test */
        if (params->numTasks > numTasksWorld) {
                if (rank == 0) {
                        fprintf(out_logfile,
                                "WARNING: More tasks requested (%d) than available (%d),",
                                params->numTasks, numTasksWorld);
                        fprintf(out_logfile, "         running on %d tasks.\n",
                                numTasksWorld);
                }
                params->numTasks = numTasksWorld;
        }
        MPI_CHECK(MPI_Comm_group(mpi_comm_world, &orig_group),
                  "MPI_Comm_group() error");
        range[0] = 0;                     /* first rank */
        range[1] = params->numTasks - 1;  /* last rank */
        range[2] = 1;                     /* stride */
        MPI_CHECK(MPI_Group_range_incl(orig_group, 1, &range, &new_group),
                  "MPI_Group_range_incl() error");
        MPI_CHECK(MPI_Comm_create(mpi_comm_world, new_group, &testComm),
                  "MPI_Comm_create() error");
        MPI_CHECK(MPI_Group_free(&orig_group), "MPI_Group_Free() error");
        MPI_CHECK(MPI_Group_free(&new_group), "MPI_Group_Free() error");
        params->testComm = testComm;
        if (testComm == MPI_COMM_NULL) {
                /* tasks not in the group do not participate in this test */
                MPI_CHECK(MPI_Barrier(mpi_comm_world), "barrier error");
                return;
        }
        if (rank == 0 && verbose >= VERBOSE_1) {
                fprintf(out_logfile, "Participating tasks: %d\n", params->numTasks);
                fflush(out_logfile);
        }
        if (rank == 0 && params->reorderTasks == TRUE && verbose >= VERBOSE_1) {
                fprintf(out_logfile,
                        "Using reorderTasks '-C' (expecting block, not cyclic, task assignment)\n");
                fflush(out_logfile);
        }
        params->tasksPerNode = CountTasksPerNode(params->numTasks, testComm);

        /* setup timers */
        for (i = 0; i < 12; i++) {
                timer[i] = (double *)malloc(params->repetitions * sizeof(double));
                if (timer[i] == NULL)
                        ERR("malloc failed");
        }

        /* bind I/O calls to specific API */
        AioriBind(params->api, params);

        /* show test setup */
        if (rank == 0 && verbose >= VERBOSE_0)
                ShowSetup(params);

        hog_buf = HogMemory(params);

        pretendRank = (rank + rankOffset) % params->numTasks;

        /* IO Buffer Setup */

        if (params->setTimeStampSignature) { // initialize the buffer properly
                params->timeStampSignatureValue = (unsigned int)params->setTimeStampSignature;
        }
        XferBuffersSetup(&ioBuffers, params, pretendRank);
        reseed_incompressible_prng = TRUE; // reset pseudo random generator, necessary to guarantee the next call to FillBuffer produces the same value as it is right now

        /* Initial time stamp */
        startTime = GetTimeStamp();

        /* loop over test iterations */
        for (rep = 0; rep < params->repetitions; rep++) {

                /* Get iteration start time in seconds in task 0 and broadcast to
                   all tasks */
                if (rank == 0) {
                        if (! params->setTimeStampSignature) {
                                time_t currentTime;
                                if ((currentTime = time(NULL)) == -1) {
                                        ERR("cannot get current time");
                                }
                                params->timeStampSignatureValue =
                                        (unsigned int)currentTime;
                                if (verbose >= VERBOSE_2) {
                                        fprintf(out_logfile,
                                                "Using Time Stamp %u (0x%x) for Data Signature\n",
                                                params->timeStampSignatureValue,
                                                params->timeStampSignatureValue);
                                }
                        }
                        if (rep == 0 && verbose >= VERBOSE_0) {
                                fprintf(out_logfile, "\n");
                                fprintf(out_logfile, "access    bw(MiB/s)  block(KiB) xfer(KiB)  open(s)    wr/rd(s)   close(s)   total(s)   iter\n");
                                fprintf(out_logfile, "------    ---------  ---------- ---------  --------   --------   --------   --------   ----\n");
                        }
                }
                MPI_CHECK(MPI_Bcast
                          (&params->timeStampSignatureValue, 1, MPI_UNSIGNED, 0,
                           testComm), "cannot broadcast start time value");
                /* use repetition count for number of multiple files */
                if (params->multiFile)
                        params->repCounter = rep;

                /*
                 * write the file(s), getting timing between I/O calls
                 */

                if (params->writeFile && !test_time_elapsed(params, startTime)) {
                        GetTestFileName(testFileName, params);
                        if (verbose >= VERBOSE_3) {
                                fprintf(out_logfile, "task %d writing %s\n", rank,
                                        testFileName);
                        }
                        DelaySecs(params->interTestDelay);
                        if (params->useExistingTestFile == FALSE) {
                                RemoveFile(testFileName, params->filePerProc,
                                           params);
                        }
                        MPI_CHECK(MPI_Barrier(testComm), "barrier error");
                        params->open = WRITE;
                        timer[0][rep] = GetTimeStamp();
                        fd = backend->create(testFileName, params);
                        timer[1][rep] = GetTimeStamp();
                        if (params->intraTestBarriers)
                                MPI_CHECK(MPI_Barrier(testComm),
                                          "barrier error");
                        if (rank == 0 && verbose >= VERBOSE_1) {
                                fprintf(out_logfile,
                                        "Commencing write performance test: %s",
                                        CurrentTimeString());
                        }
                        timer[2][rep] = GetTimeStamp();
                        dataMoved = WriteOrRead(params, results, fd, WRITE, &ioBuffers);
                        if (params->verbose >= VERBOSE_4) {
                          fprintf(out_logfile, "* data moved = %llu\n", dataMoved);
                          fflush(out_logfile);
                        }
                        timer[3][rep] = GetTimeStamp();
                        if (params->intraTestBarriers)
                                MPI_CHECK(MPI_Barrier(testComm),
                                          "barrier error");
                        timer[4][rep] = GetTimeStamp();
                        backend->close(fd, params);

                        timer[5][rep] = GetTimeStamp();
                        MPI_CHECK(MPI_Barrier(testComm), "barrier error");

                        /* get the size of the file just written */
                        results->aggFileSizeFromStat[rep] =
                                backend->get_file_size(params, testComm, testFileName);

                        /* check if stat() of file doesn't equal expected file size,
                           use actual amount of byte moved */
                        CheckFileSize(test, dataMoved, rep);

                        if (verbose >= VERBOSE_3)
                                WriteTimes(params, timer, rep, WRITE);
                        ReduceIterResults(test, timer, rep, WRITE);
                        if (params->outlierThreshold) {
                                CheckForOutliers(params, timer, rep, WRITE);
                        }
                }

                /*
                 * perform a check of data, reading back data and comparing
                 * against what was expected to be written
                 */
                if (params->checkWrite && !test_time_elapsed(params, startTime)) {
                        MPI_CHECK(MPI_Barrier(testComm), "barrier error");
                        if (rank == 0 && verbose >= VERBOSE_1) {
                                fprintf(out_logfile,
                                        "Verifying contents of the file(s) just written.\n");
                                fprintf(out_logfile, "%s\n", CurrentTimeString());
                        }
                        if (params->reorderTasks) {
                                /* move two nodes away from writing node */
                                rankOffset =
                                        (2 * params->tasksPerNode) % params->numTasks;
                        }

                        reseed_incompressible_prng = TRUE; /* Re-Seed the PRNG to get same sequence back, if random */

                        GetTestFileName(testFileName, params);
                        params->open = WRITECHECK;
                        fd = backend->open(testFileName, params);
                        dataMoved = WriteOrRead(params, results, fd, WRITECHECK, &ioBuffers);
                        backend->close(fd, params);
                        rankOffset = 0;
                }
                /*
                 * read the file(s), getting timing between I/O calls
                 */
                if ((params->readFile || params->checkRead ) && !test_time_elapsed(params, startTime)) {
                        int operation_flag = READ;
                        if ( params->checkRead ){
                          // actually read and then compare the buffer
                          operation_flag = READCHECK;
                        }
                        /* Get rankOffset [file offset] for this process to read, based on -C,-Z,-Q,-X options */
                        /* Constant process offset reading */
                        if (params->reorderTasks) {
                                /* move taskPerNodeOffset nodes[1==default] away from writing node */
                                rankOffset =
                                        (params->taskPerNodeOffset *
                                         params->tasksPerNode) % params->numTasks;
                        }
                        /* random process offset reading */
                        if (params->reorderTasksRandom) {
                                /* this should not intefere with randomOffset within a file because GetOffsetArrayRandom */
                                /* seeds every random() call  */
                                int nodeoffset;
                                unsigned int iseed0;
                                nodeoffset = params->taskPerNodeOffset;
                                nodeoffset = (nodeoffset < params->nodes) ? nodeoffset : params->nodes - 1;
                                if (params->reorderTasksRandomSeed < 0)
                                        iseed0 = -1 * params->reorderTasksRandomSeed + rep;
                                else
                                        iseed0 = params->reorderTasksRandomSeed;
                                srand(rank + iseed0);
                                {
                                        rankOffset = rand() % params->numTasks;
                                }
                                while (rankOffset <
                                       (nodeoffset * params->tasksPerNode)) {
                                        rankOffset = rand() % params->numTasks;
                                }
                                /* Get more detailed stats if requested by verbose level */
                                if (verbose >= VERBOSE_2) {
                                        file_hits_histogram(params);
                                }
                        }
                        if(operation_flag == READCHECK){
                            FillBuffer(ioBuffers.readCheckBuffer, params, 0, (rank + rankOffset) %  params->numTasks);
                        }

                        /* Using globally passed rankOffset, following function generates testFileName to read */
                        GetTestFileName(testFileName, params);

                        if (verbose >= VERBOSE_3) {
                                fprintf(out_logfile, "task %d reading %s\n", rank,
                                        testFileName);
                        }
                        DelaySecs(params->interTestDelay);
                        MPI_CHECK(MPI_Barrier(testComm), "barrier error");
                        params->open = READ;
                        timer[6][rep] = GetTimeStamp();
                        fd = backend->open(testFileName, params);
                        timer[7][rep] = GetTimeStamp();
                        if (params->intraTestBarriers)
                                MPI_CHECK(MPI_Barrier(testComm),
                                          "barrier error");
                        if (rank == 0 && verbose >= VERBOSE_1) {
                                fprintf(out_logfile,
                                        "Commencing read performance test: %s",
                                        CurrentTimeString());
                        }
                        timer[8][rep] = GetTimeStamp();
                        dataMoved = WriteOrRead(params, results, fd, operation_flag, &ioBuffers);
                        timer[9][rep] = GetTimeStamp();
                        if (params->intraTestBarriers)
                                MPI_CHECK(MPI_Barrier(testComm),
                                          "barrier error");
                        timer[10][rep] = GetTimeStamp();
                        backend->close(fd, params);
                        timer[11][rep] = GetTimeStamp();

                        /* get the size of the file just read */
                        results->aggFileSizeFromStat[rep] =
                                backend->get_file_size(params, testComm,
                                                       testFileName);

                        /* check if stat() of file doesn't equal expected file size,
                           use actual amount of byte moved */
                        CheckFileSize(test, dataMoved, rep);

                        if (verbose >= VERBOSE_3)
                                WriteTimes(params, timer, rep, READ);
                        ReduceIterResults(test, timer, rep, READ);
                        if (params->outlierThreshold) {
                                CheckForOutliers(params, timer, rep, READ);
                        }
                }

                if (!params->keepFile
                    && !(params->errorFound && params->keepFileWithError)) {
                        double start, finish;
                        start = GetTimeStamp();
                        MPI_CHECK(MPI_Barrier(testComm), "barrier error");
                        RemoveFile(testFileName, params->filePerProc, params);
                        MPI_CHECK(MPI_Barrier(testComm), "barrier error");
                        finish = GetTimeStamp();
                        PrintRemoveTiming(start, finish, rep);
                } else {
                        MPI_CHECK(MPI_Barrier(testComm), "barrier error");
                }
                params->errorFound = FALSE;
                rankOffset = 0;
        }

        MPI_CHECK(MPI_Comm_free(&testComm), "MPI_Comm_free() error");

        if (params->summary_every_test) {
                PrintLongSummaryHeader();
                PrintLongSummaryOneTest(test);
        } else {
                PrintShortSummary(test);
        }

        XferBuffersFree(&ioBuffers, params);

        if (hog_buf != NULL)
                free(hog_buf);
        for (i = 0; i < 12; i++) {
                free(timer[i]);
        }

        /* Sync with the tasks that did not participate in this test */
        MPI_CHECK(MPI_Barrier(mpi_comm_world), "barrier error");

}

/*
 * Determine if valid tests from parameters.
 */
static void ValidateTests(IOR_param_t * test)
{
        IOR_param_t defaults;
        init_IOR_Param_t(&defaults);

        /* get the version of the tests */
        AioriBind(test->api, test);
        backend->set_version(test);

        if (test->repetitions <= 0)
                WARN_RESET("too few test repetitions",
                           test, &defaults, repetitions);
        if (test->numTasks <= 0)
                ERR("too few tasks for testing");
        if (test->interTestDelay < 0)
                WARN_RESET("inter-test delay must be nonnegative value",
                           test, &defaults, interTestDelay);
        if (test->readFile != TRUE && test->writeFile != TRUE
            && test->checkRead != TRUE && test->checkWrite != TRUE)
                ERR("test must write, read, or check file");
        if (test->segmentCount < 0)
                ERR("segment count must be positive value");
        if ((test->blockSize % sizeof(IOR_size_t)) != 0)
                ERR("block size must be a multiple of access size");
        if (test->blockSize < 0)
                ERR("block size must be non-negative integer");
        if ((test->transferSize % sizeof(IOR_size_t)) != 0)
                ERR("transfer size must be a multiple of access size");
        if (test->setAlignment < 0)
                ERR("alignment must be non-negative integer");
        if (test->transferSize < 0)
                ERR("transfer size must be non-negative integer");
        if (test->transferSize == 0) {
                ERR("test will not complete with zero transfer size");
        } else {
                if ((test->blockSize % test->transferSize) != 0)
                        ERR("block size must be a multiple of transfer size");
        }
        if (test->blockSize < test->transferSize)
                ERR("block size must not be smaller than transfer size");

        /* specific APIs */
        if ((strcmp(test->api, "MPIIO") == 0)
            && ( test->blockSize < sizeof(IOR_size_t)
                || test->transferSize < sizeof(IOR_size_t)))
                ERR("block/transfer size may not be smaller than IOR_size_t for MPIIO");
        if ((strcmp(test->api, "HDF5") == 0)
            && (test->blockSize < sizeof(IOR_size_t)
                || test->transferSize < sizeof(IOR_size_t)))
                ERR("block/transfer size may not be smaller than IOR_size_t for HDF5");
        if ((strcmp(test->api, "NCMPI") == 0)
            && (test->blockSize < sizeof(IOR_size_t)
                || test->transferSize < sizeof(IOR_size_t)))
                ERR("block/transfer size may not be smaller than IOR_size_t for NCMPI");
        if ((test->useFileView == TRUE)
            && (sizeof(MPI_Aint) < 8)   /* used for 64-bit datatypes */
            &&((test->numTasks * test->blockSize) >
               (2 * (IOR_offset_t) GIBIBYTE)))
                ERR("segment size must be < 2GiB");
        if ((strcmp(test->api, "POSIX") != 0) && test->singleXferAttempt)
                WARN_RESET("retry only available in POSIX",
                           test, &defaults, singleXferAttempt);
        if ((strcmp(test->api, "POSIX") != 0) && test->fsync)
                WARN_RESET("fsync() only available in POSIX",
                           test, &defaults, fsync);
        if ((strcmp(test->api, "MPIIO") != 0) && test->preallocate)
                WARN_RESET("preallocation only available in MPIIO",
                           test, &defaults, preallocate);
        if ((strcmp(test->api, "MPIIO") != 0) && test->useFileView)
                WARN_RESET("file view only available in MPIIO",
                           test, &defaults, useFileView);
        if ((strcmp(test->api, "MPIIO") != 0) && test->useSharedFilePointer)
                WARN_RESET("shared file pointer only available in MPIIO",
                           test, &defaults, useSharedFilePointer);
        if ((strcmp(test->api, "MPIIO") == 0) && test->useSharedFilePointer)
                WARN_RESET("shared file pointer not implemented",
                           test, &defaults, useSharedFilePointer);
        if ((strcmp(test->api, "MPIIO") != 0) && test->useStridedDatatype)
                WARN_RESET("strided datatype only available in MPIIO",
                           test, &defaults, useStridedDatatype);
        if ((strcmp(test->api, "MPIIO") == 0) && test->useStridedDatatype)
                WARN_RESET("strided datatype not implemented",
                           test, &defaults, useStridedDatatype);
        if ((strcmp(test->api, "MPIIO") == 0)
            && test->useStridedDatatype && (test->blockSize < sizeof(IOR_size_t)
                                            || test->transferSize <
                                            sizeof(IOR_size_t)))
                ERR("need larger file size for strided datatype in MPIIO");
        if ((strcmp(test->api, "POSIX") == 0) && test->showHints)
                WARN_RESET("hints not available in POSIX",
                           test, &defaults, showHints);
        if ((strcmp(test->api, "POSIX") == 0) && test->collective)
                WARN_RESET("collective not available in POSIX",
                           test, &defaults, collective);

        /* parameter consitency */
        if (test->reorderTasks == TRUE && test->reorderTasksRandom == TRUE)
                ERR("Both Constant and Random task re-ordering specified. Choose one and resubmit");
        if (test->randomOffset && test->reorderTasksRandom
            && test->filePerProc == FALSE)
                ERR("random offset and random reorder tasks specified with single-shared-file. Choose one and resubmit");
        if (test->randomOffset && test->reorderTasks
            && test->filePerProc == FALSE)
                ERR("random offset and constant reorder tasks specified with single-shared-file. Choose one and resubmit");
        if (test->randomOffset && test->checkRead)
                ERR("random offset not available with read check option (use write check)");
        if (test->randomOffset && test->storeFileOffset)
                ERR("random offset not available with store file offset option)");


        if ((strcmp(test->api, "MPIIO") == 0) && test->randomOffset
            && test->collective)
                ERR("random offset not available with collective MPIIO");
        if ((strcmp(test->api, "MPIIO") == 0) && test->randomOffset
            && test->useFileView)
                ERR("random offset not available with MPIIO fileviews");
        if ((strcmp(test->api, "HDF5") == 0) && test->randomOffset)
                ERR("random offset not available with HDF5");
        if ((strcmp(test->api, "NCMPI") == 0) && test->randomOffset)
                ERR("random offset not available with NCMPI");
        if ((strcmp(test->api, "HDF5") != 0) && test->individualDataSets)
                WARN_RESET("individual datasets only available in HDF5",
                           test, &defaults, individualDataSets);
        if ((strcmp(test->api, "HDF5") == 0) && test->individualDataSets)
                WARN_RESET("individual data sets not implemented",
                           test, &defaults, individualDataSets);
        if ((strcmp(test->api, "NCMPI") == 0) && test->filePerProc)
                ERR("file-per-proc not available in current NCMPI");
        if (test->noFill) {
                if (strcmp(test->api, "HDF5") != 0) {
                        ERR("'no fill' option only available in HDF5");
                } else {
                        /* check if hdf5 available */
#if defined (H5_VERS_MAJOR) && defined (H5_VERS_MINOR)
                        /* no-fill option not available until hdf5-1.6.x */
#if (H5_VERS_MAJOR > 0 && H5_VERS_MINOR > 5)
                        ;
#else
                        char errorString[MAX_STR];
                        sprintf(errorString,
                                "'no fill' option not available in %s",
                                test->apiVersion);
                        ERR(errorString);
#endif
#else
                        WARN("unable to determine HDF5 version for 'no fill' usage");
#endif
                }
        }
        if (test->useExistingTestFile && test->lustre_set_striping)
                ERR("Lustre stripe options are incompatible with useExistingTestFile");

        /* N:1 and N:N */
        IOR_offset_t  NtoN = test->filePerProc;
        IOR_offset_t  Nto1 = ! NtoN;
        IOR_offset_t  s    = test->segmentCount;
        IOR_offset_t  t    = test->transferSize;
        IOR_offset_t  b    = test->blockSize;

        if (Nto1 && (s != 1) && (b != t)) {
                ERR("N:1 (strided) requires xfer-size == block-size");
        }
}

static IOR_offset_t *GetOffsetArraySequential(IOR_param_t * test,
                                              int pretendRank)
{
        IOR_offset_t i, j, k = 0;
        IOR_offset_t offsets;
        IOR_offset_t *offsetArray;

        /* count needed offsets */
        offsets = (test->blockSize / test->transferSize) * test->segmentCount;

        /* setup empty array */
        offsetArray =
                (IOR_offset_t *) malloc((offsets + 1) * sizeof(IOR_offset_t));
        if (offsetArray == NULL)
                ERR("malloc() failed");
        offsetArray[offsets] = -1;      /* set last offset with -1 */

        /* fill with offsets */
        for (i = 0; i < test->segmentCount; i++) {
                for (j = 0; j < (test->blockSize / test->transferSize); j++) {
                        offsetArray[k] = j * test->transferSize;
                        if (test->filePerProc) {
                                offsetArray[k] += i * test->blockSize;
                        } else {
                                offsetArray[k] +=
                                        (i * test->numTasks * test->blockSize)
                                        + (pretendRank * test->blockSize);
                        }
                        k++;
                }
        }

        return (offsetArray);
}

static IOR_offset_t *GetOffsetArrayRandom(IOR_param_t * test, int pretendRank,
                                          int access)
{
        int seed;
        IOR_offset_t i, value, tmp;
        IOR_offset_t offsets = 0;
        IOR_offset_t offsetCnt = 0;
        IOR_offset_t fileSize;
        IOR_offset_t *offsetArray;

        /* set up seed for random() */
        if (access == WRITE || access == READ) {
                test->randomSeed = seed = random();
        } else {
                seed = test->randomSeed;
        }
        srandom(seed);

        fileSize = test->blockSize * test->segmentCount;
        if (test->filePerProc == FALSE) {
                fileSize *= test->numTasks;
        }

        /* count needed offsets (pass 1) */
        for (i = 0; i < fileSize; i += test->transferSize) {
                if (test->filePerProc == FALSE) {
                        if ((random() % test->numTasks) == pretendRank) {
                                offsets++;
                        }
                } else {
                        offsets++;
                }
        }

        /* setup empty array */
        offsetArray =
                (IOR_offset_t *) malloc((offsets + 1) * sizeof(IOR_offset_t));
        if (offsetArray == NULL)
                ERR("malloc() failed");
        offsetArray[offsets] = -1;      /* set last offset with -1 */

        if (test->filePerProc) {
                /* fill array */
                for (i = 0; i < offsets; i++) {
                        offsetArray[i] = i * test->transferSize;
                }
        } else {
                /* fill with offsets (pass 2) */
                srandom(seed);  /* need same seed */
                for (i = 0; i < fileSize; i += test->transferSize) {
                        if ((random() % test->numTasks) == pretendRank) {
                                offsetArray[offsetCnt] = i;
                                offsetCnt++;
                        }
                }
        }
        /* reorder array */
        for (i = 0; i < offsets; i++) {
                value = random() % offsets;
                tmp = offsetArray[value];
                offsetArray[value] = offsetArray[i];
                offsetArray[i] = tmp;
        }
        SeedRandGen(test->testComm);    /* synchronize seeds across tasks */

        return (offsetArray);
}

static IOR_offset_t WriteOrReadSingle(IOR_offset_t pairCnt, IOR_offset_t *offsetArray, int pretendRank,
  IOR_offset_t * transferCount, int * errors, IOR_param_t * test, int * fd, IOR_io_buffers* ioBuffers, int access){
  IOR_offset_t amtXferred = 0;
  IOR_offset_t transfer;

  void *buffer = ioBuffers->buffer;
  void *checkBuffer = ioBuffers->checkBuffer;
  void *readCheckBuffer = ioBuffers->readCheckBuffer;

  test->offset = offsetArray[pairCnt];

  transfer = test->transferSize;
  if (access == WRITE) {
          /*
           * fills each transfer with a unique pattern
           * containing the offset into the file
           */
          if (test->storeFileOffset == TRUE) {
                  FillBuffer(buffer, test, test->offset, pretendRank);
          }
          amtXferred =
                  backend->xfer(access, fd, buffer, transfer, test);
          if (amtXferred != transfer)
                  ERR("cannot write to file");
  } else if (access == READ) {
          amtXferred =
                  backend->xfer(access, fd, buffer, transfer, test);
          if (amtXferred != transfer)
                  ERR("cannot read from file");
  } else if (access == WRITECHECK) {
          memset(checkBuffer, 'a', transfer);
          amtXferred =
                  backend->xfer(access, fd, checkBuffer, transfer,
                                test);
          if (amtXferred != transfer)
                  ERR("cannot read from file write check");
          (*transferCount)++;
          *errors += CompareBuffers(buffer, checkBuffer, transfer,
                                   *transferCount, test,
                                   WRITECHECK);
  } else if (access == READCHECK) {
          amtXferred = backend->xfer(access, fd, buffer, transfer, test);
          if (amtXferred != transfer){
            ERR("cannot read from file");
          }
          if (test->storeFileOffset == TRUE) {
                  FillBuffer(readCheckBuffer, test, test->offset, pretendRank);
          }
          *errors += CompareBuffers(readCheckBuffer, buffer, transfer, *transferCount, test, READCHECK);
  }
  return amtXferred;
}

/*
 * Write or Read data to file(s).  This loops through the strides, writing
 * out the data to each block in transfer sizes, until the remainder left is 0.
 */
static IOR_offset_t WriteOrRead(IOR_param_t * test, IOR_results_t * results, void *fd, int access, IOR_io_buffers* ioBuffers)
{
        int errors = 0;
        IOR_offset_t amtXferred;
        IOR_offset_t transferCount = 0;
        uint64_t pairCnt = 0;
        IOR_offset_t *offsetArray;
        int pretendRank;
        IOR_offset_t dataMoved = 0;     /* for data rate calculation */
        double startForStonewall;
        int hitStonewall;

        /* initialize values */
        pretendRank = (rank + rankOffset) % test->numTasks;

        if (test->randomOffset) {
                offsetArray = GetOffsetArrayRandom(test, pretendRank, access);
        } else {
                offsetArray = GetOffsetArraySequential(test, pretendRank);
        }


        /* check for stonewall */
        startForStonewall = GetTimeStamp();
        hitStonewall = ((test->deadlineForStonewalling != 0)
                        && ((GetTimeStamp() - startForStonewall)
                            > test->deadlineForStonewalling));

        /* loop over offsets to access */
        while ((offsetArray[pairCnt] != -1) && !hitStonewall ) {
                dataMoved += WriteOrReadSingle(pairCnt, offsetArray, pretendRank, & transferCount, & errors, test, fd, ioBuffers, access);
                pairCnt++;

                hitStonewall = ((test->deadlineForStonewalling != 0)
                                && ((GetTimeStamp() - startForStonewall)
                                    > test->deadlineForStonewalling)) || (test->stoneWallingWearOutIterations != 0 && pairCnt == test->stoneWallingWearOutIterations) ;
        }
        if (test->stoneWallingWearOut){
          if (verbose >= VERBOSE_1){
            fprintf(out_logfile, "%d: stonewalling pairs accessed: %lld\n", rank, (long long) pairCnt);
          }
          long long data_moved_ll = (long long) dataMoved;
          long long pairs_accessed_min = 0;
          MPI_CHECK(MPI_Allreduce(& pairCnt, &results->pairs_accessed,
                                  1, MPI_LONG_LONG_INT, MPI_MAX, testComm), "cannot reduce pairs moved");
          double stonewall_runtime = GetTimeStamp() - startForStonewall;
          results->stonewall_time = stonewall_runtime;
          MPI_CHECK(MPI_Reduce(& pairCnt, & pairs_accessed_min,
                                  1, MPI_LONG_LONG_INT, MPI_MIN, 0, testComm), "cannot reduce pairs moved");
          MPI_CHECK(MPI_Reduce(& data_moved_ll, & results->stonewall_min_data_accessed,
                                  1, MPI_LONG_LONG_INT, MPI_MIN, 0, testComm), "cannot reduce pairs moved");
          MPI_CHECK(MPI_Reduce(& data_moved_ll, & results->stonewall_avg_data_accessed,
                                  1, MPI_LONG_LONG_INT, MPI_SUM, 0, testComm), "cannot reduce pairs moved");

          if(rank == 0){
            fprintf(out_logfile, "stonewalling pairs accessed min: %lld max: %zu -- min data: %.1f GiB mean data: %.1f GiB time: %.1fs\n",
             pairs_accessed_min, results->pairs_accessed,
             results->stonewall_min_data_accessed /1024.0 / 1024 / 1024,  results->stonewall_avg_data_accessed / 1024.0 / 1024 / 1024 / test->numTasks , results->stonewall_time);
             results->stonewall_min_data_accessed *= test->numTasks;
          }
          if(pairs_accessed_min == pairCnt){
            results->stonewall_min_data_accessed = 0;
            results->stonewall_avg_data_accessed = 0;
          }
          if(pairCnt != results->pairs_accessed){
            // some work needs still to be done !
            for(; pairCnt < results->pairs_accessed; pairCnt++ ) {
                    dataMoved += WriteOrReadSingle(pairCnt, offsetArray, pretendRank, & transferCount, & errors, test, fd, ioBuffers, access);
            }
          }
        }else{
          results->pairs_accessed = pairCnt;
        }


        totalErrorCount += CountErrors(test, access, errors);

        free(offsetArray);

        if (access == WRITE && test->fsync == TRUE) {
                backend->fsync(fd, test);       /*fsync after all accesses */
        }
        return (dataMoved);
}

/*
 * Write times taken during each iteration of the test.
 */
static void
WriteTimes(IOR_param_t * test, double **timer, int iteration, int writeOrRead)
{
        char accessType[MAX_STR];
        char timerName[MAX_STR];
        int i, start = 0, stop = 0;

        if (writeOrRead == WRITE) {
                start = 0;
                stop = 6;
                strcpy(accessType, "WRITE");
        } else if (writeOrRead == READ) {
                start = 6;
                stop = 12;
                strcpy(accessType, "READ");
        } else {
                ERR("incorrect WRITE/READ option");
        }

        for (i = start; i < stop; i++) {
                switch (i) {
                case 0:
                        strcpy(timerName, "write open start");
                        break;
                case 1:
                        strcpy(timerName, "write open stop");
                        break;
                case 2:
                        strcpy(timerName, "write start");
                        break;
                case 3:
                        strcpy(timerName, "write stop");
                        break;
                case 4:
                        strcpy(timerName, "write close start");
                        break;
                case 5:
                        strcpy(timerName, "write close stop");
                        break;
                case 6:
                        strcpy(timerName, "read open start");
                        break;
                case 7:
                        strcpy(timerName, "read open stop");
                        break;
                case 8:
                        strcpy(timerName, "read start");
                        break;
                case 9:
                        strcpy(timerName, "read stop");
                        break;
                case 10:
                        strcpy(timerName, "read close start");
                        break;
                case 11:
                        strcpy(timerName, "read close stop");
                        break;
                default:
                        strcpy(timerName, "invalid timer");
                        break;
                }
                fprintf(out_logfile, "Test %d: Iter=%d, Task=%d, Time=%f, %s\n",
                        test->id, iteration, (int)rank, timer[i][iteration],
                        timerName);
        }
}
