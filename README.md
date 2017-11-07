# IO500-benchmark (C-Version)
The C version for the IO500 benchmark (in incubator)

## Usage

* git clone https://github.com/VI4IO/io500-app.git
* download two required repositories
  $ ./prepare.sh
  # hopefully that built libcircle and configured ior
* build io500
  $ ./compile.sh (feel free to adjust the trivial build script)
  # run
  $ mpiexec ... ./io500 -w <WORKING DIR> -r <RESULTS-dir>
  # help:
  $ ./io500 --help
* For a first test run add the options "-s 1" which performs stonewalling with one second.
  $ ./io500 -s 1 -w /tmp/testdir -r results-dir
