bin_PROGRAMS = ior mdtest
if USE_CAPS
bin_PROGRAMS += IOR MDTEST
endif

noinst_HEADERS = ior.h utilities.h parse_options.h aiori.h iordef.h ../getopt/optlist.h

extraSOURCES = aiori.c
extraLDADD =
extraLDFLAGS =
extraCPPFLAGS =

ior_SOURCES = ior.c utilities.c parse_options.c ../getopt/optlist.c
ior_LDFLAGS =
ior_LDADD =
ior_CPPFLAGS = -I../

mdtest_SOURCES = mdtest.c utilities.c ../getopt/optlist.c
mdtest_LDFLAGS =
mdtest_LDADD =
mdtest_CPPFLAGS =  -I../

if USE_HDFS_AIORI
# TBD: figure out how to find the appropriate -I and -L dirs.  Maybe we can
#      get them from the corresponding bin/ dir in $PATH, or pick an
#      environment var to use (and set it in modulefiles), or provide a
#      config-flag, to set a variable we use here.
extraSOURCES  += aiori-HDFS.c
extraCPPFLAGS += -I/opt/hadoop-2.2.0/include
extraLDFLAGS  += -L/opt/hadoop-2.2.0/lib/native
extraLDADD    += -lhdfs
endif

if USE_HDF5_AIORI
extraSOURCES  += aiori-HDF5.c
extraLDADD    += -lhdf5 -lz
endif

if USE_MPIIO_AIORI
extraSOURCES += aiori-MPIIO.c
endif

if USE_NCMPI_AIORI
extraSOURCES += aiori-NCMPI.c
extraLDADD   += -lpnetcdf
endif

if USE_POSIX_AIORI
extraSOURCES += aiori-POSIX.c
endif


if USE_S3_AIORI
extraSOURCES  += aiori-S3.c
if AWS4C_DIR
  extraCPPFLAGS += $(AWS4C_CPPFLAGS)
  extraLDFLAGS  += $(AWS4C_LDFLAGS)
endif
extraLDADD    += -lcurl
extraLDADD    += -lxml2
extraLDADD    += -laws4c
extraLDADD    += -laws4c_extra
endif


ior_SOURCES += $(extraSOURCES)
ior_LDFLAGS += $(extraLDFLAGS)
ior_LDADD += $(extraLDADD)
ior_CPPFLAGS += $(extraCPPFLAGS)

mdtest_SOURCES += $(extraSOURCES)
mdtest_LDFLAGS += $(extraLDFLAGS)
mdtest_LDADD += $(extraLDADD)
mdtest_CPPFLAGS += $(extraCPPFLAGS)

IOR_SOURCES  = $(ior_SOURCES)
IOR_LDADD    = $(ior_LDADD)
IOT_CPPFLAGS = $(ior_CPPFLAGS)

MDTEST_SOURCES  = $(mdtest_SOURCES)
MDTEST_LDADD    = $(mdtest_LDADD)
MDTEST_CPPFLAGS = $(mdtest_CPPFLAGS)
