#!/bin/bash
#SBATCH --ntasks-per-node=10
#SBATCH --nodes=100
#SBATCH --job-name=IO-500
#SBATCH --time=07:50:00
#SBATCH -o io_500_out_%J
#SBATCH -e io_500_err_%J
#SBATCH --dependency=singleton

# Load necessary packages
module load intel mxm/3.4.3082 fca/2.5.2431 bullxmpi_mlx/bullxmpi_mlx-1.2.9.2 cmake/3.2.3 gcc/7.1.0


dir=/home/dkrz/k202079/work/io-500/io-500-dev/utilities/io500-app/

# Name for the output + result directory, timestamped with NNODES + NPROC
stamp=$(date | sed "s/ //g" | sed "s/:/-/g")
workdir=/mnt/lustre02/work/k20200/k202079/io500/data
resdir=$dir/res-${SLURM_NNODES}-${SLURM_NPROCS}-$stamp

# Cleanup the files from the directory with a parallel cleanup

srun --propagate=STACK $dir/io500 -w $workdir -C 2>/dev/null

# Remove the directory, otherwise: when running with more procs first, then directories from mdtest remain that produce an error
rm -rf $workdir


# precreate directories for lustre with the appropriate striping
mkdir -p ${workdir}/ior_easy
lfs setstripe --stripe-count 2  ${workdir}/ior_easy

mkdir -p ${workdir}/ior_hard
lfs setstripe --stripe-count 200  ${workdir}/ior_hard

# ulimit -s 102400


echo "Now running IO500"
# Now run, provide upper limits of the number of files and file size
srun --propagate=STACK $dir/io500 -w $workdir -r $resdir -s 300 -S -v -f 20000 -F 20000 -I 15000 -e "-F -t 1m -b 128g" | tee $resdir/io500-summary.txt

