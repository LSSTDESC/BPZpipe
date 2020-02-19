#!/bin/bash
#SBATCH --qos=debug
#SBATCH --time=00:09:59
#SBATCH --nodes=1
#SBATCH --constraint=haswell
#SBATCH --error="bpzpipe_test1000.err"
#SBATCH --output="bpzpipe_test1000.out"

module load python/3.6-anaconda-4.4
source activate /global/homes/s/schmidt9/CONDA/BPZPIPE/conda-bpz-new
module swap PrgEnv-intel PrgEnv-gnu
module load PrgEnv-gnu
module unload darshan
module load h5py-parallel
module load cfitsio/3.47
module load gsl/2.5

export CECI_SETUP="/global/projecta/projectdirs/lsst/groups/PZ/BPZ/BPZpipe/test/setup-cori-update"
export HDF5_USE_FILE_LOCKING=FALSE
export PYTHONPATH=$PYTHONPATH:/global/projecta/projectdirs/lsst/groups/PZ/BPZ/BPZpipe:/global/homes/s/schmidt9/DESC/software/descformats/DESCFormats/descformats:/global/homes/s/schmidt9/DESC/software/ceci/ceci

srun -n 6 python3 -m bpzpipe BPZ_pz_pdf  --photometry_catalog=./test1000_h5pyfmt_new.h5 --config=./config.yml --photoz_pdfs=./outputs/test1000_outputfile.hdf5 --mpi
