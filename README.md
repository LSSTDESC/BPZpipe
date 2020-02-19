DESC BPZpipe Pipeline Stages
------------------------------------------------

Goals
-----
Run BPZ_py3 as a DESC pipeline stage

Installation
------------

Requires python3, ceci, parsl, DESCFormats, and BPZ_py3 

Tested using the python/3.6-anaconda-4.4 module on cori
NOTE: using the desc-python setup currently causes errors, possibly due
to updated parsl version (0.7.2 vs 0.5.2)

This pipeline assumes that you are using the python3 version of BPZ that
reads in via hdf5 files, still currently under development in the
bpzpy_whdf5 branch of pz_pdf:
https://github.com/LSSTDESC/pz_pdf/tree/u/sschmidt23/bpzpy_whdf5


Running the pipeline
--------------------

You can run:

```bash
ceci test/test.yml
```
to run a small example hd5 file containing 1000 galaxies with mock 10 year 
errors from cosmoDC2v1.1.4_small.  The names of the input and output files, 
the BPZ parameter file, and an example INTERP keyword, are specified in 
test/config.yml

If you are running on cori at NERSC you can run the test batch script
```bash
sbatch runbpzpipe_batch.sh
```
to run the same 1000 galaxy test sample, and shows the necessary format
for running in the NERSC environment.

Notes
-----

Personnel
---------
Sam Schmidt