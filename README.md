DESC BPZpipe Pipeline Stages
------------------------------------------------

Goals
-----
Run BPZ_py3 as a DESC pipeline stage

Installation
------------

Requires python3, ceci, DESCFormats, BPZ_py3 and ...

Tested using the python/3.6-anaconda-4.4 module on cori
NOTE: using the desc-python setup currently causes errors, possibly due
to updated parsl version (0.7.2 vs 0.5.2)

Add this directory to your PYTHONPATH variable to use it in other pipelines.

Cori
----

You can set up the environment which has these dependencies on cori:
```bash
source ./test/cori.yml

```

Running the pipeline
--------------------

You can run:

```bash
ceci test/test.yml
```
to run the implemented stages

Notes
-----

Personnel
---------
Sam Schmidt