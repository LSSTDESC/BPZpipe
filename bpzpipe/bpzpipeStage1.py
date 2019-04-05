from ceci import PipelineStage
from descformats import TextFile, HDFFile, YamlFile
import subprocess
import os

# This class runs the python3 version of BPZ from the command line
class BPZpipeStage1(PipelineStage):
    """Pipeline stage to run the BPZpy3 version of BPZ 
       The inputs and outputs are named in the associated yml file used by ceci
    inputs:
    -------
    input_photometry_file: hdf5 file 
      file containing the ugrizy photometry and errors
    configuration options:
    ----------------------
    param_file: ascii file containing the BPZ parameters other than INTERP, infile, and outfiles.  
      Many of the parameters will  be the same run to run, so we can use a default file for many 
      healpix pixels' of data.
    INTERP: integer (though read in as a strint): the number of interpolated SEDs to be created
      in between the grid.  The default value is 0.
      path_to_bpz: string
    path_to_BPZ: string
      The path to the installed BPZpy3 version, will be set as the $BPZPY3PATH env. variable
    outputs:
    --------
      photoz_pointfile: ascii file usually ending in ".bpz" that contains the point estimate data 
      created by BPZ
      photoz_probsfile: ascii file usually ending in "_probs.out" containing the p(z) for each object
      in the catalog
    -----
    For now, the code assumes that this is being run on cori and using the version of BPZ installed 
    there, you can specify a different installation via the "path_to_bpz" config option.
    """
    name = "BPZpipeStage1"
    #
    inputs = [
        ('input_photometry_file', HDFFile),
    ]
    outputs = [
        ('photoz_pointfile', TextFile),
        ('photoz_probsfile', TextFile),
        # More inputs can go here
    ]
    config_options = {
        'INTERP': '0',  #Interpolate between input templates N times
        'param_file': "test.pars",
        'path_to_bpz': "/global/projecta/projectdirs/lsst/groups/PZ/BPZ/BPZpy3/pz_pdf/pz/BPZ"
        }

    def run(self):
        outfile_point = self.get_output('photoz_pointfile')
        outfile_probs= self.get_output('photoz_probsfile')
        interp = self.config['INTERP']
        pfile = self.config['param_file']
        infile = self.get_input('input_photometry_file')
        bpz_path = self.config['path_to_bpz']
        #BPZ uses paths relevant to an environment variable, set this
        os.environ["BPZPY3PATH"]=bpz_path
        #BPZ is so old that it has some leftover references to NUMERIX
        os.environ["NUMERIX"]="numpy"
        print ("Running BPZ...")
        #Set up the command line command to run bpz_py3_hdf5.  The format is
        #bpz_py3_hdf5.py [infile] -P [parsfile] [specific BPZ keywords not in pars file, e.g. -INTERP]
        args = ['python3',
                '/global/projecta/projectdirs/lsst/groups/PZ/BPZ/BPZpy3/pz_pdf/pz/BPZ/bpz_py3_hdf5.py',
                infile,
                '-P',pfile,'-OUTPUT',
                outfile_point,'-PROBS_LITE',outfile_probs]
        subprocess.Popen(args)
        
        # You would normally call some other function or method
        # here to generate some output.  You can use self.comm, 
        # self.rank, and self.size to use MPI.
        print ("finished")
       


