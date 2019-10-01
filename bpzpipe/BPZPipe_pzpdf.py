from ceci import PipelineStage
from descformats import TextFile, HDFFile, YamlFile
#from txpipe.data_types import PhotozPDFFile
import os
import sys
import numpy as np
import time

# This class runs the python3 version of BPZ from the command line
class BPZ_pz_pdf(PipelineStage):
    """Pipeline stage to run the BPZpy3 version of BPZ, directly from python

    """
    name = "BPZ_pz_pdf"
    #
    inputs = [
        ('photometry_catalog', HDFFile),
    ]
    outputs = [
#        ('photoz_pdfs', PhotozPDFFile),
        ('photoz_pdfs', HDFFile),

    ]
    config_options = {
        "path_to_bpz": str,
        "metacal_fluxes": False, #if true, sets suffices to metacal variants   
        "dz":   0.01,
        "zmin": 0.005,
        "zmax": 3.505,
        "interp": 0,
        "prior_file": 'dc2v4_PCA_cosmodc2v114_py3',
        "spectra_file": 'SED/dc2_PCAsortedtemplates_v4.list',
        "columns_file": 'test/CSDC2114_test.columns',
        "ab_dir" : "AB",
        "bands" : "ugrizy",
        "zp_errors": [0.01, 0.01, 0.01, 0.01, 0.01, 0.01],
        "prior_band": 'i',
        "p_min": 0.01,
        "sigma_g": 0.03,  # use sigma_g <= 0 for no convolution
        "chunk_rows": 100,
        "mag_err_min": 1e-3,
        "sigma_intrins": 0.05, #"intrinsic" assumed scatter, used in ODDS
        "odds_int": 0.99445, #number of sigma_intrins to integrate +/- around peak
        # note that 1.95993 is the number of sigma you get for old "ODDS" =0.95 
        #in old BPZ, 0.68 is 0.99445
        #"point_estimate": "mode",  # mean, mode, or median
    }

    def run(self):
        
        starttime = time.time()
        self.setup_bpz()


        # Load the template BPZ will need from the BPZ sub-directories
        z, flux_templates = self.load_templates()

        # Columns we will need from the data
        # Note that we need all the metacalibrated variants too.
        metacal_fluxes = self.config['metacal_fluxes']
        if metacal_fluxes == False:
            suffices = [""]
        else:
            suffices = ["", "_1p", "_1m", "_2p", "_2m"]
        self.suffices = suffices 
        bands = self.config['bands']
        cols =  [f'mag_{band}_lsst{suffix}' for band in bands for suffix in suffices] 
        # We only have one set of errors, though
        cols += [f'mag_err_{band}_lsst' for band in bands]
        cols += ["id"]
        
        # Prepare the output HDF5 file
        output_file = self.prepare_output(z)

        # Amount of data to load at once
        chunk_rows = self.config['chunk_rows']
        # Loop through chunks of the data.
        # Parallelism is handled in the iterate_input function - 
        # each processor will only be given the sub-set of data it is 
        # responsible for.  The HDF5 parallel output mode means they can
        # all write to the file at once too.
        for start, end, data in self.iterate_hdf('photometry_catalog', "photometry", cols, chunk_rows):
            print(f"Process {self.rank} running photo-z for rows {start}-{end}")

            # Calculate the pseudo-fluxes that we need
            self.preprocess_magnitudes(data)

            # Actually run BPZ
            point_estimates, pdfs = self.estimate_pdfs(flux_templates, z, data)

            # Save this chunk of data
            self.write_output(output_file, start, end, pdfs, point_estimates)

        # Synchronize processors
        if self.is_mpi():
            self.comm.Barrier()
        endtime = time.time()
        print(f"finished, took {endtime - starttime} seconds")

    def setup_bpz(self):
        bpz_path = self.config['path_to_bpz']

        #BPZ uses paths relevant to an environment variable, set this
        os.environ["BPZPY3PATH"] = bpz_path

        #BPZ is so old that it has some leftover references to NUMERIX
        os.environ["NUMERIX"]="numpy"

        #need to set the env. variable to set up cori for mpi
        os.environ["CECI_SETUP"]="/global/projecta/projectdirs/lsst/groups/PZ/BPZ/BPZpipe/test/setup-cori-update"
        os.environ["HDF5_USE_FILE_LOCKING"]="FALSE"
        

        # We will import from BPZ in a moment.
        sys.path.append(bpz_path)



    def prepare_output(self, z):
        """
        Prepare the output HDF5 file for writing.

        Note that this is done by all the processes if running in parallel;
        that is part of the design of HDF5.
    
        Parameters
        ----------

        nobj: int
            Number of objects in the catalog

        z: array
            Points on the redshift axis that the PDF will be evaluated at.

        Returns
        -------
        f: h5py.File object
            The output file, opened for writing.

        """

        # Work out how much space we will need.
        cat = self.open_input("photometry_catalog")
        ids = np.array(cat['photometry/id'])
        nobj = ids.size
        cat.close()

        # Open the output file.
        # This will automatically open using the HDF5 mpi-io driver 
        # if we are running under MPI and the output type is parallel
        f = self.open_output('photoz_pdfs', parallel=True)

        # Create the space for output data
        nz = len(z)
        groupid = f.create_group('id')
        groupid.create_dataset('galaxy_id', (nobj,), dtype = 'i8')
        grouppt = f.create_group('point_estimates')
        grouppt.create_dataset('z_mode', (nobj,), dtype='f4')
        grouppt.create_dataset('z_mean', (nobj,), dtype='f4')
        grouppt.create_dataset('z_median', (nobj,), dtype='f4')
        grouppt.create_dataset('ODDS', (nobj,), dtype = 'f4')
        grouppt.create_dataset('z_mode_ml', (nobj,), dtype= 'f4')
        grouppt.create_dataset('z_mode_ml_red_chi2', (nobj,), dtype='f4')
        group = f.create_group('pdf')
        group.create_dataset("zgrid", (nz,), dtype='f4')
        group.create_dataset("pdf", (nobj,nz), dtype='f4')

        # One processor writes the redshift axis to output.
        if self.rank==0:
            group['zgrid'][:] = z
            groupid['galaxy_id'][:] = ids
        return f

    def load_templates(self):
        from useful_py3 import get_str, get_data, match_resol


        # The redshift range we will evaluate on
        zmin = self.config['zmin']
        zmax = self.config['zmax']
        dz = self.config['dz']
        z = np.arange(zmin, zmax + dz, dz)


        bpz_path = self.config['path_to_bpz']
        columns_file = self.config['columns_file']
        ignore_rows =['M_0','OTHER','ID','Z_S']
        filters = [f for f in get_str(columns_file, 0) if f not in ignore_rows]

        m0_index = 'ugrizy'.index('i')
        spectra_file = os.path.join(bpz_path, self.config['spectra_file'])
        spectra = [s[:-4] for s in get_str(spectra_file)]
        ab_dir = os.path.join(bpz_path, self.config['ab_dir'])

        nt = len(spectra)
        nf = len(filters)
        nz = len(z)
        flux_templates = np.zeros((nz,nt,nf))

        for i,s in enumerate(spectra):
            for j,f in enumerate(filters):
                model = f"{s}.{f}.AB"
                model_path=os.path.join(ab_dir, model)
                zo, f_mod_0 = get_data(model_path,(0,1))
                flux_templates[:,i,j] = match_resol(zo, f_mod_0, z)

        return z, flux_templates

    def preprocess_magnitudes(self, data):
        from bpz_tools_py3 import mag2flux, e_mag2frac

        bands = self.config['bands']
        ##suffices = ["", "_1p", "_1m", "_2p", "_2m"]
        #suffices = [""]
        
        # Load the magnitudes
        zp_errors = np.array(self.config['zp_errors'])
        zp_frac=e_mag2frac(zp_errors)

        # Only one set of mag errors
        mag_errs = np.array([data[f'mag_err_{b}_lsst'] for b in bands]).T

        # But many sets of mags, for now
        for suffix in self.suffices:
            # Group the magnitudes and errors into one big array
            mags = np.array([data[f'mag_{b}_lsst{suffix}'] for b in bands]).T

            # Clip to min mag errors
            np.clip(mag_errs, self.config['mag_err_min'], 1e10, mag_errs)

            # Convert to pseudo-fluxes
            flux = 10.0**(-0.4*mags)
            flux_err = flux * (10.0**(0.4*mag_errs) - 1.0)

            # Check if an object is seen in each band at all.
            # Fluxes not seen at all are listed as infinity in the input,
            # so will come out as zero flux and zero flux_err.
            # Check which is which here, to use with the ZP errors below
            seen1 = (flux > 0) & (flux_err > 0)
            seen = np.where(seen1)
            #unseen = np.where(~seen1)
            #replace Joe's definition with more standard BPZ style
            nondetect = 99.
            nondetflux = 10.**(-0.4*nondetect)
            unseen = np.isclose(flux,nondetflux,atol=nondetflux*0.5)

            #replace mag = 99 values with 0 flux and 1 sigma limiting magnitude
            #value, which is stored in the mag_errs column for non-detects
            #NOTE: We should check that this same convention will be used in
            #LSST, or change how we handle non-detects here!
            flux[unseen] = 0.
            flux_err[unseen]= 10.**(-0.4*np.abs(mag_errs[unseen]))

            # Add zero point magnitude errors.
            # In the case that the object is detected, this
            # correction depends onthe flux.  If it is not detected
            # then BPZ uses half the errors instead
            add_err = np.zeros_like(flux_err)
            add_err[seen] = ((zp_frac*flux)**2)[seen]
            add_err[unseen] = ((zp_frac*0.5*flux_err)**2)[unseen]
            flux_err = np.sqrt(flux_err**2 + add_err)

            # Upate the input dictionary with new things we have calculated
            data[f'flux{suffix}'] = flux
            data[f'flux_err{suffix}'] = flux_err
            data[f'mags{suffix}'] = mags

    def estimate_pdfs(self, flux_templates, z, data):

        
        # BPZ uses the magnitude in one band to get a prior.
        # Select which one here.
        bands = self.config['bands']
        prior_band = self.config['prior_band']
        m_0_col = bands.index(prior_band)

        # Size of output
        ng = len(data['mags'])
        nz = len(z)
        

        # Optionally, BPZ can convolve the PDF with a Gaussian.
        # This consolidates multiple nearby peaks into one, which is
        # useful for estimating the MAP point, among other things.
        sigma_g = self.config['sigma_g']

        # Prepare said Gaussian
        if sigma_g > 0:
            dz = self.config['dz']
            x = np.arange(-3.*sigma_g, 3.*sigma_g + dz/10., dz)
            kernel = np.exp(-(x/sigma_g)**2)
        else:
            kernel = None


        # Space for the output
        pdfs = np.zeros((ng, nz))
        point_estimates = np.zeros((6, ng))
#        point_estimator = self.config['point_estimate']

        # Metacal variants
        ##suffices = ["", "_1p", "_1m", "_2p", "_2m"]
        #suffices = [""]
        for s, suffix in enumerate(self.suffices):
            for i in range(ng):
                # Pull out the rows of data for this galaxy
                mag_0 = data[f'mags{suffix}'][i, m_0_col]
                flux  = data[f'flux{suffix}'][i]
                flux_err = data[f'flux_err{suffix}'][i]

                # and compute the PDF for it
                pdf,zb_ml,red_chi_ml = self.estimate_pdf(flux_templates,
                                                         kernel, 
                                                         flux, flux_err,
                                                         mag_0, z)
                
                if suffix=="":
                    pdfs[i] = pdf
                
                    #Remove selector and compute all three point est.
                    #variants to store
                    #if point_estimator == 'mean':
                    # The pdf already sums to unity, so this gives us the mean
                    point_estimates[0, i] = (pdf * z).sum()
                    #elif point_estimator == 'mode':
                    # This is the BPZ default
                    point_estimates[1, i] = z[np.argmax(pdf)]
                    #elif point_estimator == 'median':
                    # Just for completeness, not idea if sensible. Defined\
                    #below
                    point_estimates[2, i] = pdf_median(z, pdf)
                    #else:
                    #    raise ValueError(f"Unknown value for point_estimate\
                    #parameter"
                    #        f"'{point_estimator}' - should be 'mean', 'mode'\,
                    #or 'median'")

                    point_estimates[3,i] = self.calculate_odds(z,
                                                               point_estimates[1,i],
                                                               pdf)
                    #tack on the max likelihood point redshift (pre-prior) \
                    #and reduced chi^2
                    point_estimates[4,i] = zb_ml
                    point_estimates[5,i] = red_chi_ml
                
        # Return full set
        return point_estimates, pdfs

    def calculate_odds(self, z, zb, pdf):
        """
        calculates the integrated pdf between -N*sigma_intrins and +N*sigma_intrins
        around the mode of the PDF, zb
        parameters: 
          -sigma_intrins: intrinsic scatter of distn, read in from config 
          -odds_int: number of sigma_intrins to multiply by to define interval
          (read in from config)
          -z : redshift grid of pdf
          -pdf: posterior redshift estimate
          -zb: mode of posterior
        """
        cumpdf = np.cumsum(pdf)
        if np.isclose(cumpdf[-1],0.0):
            return 0.0
        else:
            zo1 = zb - self.config['sigma_intrins']*self.config['odds_int']*(1.+zb)
            zo2 = zb + self.config['sigma_intrins']*self.config['odds_int']*(1.+zb)
            i1 = np.searchsorted(z,zo1)-1
            i2 = np.searchsorted(z,zo2)
            if i1<0:
                return cumpdf[i2]/cumpdf[-1]
            if i2>len(z)-1:
                return 1. - cumpdf[i1]/cumpdf[-1]
            return(cumpdf[i2]-cumpdf[i1])/cumpdf[-1]


    def estimate_pdf(self, flux_templates, kernel, flux, flux_err, mag_0, z):
        from bpz_tools_py3 import p_c_z_t, prior

        # Various options
        prior_file = self.config['prior_file']
        ninterp = self.config['interp']
        p_min = self.config['p_min']

        # The number of templates is needed by the prior code
        nt = flux_templates.shape[1]

        # The likelihood and prior...
        #modify to add back in chisq calc
        #L = p_c_z_t(flux, flux_err, flux_templates).likelihood
        pczt = p_c_z_t(flux, flux_err, flux_templates)
        L = pczt.likelihood
        zb_ml = pczt.i_z_ml
        reduced_chi_ml = pczt.min_chi2/float(flux_templates.shape[2]-1.)

        P = prior(z, mag_0, prior_file, nt, ninterp=ninterp)

        # Time for everyone's favourite Theorem!
        post = L * P
        

        
        # Right now we jave the joint PDF of p(z,template). Marginalize
        # over the templates to just get p(z)
        post_z = post.sum(axis=1)

        # Convolve with Gaussian kernel, if present        
        if kernel is not None:
            post_z = np.convolve(post_z, kernel, 1)
            
        # Disconcertingly, BPZ seems to cut off any probabilities
        # below a certain threshold (default 0.01)
        p_max = post_z.max()
        post_z[post_z < (p_max * p_min)] = 0

        # Normalize in the same way that BPZ does
        #But, only normalize if the elements don't sum to zero
        #if they are all zero, just leave p(z) as all zeros, as no templates
        #are a good fit.
        if not np.isclose(post_z.sum(),0.0):
            post_z /= post_z.sum()
        
        # And all done
        return post_z,zb_ml,reduced_chi_ml


    def write_output(self, output_file, start, end, pdfs, point_estimates):
        """
        Write out a chunk of the computed PZ data.

        Parameters
        ----------

        output_file: h5py.File
            The object we are writing out to

        start: int
            The index into the full range of data that this chunk starts at

        end: int
            The index into the full range of data that this chunk ends at

        pdfs: array of shape (n_chunk, n_z)
            The output PDF values

        point_estimates: array of shape (3, n_chunk)
            Point-estimated photo-zs for each of the 5 metacalibrated variants

        """
        group = output_file['pdf']
        group['pdf'][start:end] = pdfs
        grouppt = output_file['point_estimates']
        grouppt['z_mean'][start:end] = point_estimates[0]
        grouppt['z_mode'][start:end] = point_estimates[1]
        grouppt['z_median'][start:end] = point_estimates[2]
        grouppt['ODDS'][start:end] = point_estimates[3]
        grouppt['z_mode_ml'][start:end] = point_estimates[4]
        grouppt['z_mode_ml_red_chi2'][start:end] = point_estimates[5]


def pdf_median(z, p):
    psum = p.sum()
    cdf = np.cumsum(p)
    if np.isclose(psum,0.0):
        #print("problem with p(z), forcing to 0.0")
        return 0.0
    else:
        cdf = np.concatenate([[0.0], cdf])
        i = np.where(cdf<psum/2.0)[0].max()
        return z[i]
