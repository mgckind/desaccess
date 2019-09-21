#!/usr/bin/env python

"""
author: Landon Gelman, 2019
description: command line tools for making single epoch cutouts from the Dark Energy Survey catalogs
"""

import os, sys
import argparse
import datetime
import easyaccess as ea
import logging
import logging.config
import glob
import json
import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymongo
import pywraps2 as s2
import sys
import time
import uuid
import yaml
from astropy import units
from astropy.io import fits
from astropy.nddata import Cutout2D
from astropy.coordinates import SkyCoord
from astropy.wcs import WCS
from astropy.wcs import utils
from pymongo import MongoClient


ARCMIN_TO_DEG = 0.0166667       # deg per arcmin
LEVELS = [7,8,9,10] # Should not be set by user unless we have multiple databases to choose from that use different levels.
SURVEYS = ['Y1A1_FINALCUT','Y3A1_FINALCUT']
CCDS_FOLDER = ''
CCDS_PREFIX = ''
OUTDIR = ''
CLIENT = ''
PORT = ''
DBNAME = ''

def getPathSize(path):
    dirsize = 0
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            dirsize += getPathSize(entry.path)
        else:
            try:
                dirsize += os.path.getsize(entry)
            except FileNotFoundError:
                continue

    return dirsize


def _DecConverter(ra, dec):
    ra1 = np.abs(ra/15)
    raHH = int(ra1)
    raMM = int((ra1 - raHH) * 60)
    raSS = (((ra1 - raHH) * 60) - raMM) * 60
    raSS = np.round(raSS, decimals=4)
    raOUT = '{0:02d}{1:02d}{2:07.4f}'.format(raHH, raMM, raSS) if ra > 0 else '-{0:02d}{1:02d}{2:07.4f}'.format(raHH, raMM, raSS)

    dec1 = np.abs(dec)
    decDD = int(dec1)
    decMM = int((dec1 - decDD) * 60)
    decSS = (((dec1 - decDD) * 60) - decMM) * 60
    decSS = np.round(decSS, decimals=4)
    decOUT = '-{0:02d}{1:02d}{2:07.4f}'.format(decDD, decMM, decSS) if dec < 0 else '+{0:02d}{1:02d}{2:07.4f}'.format(decDD, decMM, decSS)

    return raOUT + decOUT


def _getColorList(colors):
    c = []
    if 'G' in colors:
        c.append('g')
    if 'R' in colors:
        c.append('r')
    if 'I' in colors:
        c.append('i')
    if 'Z' in colors:
        c.append('z')
    if 'Y' in colors:
        c.append('Y')
    return c


def MakeFitsCut(ccd, outdir, size, positions, rect_id, df, p):
    logger = logging.getLogger(__name__)
    os.makedirs(outdir, exist_ok=True)

    # Get the name of the CCD's FITS file and open it.
    #path = CCDS_FOLDER + ccd['FILENAME'] + ccd['COMPRESSION'] #ccd['FULL_PATH']
    path = CCDS_PREFIX + ccd['FULL_PATH'] + ccd['COMPRESSION']
    #print(path)
    
    hdul = None
    try:
        hdul = fits.open(path)
    except FileNotFoundError as e:
        logger.info('No FITS file for CCD {} found. Will not create cutout for this CCD.'.format(rect_id))
        return

    # Create a file name for the new fits file.
    filenm = outdir + 'DESJ' + _DecConverter(df['RA'][p], df['DEC'][p]) + '_{}.fits'.format(rect_id)
    #print('FILENM: {}'.format(filenm))

    # Start new fits file and define the pixel scale.
    newhdul = fits.HDUList()
    pixelscale = None

    # Get the correct xs/ys sizes from the user
    if 'XSIZE' in df or 'YSIZE' in df:
        if 'XSIZE' in df and not np.isnan(df['XSIZE'][p]):
            uxsize = df['XSIZE'][p] * units.arcmin
        else:
            uxsize = size[1]
        if 'YSIZE' in df and not np.isnan(df['YSIZE'][p]):
            uysize = df['YSIZE'][p] * units.arcmin
        else:
            uysize = size[0]
        usize = units.Quantity((uysize, uxsize))
    else:
        usize = size

    # Iterate over all HDUs in the CCD
    for i in range(len(hdul)):
        if hdul[i].name == 'PRIMARY':
            continue

        h = hdul[i].header
        data = hdul[i].data
        header = h.copy()
        w = WCS(header)

        try:
            cutout = Cutout2D(data, positions[p], usize, wcs=w, mode='trim')
        except ValueError as e:
            #print('MakeFitsCut - File: {} - EXTNAME: \'{}\', Error: {}'.format(ccd['FILENAME'], header['EXTNAME'], e))
            logger.info('MakeFitsCut - HDU \"{}\" in CCD {} does not contain WCS element. Cutout will not contain this HDU.'.format(header['EXTNAME'], rect_id))
            pass
        else:
            crpix1, crpix2 = cutout.position_cutout
            x, y = cutout.position_original
            crval1, crval2 = w.wcs_pix2world(x, y, 1)

            header['CRPIX1'] = crpix1
            header['CRPIX2'] = crpix2
            header['CRVAL1'] = float(crval1)
            header['CRVAL2'] = float(crval2)
            header['HIERARCH RA_CUTOUT'] = df['RA'][p]
            header['HIERARCH DEC_CUTOUT'] = df['DEC'][p]

        if not newhdul:
            newhdu = fits.PrimaryHDU(data=cutout.data, header=header)
            pixelscale = utils.proj_plane_pixel_scales(w)
        else:
            newhdu = fits.ImageHDU(data=cutout.data, header=header, name=h['EXTNAME'])
        newhdul.append(newhdu)

    if pixelscale is not None:
        dx = int(usize[1] * ARCMIN_TO_DEG / pixelscale[0] / units.arcmin) # pixelscale is in degrees (CUNIT)
        dy = int(usize[0] * ARCMIN_TO_DEG / pixelscale[1] / units.arcmin)
        if newhdul[0].header['NAXIS1'] < dx or newhdul[0].header['NAXIS2'] < dy:
            logger.info('MakeFitsCut - {} is smaller than user requested. This is likely because the object/coordinate was in close proximity to the edge of a tile.'.format(('/').join(filenm.split('/')[-2:])))

    newhdul.writeto(filenm, output_verify='exception', overwrite=True, checksum=False)
    newhdul.close()
    logger.info('MakeFitsCut - CCD {} complete.'.format(rect_id))


def run(args):
    ### Connect to the local MongoDB database and get the collections.
    client = MongoClient(CLIENT, PORT)
    db = client[ DBNAME ]
    collections = db.list_collection_names()

    conn = ea.connect(user=args.usernm, passwd=args.passwd)
    curs = conn.cursor()

    usernm = conn.user

    conn.close()

    if args.jobid:
        jobid = args.jobid
    else:
        jobid = str(uuid.uuid4())
    
    outdir = OUTDIR

    """
    try:
        os.makedirs(outdir, exist_ok=False)
    except OSError as e:
        print(e)
        print('Specified jobid already exists in output directory. Aborting job.')
        conn.close()
        sys.exit(1)
    """

    ### Start logging
    summary = {}
    logtime = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    #logname = outdir + 'SingleEpoch_' + logtime + '.log'
    logname = outdir + 'log.log'
    logging.basicConfig(filename=logname, format='%(asctime)s - %(levelname)-8s - %(message)s', level=logging.INFO)
    logger = logging.getLogger(__name__)

    ### Get the user variables from args.
    xs = float(args.xsize)
    ys = float(args.ysize)
    colors = _getColorList(args.colors.split(','))

    logger.info('Selected Options:')

    ### This puts any input type into a pandas dataframe. Refer to L399-418 in bulkthumbs_12.py if you want to follow the same logging strategy.
    if args.csv:
        userdf = pd.DataFrame(pd.read_csv(args.csv))
        logger.info('    CSV: '+args.csv)
        summary['csv'] = args.csv
    elif args.ra:
        coords = {}
        coords['RA'] = args.ra
        coords['DEC'] = args.dec
        userdf = pd.DataFrame.from_dict(coords, orient='columns')
        logger.info('    RA: '+str(args.ra))
        logger.info('    DEC: '+str(args.dec))
        summary['ra'] = str(args.ra)
        summary['dec'] = str(args.dec)

    logger.info('    X size: '+str(args.xsize))
    logger.info('    Y size: '+str(args.ysize))
    logger.info('    Make FITS? '+str(args.make_fits))
    logger.info('    Bands: '+args.colors)
    if args.airmass:
        logger.info('    Airmass (upper limit): '+str(args.airmass))
    if args.fwhm:
        logger.info('    FWHM (upper limit, arcseconds): '+str(args.fwhm))
    logger.info('User: ' + usernm)
    logger.info('JobID: ' + str(jobid))
    summary['xsize'] = str(args.xsize)
    summary['ysize'] = str(args.ysize)
    summary['make_fits'] = str(args.make_fits)
    summary['bands'] = args.colors
    summary['user'] = usernm
    summary['jobid'] = str(jobid)

    ### Get the S2Cell ID of each input coordinate. Add it as a new column in the user dataframe.
    if 'RA' in userdf:
        unmatched_coords = {'RA':[], 'DEC':[]} # For any coordinates not matching our S2 cell database.
        S2LL, S2ID = [], []
        
        for i in range(len(userdf)):
            latlng = s2.S2LatLng.FromDegrees(userdf['DEC'][i], userdf['RA'][i]).Normalized()
            S2LL.append(latlng)

            cellid30 = s2.S2CellId(latlng)
            face = cellid30.face()
            pos = cellid30.pos()
            
            for lvl in LEVELS:
                cellid_at_level = s2.S2CellId.FromFacePosLevel(face, pos, lvl)
                cellid_at_level = str(cellid_at_level.id())
                
                # Check if the cellid exists at this level. If not found at last level, set it to null.
                if cellid_at_level in collections:
                    S2ID.append(cellid_at_level)
                    #print(userdf['RA'][i], userdf['DEC'][i], lvl, cellid_at_level)
                    #print()
                    break
                elif lvl == LEVELS[-1]:
                    S2ID.append(np.nan)
                else:
                    continue
            
        userdf = userdf.assign(S2LL=S2LL, S2ID=S2ID)
        dftemp = userdf[ (userdf['S2ID'].isnull()) ]
        unmatched_coords['RA'] = dftemp['RA'].tolist()
        unmatched_coords['DEC'] = dftemp['DEC'].tolist()
        userdf = userdf.dropna(axis=0, how='any', subset=['S2ID'])
        
        logger.info('Unmatched coordinates: \n{0}\n{1}'.format(unmatched_coords['RA'], unmatched_coords['DEC']))
        summary['Unmatched_Coords'] = unmatched_coords
        #print('Unmatched coordinates: \n{0}\n{1}'.format(unmatched_coords['RA'], unmatched_coords['DEC']))
        #print()

    userdf = userdf.sort_values(by=['S2ID'])
    userdf = userdf.drop_duplicates(['RA','DEC'], keep='first')

    query_time = []     # startq, endq
    process_time = []       #startp, endp

    ccddict = {}

    cellids = userdf['S2ID'].unique()
    for i in cellids:
        udf = userdf[ userdf.S2ID == i ]
        udf = udf.reset_index()

        #print(udf.head())
        #print()
        
        collection = db[ i ]

        size = units.Quantity((ys, xs), units.arcmin)
        positions = SkyCoord(udf['RA'], udf['DEC'], frame='icrs', unit='deg', equinox='J2000', representation_type='spherical')

        # Set up the mongo query so we can filter out CCDs that don't satisfy the user's requirements
        query = {}
        #if args.blacklist:
        #   query['BLACKLISTED'] = { '$in' : ['Y','N'] }
        #else:
        #   query['BLACKLISTED'] = 'N'
        if args.colors:
            query['BAND'] = { '$in' : colors }
        if args.airmass:
            query['AIRMASS'] = { '$lte' : args.airmass }
        if args.fwhm:
            query['FWHM'] = { '$lte' : args.fwhm }
        logger.info('DB query: ' + query)
        #print(query)
        #print()

        startq = time.time()

        # This makes rectangles from ccds in this collection/cellid
        # collection.find() finds all matches
        crects = []
        cids = []
        for c in collection.find(query):
            cpnt1 = s2.S2LatLng.FromDegrees(c['DECC1'],c['RAC1']).Normalized()
            cpnt2 = s2.S2LatLng.FromDegrees(c['DECC2'],c['RAC2']).Normalized()
            cpnt3 = s2.S2LatLng.FromDegrees(c['DECC3'],c['RAC3']).Normalized()
            cpnt4 = s2.S2LatLng.FromDegrees(c['DECC4'],c['RAC4']).Normalized()
            
            ccd_rect = s2.S2LatLngRect()
            ccd_rect.AddPoint(cpnt1)
            ccd_rect.AddPoint(cpnt2)
            ccd_rect.AddPoint(cpnt3)
            ccd_rect.AddPoint(cpnt4)
            crects.append(ccd_rect)
            cids.append(c['_id'])

        endq = time.time()
        query_time.append(endq-startq)
        startp = time.time()

        # This goes through the user coordinates in this collection/cellid
        for u in range(len(udf)):
            dictname = '{}_{}'.format(udf['RA'][u], udf['DEC'][u])
            ccddict[dictname] = {}
            ccddict['S2CELL'] = i
            ccddict[dictname]['FILENAME'] = []
            ccddict[dictname]['FULL_PATH'] = []

            logger.info('CCDs which contain RA: {} DEC: {}'.format(udf['RA'][u], udf['DEC'][u]))
            #print('CCDs which contain RA: {} DEC: {}'.format(udf['RA'][u], udf['DEC'][u]))
            #print()

            # This checks if the current coordinates exist within any ccd rectangles
            for r in range(len(crects)):
                if s2.S2LatLngRect.Contains(crects[r], udf['S2LL'][u]):
                    #print(collection.find_one({'_id':cids[r]})['FILENAME'] + collection.find_one({'_id':cids[r]})['COMPRESSION'])
                    ccd = collection.find_one({'_id':cids[r]})

                    ccddict[dictname]['FILENAME'].append('{}{}'.format(ccd['FILENAME'], ccd['COMPRESSION']))
                    ccddict[dictname]['FULL_PATH'].append(ccd['FULL_PATH'])
                    
                    if args.make_fits:
                        MakeFitsCut(ccd, outdir+i+'/', size, positions, cids[r], udf, u)
                    #print()
            #print()
        endp = time.time()
        process_time.append(endp-startp)

    qtime = '{0:.2f}'.format(np.sum(query_time))
    ptime = '{0:.2f}'.format(np.sum(process_time))
    #print('Queryring took (s): ' + qtime)
    #print('Processing took (s): ' + ptime)
    logger.info('Querying took (s): ' + qtime)
    logger.info('Prcoessing took (s): ' + ptime)
    summary['query_time'] = qtime
    summary['prcoessing_time'] = ptime

    if args.return_list:
        #print(ccddict)
        os.makedirs(outdir, exist_ok=True)
        listname = 'SE_' + jobid.upper().replace("-","_") + '.json'
        with open(outdir+listname, 'w') as outfile:
            json.dump(ccddict, outfile)

    os.makedirs(outdir, exist_ok=True)
    dirsize = getPathSize(outdir)
    dirsize = dirsize * 1. / 1024
    if dirsize > 1024. * 1024:
        dirsize = '{0:.2f} GB'.format(1. * dirsize / 1024. /1024)
    elif dirsize > 1024.:
        dirsize = '{0:.2f} MB'.format(1. * dirsize / 1024.)
    else:
        dirsize = '{0:.2f} KB'.format(dirsize)

    logger.info('All processing finished.')
    logger.info('Total file size on disk: {}'.format(dirsize))
    summary['size_on_disk'] = str(dirsize)

    files = glob.glob(outdir + '*/*')
    logger.info('Total number of files: {}'.format(len(files)))
    summary['number_of_files'] = len(files)
    files = [i.split('/')[-2:] for i in files]
    files = [('/').join(i) for i in files]
    files = [('.').join(i.split('.')[-4:-1]) for i in files]
    files = [i.split('_')[0] for i in files]
    files = list(set(files))
    summary['files'] = files

    jsonfile = outdir + 'SingleEpoch_'+logtime+'_SUMMARY.json'
    with open(jsonfile, 'w') as fp:
        json.dump(summary, fp)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Dark Energy Survey Single Epoch Cutout Service")

    parser.add_argument('--mdb', type=str, required=False, help='Specify Mongo DB to use.')
    parser.add_argument('--csv', type=str, required=False, help='A CSV with columns \'RA,DEC\'')
    parser.add_argument('--ra', nargs='*', type=float, required=False, help='RA (decimal degrees)')
    parser.add_argument('--dec', nargs='*', type=float, required=False, help='DEC (decimal degrees)')
    parser.add_argument('--make_fits', action='store_true', help='Creates FITS files in the desired bands of the cutout region.')
    parser.add_argument('--return_list', action='store_true', help='Saves list of inputted objects and their matched ccds to user directory.')
    parser.add_argument('--xsize', type=float, required=False, default=1.0, help='Size in arcminutes of the cutout x-axis. Default: 1.0')
    parser.add_argument('--ysize', type=float, required=False, default=1.0, help='Size in arcminutes of the cutout y-axis. Default: 1.0')
    parser.add_argument('--colors', type=str.upper, default='G,R,I,Z,Y', help='Color bands for the fits cutout. Enter comma-separated list. Not case sensitive. Default: g,r,i,z,y')
    #parser.add_argument('--blacklist', action='store_true', help='Include blacklisted CCDs. Default: False (no blacklisted CCDs)')
    parser.add_argument('--airmass', required=False, type=float, help='Upper limit of air mass.')
    parser.add_argument('--fwhm', required=False, type=float, help='Upper limit of FWHM, PSF-based.')
    parser.add_argument('--jobid', type=str, required=False, help='Option to manually specify a jobid for this job.')
    parser.add_argument('--usernm', required=False, help='Username for database; otherwise uses values from desservices file.')
    parser.add_argument('--passwd', required=False, help='Password for database; otherwise uses values from desservices file.')
    parser.add_argument('--outdir', type=str, required=False, help='Specify output directory.')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    with open('config/desaccess.yaml','r') as cfile:
        conf = yaml.load(cfile)
    
    CCDS_FOLDER = conf['directories']['ccds'] + '/'
    CCDS_PREFIX = conf['directories']['ccds_prefix'] + '/'
    CLIENT = conf['mongoclient']['host']
    PORT = int(conf['mongoclient']['port'])
    
    if args.outdir:
        OUTDIR = args.outdir
    else:
        OUTDIR = conf['directories']['outdir'] + '/'
    
    if args.mdb:
        DBNAME = args.mdb
    else:
        DBNAME = conf['mongoclient']['db']

    if not args.csv and not (args.ra and args.dec):
        print('Please include either RA/DEC coordinates or Coadd IDs.')
        sys.exit(1)
    if (args.ra and args.dec) and len(args.ra) != len(args.dec):
        print('Remember to have the same number of RA and DEC values when using coordinates.')
        sys.exit(1)
    if (args.ra and not args.dec) or (args.dec and not args.ra):
        print('Please include BOTH RA and DEC if not using Coadd IDs.')
        sys.exit(1)
    if not args.make_fits and not args.return_list:
        print('Nothing to do. Please select make_fits or return_list.')
        sys.exit(1)

    run(args)