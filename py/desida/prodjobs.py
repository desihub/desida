# Licensed under a 3-clause BSD style license - see LICENSE.rst.
"""
===============
desida.prodjobs
===============

Tools for querying and saving queue info for production jobs.
"""

import os, sys, glob, argparse
import numpy as np
from astropy.table import Table, vstack

from desiutil.log import get_logger

import desispec.io
from desispec.workflow.tableio import load_table
from desispec.workflow.queue import queue_info_from_qids

def load_proctables(specprod=None):
    """
    Return list of processing tables, loaded from a production

    Options:
        specprod (str): override $SPECPROD, production name

    Returns: list of proctables
    """

    procfiletemplate = desispec.io.findfile('proctable', night='99999999', specprod=specprod, readonly=True)
    procfiles = sorted(glob.glob(procfiletemplate.replace('99999999', '202?????')))

    proctables = [load_table(fn, tabletype='proctable', suppress_logging=True) for fn in procfiles]
    return proctables

def hhmmss2hours(hhmmss):
    """
    Convert an hh:mm:ss string into floating point hours
    """
    hh, mm, ss = hhmmss.split(':')
    hours = int(hh) + int(mm)/60. + int(ss)/3600.
    return hours

def get_zpix_qids(specprod=None):
    """
    Return list of zpix jobids, parsed from log filenames

    Options:
        specprod (str): override $SPECPROD, production name

    Returns: list of jobids
    """
    log = get_logger()
    proddir = desispec.io.specprod_root(specprod)
    zpixlogs = sorted(glob.glob(f'{proddir}/run/scripts/healpix/*/*/*/zpix-*.log'))
    zpix_qids = list()
    for filename in zpixlogs:
        try:
            qid = os.path.splitext(os.path.basename(filename))[0].split('-')[-1]
            qid = int(qid)
        except ValueError:
            log.error(f'Unable to parse integer qid from {filename}; skipping')
            continue

        zpix_qids.append(qid)

    return zpix_qids

def load_qinfo(specprod=None):
    """
    Load queue job info for a spectroscopic production

    Options:
        specprod (str): override $SPECPROD, production name

    Returns: qinfo Table with columns JOBID,JOBNAME,PARTITION,CONSTRAINTS,NNODES,SUBMIT,ELIGIBLE,START,END,ELAPSED,STATE,EXITCODE
    """

    ptables = load_proctables(specprod)

    #- ptab['ALL_QIDS'] is a column of arrays of QIDs per task;
    #- concatenate and group by JOBDESC
    jobdesc_qids = dict()
    for ptab in ptables:
        for jobdesc in np.unique(ptab['JOBDESC']):
            ii = (ptab['JOBDESC'] == jobdesc)
            qids = list(np.concatenate(ptab['ALL_QIDS'][ii]))
            if jobdesc not in jobdesc_qids:
                jobdesc_qids[jobdesc] = qids
            else:
                jobdesc_qids[jobdesc].extend(qids)

    #- Get healpix quids from job log filenames since they aren't tracked in proctables
    jobdesc_qids['zpix'] = get_zpix_qids(specprod)

    #- Cache all the qinfo before printing summaries do to intermediate logging
    qinfo_tables = list()
    columns='jobid,jobname,partition,constraints,nnodes,submit,eligible,start,end,elapsed,state,exitcode'
    for jobdesc, qids in jobdesc_qids.items():
        jobdesc_qinfo = queue_info_from_qids(qids, columns=columns)
        jobdesc_qinfo['JOBDESC'] = jobdesc
        qinfo_tables.append(jobdesc_qinfo)

    qinfo = vstack(qinfo_tables)

    #- Parse HH:MM:SS strings into hours
    #- round to 4 digits (sub-second) to avoid clutter in output files
    hours = np.array([hhmmss2hours(hhmmss) for hhmmss in qinfo['ELAPSED']])
    hours *= qinfo['NNODES']
    qinfo['NODE_HOURS'] = hours.round(4)

    #- Sort CPU / GPU
    qinfo['GPU'] = np.array(np.char.count(qinfo['CONSTRAINTS'], 'gpu')>0, dtype=int)

    #- Convert 'CANCELLED by...' to 'CANCELLED'
    ii = np.char.startswith(qinfo['STATE'], 'CANCELLED by')
    if np.any(ii):
        qinfo['STATE'][ii] = 'CANCELLED'

    if specprod is None:
        qinfo.meta['SPECPROD'] = os.environ['SPECPROD']
    else:
        qinfo.meta['SPECPROD'] = specprod

    return qinfo

def summarize_qinfo(qinfo):
    """
    Convert qinfo table into summary table

    Args:
        qinfo: Table of job queue info

    Returns summary Table with columns JOBDESC,CPUGPU,NODE_HOURS,PERCENT,+jobstates
    with one row per jobdesc.
    """
    jobstates = ['COMPLETED', 'TIMEOUT', 'FAILED', 'CANCELLED', 'NODE_FAIL']

    #- Total node hours across all jobdesc
    tot_hours = np.sum(qinfo['NODE_HOURS'])

    rows = list()
    for jobdesc in ['linkcal', 'nightlybias', 'ccdcalib', 'arc', 'psfnight', 'flat', 'nightlyflat',
                    'tilenight', 'cumulative', 'zpix']:
        ii = qinfo['JOBDESC'] == jobdesc
        jobdesc_qinfo = qinfo[ii]

        row = [jobdesc,]

        if jobdesc_qinfo['GPU'][0]:
            row.append('gpu')
        else:
            row.append('cpu')

        node_hours = round(np.sum(jobdesc_qinfo['NODE_HOURS']), 1)
        percent_time = round(100 * node_hours/tot_hours, 1)

        row.append(node_hours)
        row.append(percent_time)

        for state in jobstates:
            n = np.count_nonzero(jobdesc_qinfo['STATE'] == state)
            row.append(n)

        rows.append(row)

    t = Table(rows=rows, names=['JOBDESC', 'CPUGPU', 'NODE_HOURS', 'PERCENT'] + jobstates)
    return t

def parse(options=None):
    p = argparse.ArgumentParser()
    p.add_argument('-i', '--input',    help="input table of jobs (from a prior run of this script)")
    p.add_argument('-o', '--output',   help="output table of jobs")
    p.add_argument('--summary',   help="save summary table to this file")
    p.add_argument('-s', '--specprod', help="override $SPECPROD")
    p.add_argument('--overwrite', action="store_true", help="Overwrite pre-existing --output and --summary files")
    p.add_argument('--debug', action="store_true", help="Start IPython at end instead of exiting")
    args = p.parse_args(options)
    return args

def main(args=None):
    if not isinstance(args, argparse.Namespace):
        args = parse(args)

    if args.input is not None:
        qinfo = Table.read(args.input)
    else:
        qinfo = load_qinfo(args.specprod)

    if args.output is not None:
        qinfo.write(args.output, overwrite=args.overwrite)

    summary = summarize_qinfo(qinfo)

    if args.summary is not None:
        summary.write(args.summary, overwrite=args.overwrite)

    print(summary)

    if args.debug:
        import IPython
        IPython.embed()

if __name__ == '__main__':
    main()

