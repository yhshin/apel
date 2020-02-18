#!/usr/bin/env python
'''
Create BLAHP or BATCH log file to be processed by 'apelparser' for the jobs completed in given time period
'''

from __future__ import print_function

import os
import sys
import platform

from datetime import datetime
from functools import partial

#from configparser import ConfigParser   # py3
from ConfigParser import ConfigParser

from argparse import ArgumentParser

#-------------------------------------------------------------------------------
# util function
#
host = platform.node().split('.')[0]
sep = '.'

#
def build_local_jobid(endtime, jobname):
    return sep.join( (host, str(endtime), jobname[:10]))

#-------------------------------------------------------------------------------
class BoincBlah:

    def __init__(self, ce, dn, fqan, user, since=0, until=None):
        fmt  = '"timestamp={endtime}" "userDN={dn}" "userFQAN={fqan}" "ceID={ce}"'
        fmt += ' "jobID={jobname}" "lrmsID={lrmsid}" "localUser={user}"'
        self.fmt = partial(fmt.format, dn=dn, fqan=fqan, ce=ce, user=user)
        self.since = since
        self.until = sys.maxint if until is None else until

    def gen_timestamp(self, endtime):
        dt = datetime.utcfromtimestamp(endtime)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def parse(self, line):
        '''
        Parse boinc job log and print blahp log from it
        '''
        # endtime ue <ue> ct <cputime> fe <flops> nm <jobname> et <runtime> es <exit_status>
        
        values = line.split()
        endtime = int(values[0])
        jobname = values[8]

        if endtime < since or endtime > until:
            return

        timestamp = self.gen_timestamp(endtime)
        localJobID = build_local_jobid(endtime, jobname)

        #return self.fmt.format(endtime=timestamp, jobname=jobname, lrmsid=localJobID)
        return self.fmt(endtime=timestamp, jobname=jobname, lrmsid=localJobID)

#-------------------------------------------------------------------------------
class BoincBatch:

    def __init__(self, user, since=0, until=None):
        self.fmt = "{jobname}|%s|{wall}|{cpu}|{start}|{stop}" % user
        self.since = since
        self.until = sys.maxint if until is None else until

    def parse(self, line):
        '''
        Parse boinc job log and print blahp log from it
        '''
        # input:
        #   endtime ue <ue> ct <cputime> fe <flops> nm <jobname> et <runtime> es <exit_status>
        # output:
        #   localJobName|localUserId|wallDuration|cpuDuration|startTime|stopTime
        
        values = line.split()
        endtime = int(values[0])

        if endtime < since or endtime > until:
            return

        cputime = int(round(float(values[4])))
        runtime = float(values[10])
        jobname = values[8]

        starttime = int(endtime - runtime)
        walltime  = cputime     # Di's suggestion

        localJobID = build_local_jobid(endtime, jobname)

        return self.fmt.format(jobname=localJobID, wall=walltime, cpu=cputime,
                               start=starttime, stop=endtime)
 
#-------------------------------------------------------------------------------
if __name__ == '__main__':

    boinc_job_log = '/home/boinc/job_log_lhcathome.cern.ch_lhcathome.txt'

    arg_parser = ArgumentParser(description='Create BLAHP or BATCH log file for boinc jobs to be handled by apelparser')
    arg_parser.add_argument('--blah', action='store_true',
                            help='generate blahp.log if set; otherwise batch job logs')
    arg_parser.add_argument('--since','-s', type=int, default=0,
                            help='UTC timestamp of the jobs to be processed')
    arg_parser.add_argument('--until','-u', type=int, default=None,
                            help='UTC timestamp of the jobs to be processed')
    arg_parser.add_argument('--conf','-c', default='/etc/apel/boinc.cfg',
                            help='config file for boinc parameters')
    arg_parser.add_argument('--flog','-f', default=boinc_job_log,
                            help='config file for boinc parameters')
                            
    args = arg_parser.parse_args()

    boinc_job_log = args.flog
    since = args.since
    until = args.until

    # read config params
    cp = ConfigParser()
    cp.read(args.conf)

    ce = cp.get('blah', 'ce')
    dn = cp.get('blah', 'dn')
    fqan = cp.get('blah', 'fqan')
    user = cp.get('batch', 'local_user_id')

    if args.blah:
        boinc = BoincBlah(ce, dn, fqan, user, since, until)
    else:
        boinc = BoincBatch(user)

    with open(boinc_job_log) as f:
        for line in f:
            output = boinc.parse(line)
            print(output)
