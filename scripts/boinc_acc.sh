#!/bin/bash

# Run 'apelparser' for BOINC job logs
#
#  1. Generate BLAHP log and BATCH job logs to be parsed by apelparser
#  2. Run apelparser against the above generated log files
#
# NOTE: Idea is borrowed from HTCondor-CE
#
#  https://github.com/htcondor/htcondor-ce/blob/master/contrib/apelscripts/condor_blah.sh
#  https://github.com/htcondor/htcondor-ce/blob/master/contrib/apelscripts/condor_batch.sh
#

#-------------------------------------------------------------------------------
# Some env vars to be used
#
APELPARSER=/usr/bin/apelparser
APEL_SCRIPTS_DIR=$(dirname ${BASH_SOURCE[0]})   # /usr/share/apel

BOINC_ACC_CFG=/etc/apel/boinc-acc.cfg
BOINC_JOB_LOG=/home/boinc/job_log_lhcathome.cern.ch_lhcathome.txt

BOINC_PARSER_CFG=/etc/apel/parser-boinc.cfg

OUTPUT_DIR=/var/log/apel/accounting
BLAH_FNAME=boinc_blahp.log
BATCH_FNAME=boinc_job_logs.txt

#-------------------------------------------------------------------------------
# Run it 
#
[ ! -d $OUTPUT_DIR ] && mkdir -p $OUTPUT_DIR

BLAH_OUTPUT_FILE=${OUTPUT_DIR}/${BLAH_FNAME}
BATCH_OUTPUT_FILE=${OUTPUT_DIR}/${BATCH_FNAME}

today=$(date -u --date='00:00:00 today' +%s)
yesterday=$(date -u --date='00:00:00 yesterday' +%s)

# generate blahp.log
${APEL_SCRIPTS_DIR}/boinc_acc.py --blah \
                                 --conf $BOINC_ACC_CFG \
                                 --flog $BOINC_JOB_LOG \
                                 --since $yesterday \
                                 --until $today > $BLAH_OUTPUT_FILE
# generate batch job log
${APEL_SCRIPTS_DIR}/boinc_acc.py --conf $BOINC_ACC_CFG \
                                 --flog $BOINC_JOB_LOG \
                                 --since $yesterday \
                                 --until $today > $BATCH_OUTPUT_FILE
# run apelparser 
$APELPARSER --config $BOINC_PARSER_CFG
