'''
   Copyright 2014 The Science and Technology Facilities Council

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   @author: Pavel Demin
'''
#
# Adapted from HTCondorParser by Yun-Ha Shin
#
#  1. Processors and NodeCount are fixed to 1
#  2. MeoryReal and MemoryVirtual are undefined
#  3. Infrastructure is set to APEL-CREAM-BOINC
#

import logging

from apel.db.records.event import EventRecord
from apel.parsers import Parser


log = logging.getLogger(__name__)


class BoincParser(Parser):
    '''
    First implementation of the APEL parser for Boinc
    '''
    def __init__(self, site, machine_name, mpi):
        Parser.__init__(self, site, machine_name, mpi)
        log.info('Site: %s; batch system: %s' % (self.site_name, self.machine_name))

    def parse(self, line):
        '''
        Parses single line from accounting log file.
        '''
        # jobName|localUserId|wallDuration|cpuDuration|startTime|stopTime

        values = line.strip().split('|')

        mapping = {'Site'            : lambda x: self.site_name,
                   'MachineName'     : lambda x: self.machine_name,
                   'Infrastructure'  : lambda x: "APEL-CREAM-BOINC",
                   'JobName'         : lambda x: x[0],
                   'LocalUserID'     : lambda x: x[1],
                   'LocalUserGroup'  : lambda x: "",
                   'WallDuration'    : lambda x: int(x[2]),
                   'CpuDuration'     : lambda x: int(x[3]),
                   'StartTime'       : lambda x: x[4],
                   'StopTime'        : lambda x: x[5],
                   #'MemoryReal'      : lambda x: None,  #int(x[7]),
                   #'MemoryVirtual'   : lambda x: None,  #int(x[8]),
                   'Processors'      : lambda x: 1,
                   'NodeCount'       : lambda x: 1
                  }

        rc = {}

        for key in mapping:
            rc[key] = mapping[key](values)

        record = EventRecord()
        record.set_all(rc)
        return record
