#! /usr/bin/python

# Gecko's Toolkit's Storage module logs to the mozStorage module using NSPR.
# This file parses and understands the log results.  The primary goal is to
# be able to run Firefox with the logging enabled and end up with a pile of
# useful information.
#
# An example command line to produce such a log would be to have the following
# environment variables set when running firefox, such as by pasting this at
# the front of a shell command line:
# NSPR_LOG_MODULES=mozStorage:5,timestamp NSPR_LOG_FILE=/tmp/mozStorage.log 
#
# Core goals:
# - Be able to filter results to connections based on filename.  (We don't have
#   path to go on right now; we should probably enhance mozStorage's logging
#   to be profile-relative or something like that.)
# - Be able to easily extract the list of statements used (with values) to
#   generate EXPLAIN and grokexplain.py output for them in a bulk-ish fashion.
# - Be able to produce some profilish-ish performance statistics from the logs
#
# ## Implementation Overview ##
# 
# Log lines are parsed into a simple normalized dictionary-style representation
# from their human readable form.  Higher level processing is then done on
# those, but always keeping thost dicts around.
#
# All entries include the following keys:
# - ts (long): timestamp, JS-style; millis since epoch
# - tid (str): thread id
# - raw: the raw string payload of the message
# - type: the entry type
#
# We define the following named types of these entries:
# - open: connection opened
# - close: connection closed
# - init: statement initialized
# - exec: statement executed (sqlite3_trace)
# - reset: statement reset
# - finalize: statement finalized
# 
# 
# ## Meta ##
# This script is written in python because I had some existing log parsing logic
# from my Thunderbird days available and because both grokexplain.py (from this
# project, grok-sqlite-explain) and the amazing
# https://github.com/laysakura/SQLiteDbVisualizer tool were written in Python.
#
# My plan is that, like SQLiteDbVisualizer, we use JSON as an interchange
# format, supporting JSON dumping at multiple levels of abstraction, and that
# any UIs will just consume the JSON.

import calendar, time
from datetime import datetime
import os, os.path
import re

import optparse

VERBOSE = False

def coalesce_indented_lines(lineGen):
    '''Consume a line-providing iterator, coalescing lines that were wrapped
    with indentation, just like many storage users may do when trying to make
    their SQL pretty.  This isn't super-smart; lazy log jerks will break us.
    '''
    accum_line = None
    for line in lineGen:
        is_indented = line.startswith(' ') or line.startswith(')')
        if accum_line is not None:
            if not is_indented:
                yield accum_line
                accum_line = line
            else:
                accum_line += line
        else:
            accum_line = line
    if accum_line is not None:
        yield accum_line

# example:
# 2015-01-15 21:24:23.942870 UTC
NSPR_LOG_TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S.%f %Z'
def parse_nspr_log_timestamp(s):
    '''Parse the NSPR log timestamp to a JS-style milliseconds-since-epoch'''
    # Ugh.  Although I fear that ugh may also be partially due to Stockholm
    # syndrome from JS's time representation.
    dt = datetime.strptime(s, NSPR_LOG_TIMESTAMP_FORMAT)
    return calendar.timegm(dt.utctimetuple()) * 1000 + dt.microsecond

def unwrap_nspr_log_lines(lineGen):
    '''Consume NSPR timestamped log lines, generating a tuple of (JS-style
    timestamp (long), thread id (str), the message payload)'''
    for line in lineGen:
        tsStr = line[:30]
        if not tsStr.endswith('UTC'):
            if VERBOSE:
                print 'Line with bad timestamp:', line
            continue
        ts = parse_nspr_log_timestamp(tsStr)

        idxThreadStart = line.find('[', 32)
        idxThreadEnd = line.find(']:', idxThreadStart + 1)
        if idxThreadStart == -1:
            if VERBOSE:
                print 'Line with bad thread id:', line
            continue
        tid = line[idxThreadStart+1:idxThreadEnd]
        
        msg = line[idxThreadEnd+3:]

        yield ts, tid, msg


# Opening connection to 'places.sqlite' (7f39931861a0)
RE_OPEN = re.compile("^Opening connection to '(.+)' \(([0-9a-fA-F]+)\)$")
# Closing connection to 'cookies.sqlite'
RE_CLOSE = re.compile("^Closing connection to '(.+)'$")
# Initialized statement 'SOME SQL WITH PARAMETER PLACEHOLDERS' (0x7f398ea28b20)
RE_INIT = re.compile("^Initialized statement '(.+)' \(0x([0-9a-fA-F]+)\)$",
                     re.DOTALL)
# Inited async statement 'SOME SQL' (0x7ffff9de14f8)
RE_INITASYNC = re.compile("^Inited async statement '(.+)' " +
                          "\(0x([0-9a-fA-F]+)\)$",
                          re.DOTALL)
# sqlite3_trace on 7f399f719fe0 for 'SOME SQL WITH PARAMETERS FILLED'
RE_EXEC = re.compile("^sqlite3_trace on ([0-9a-fA-F]+) for '(.+)'$",
                     re.DOTALL)
# Resetting statement: 'SOME SQL WITH PARAMETER PLACEHOLDERS'
RE_RESET = re.compile("^Resetting statement: '(.+)'$",
                      re.DOTALL)
# Finalizing statement 'SOME SQL W/PLACEHOLDERS'
# Finalizing statement 'SOME SQL W/PLACEHOLDERS' during garbage-collection
RE_FINALIZE = re.compile("^Finalizing statement '(.+)'" +
                         "( during garbage-collection)?$",
                         re.DOTALL)
class StorageLogParser(object):
    '''
    Generator style low-level parser.
    '''
    def parse(self, f):
        for ts, tid, msg in unwrap_nspr_log_lines(coalesce_indented_lines(f)):
            firstWord = msg[:msg.find(' ')]
            d = {
                'ts': ts,
                'tid': tid,
                #'raw': msg
            }
            if firstWord == 'Opening':
                m = RE_OPEN.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad open msg:', msg
                else:
                    d['type'] = 'open'
                    d['filename'] = m.group(1)
                    d['conn'] = m.group(2)
            elif firstWord == 'Closing':
                m = RE_CLOSE.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad close msg:', msg
                else:
                    d['type'] = 'close'
                    d['filename'] = m.group(1)
            elif firstWord == 'Initialized':
                m = RE_INIT.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad init msg:', msg
                else:
                    d['type'] = 'init'
                    d['async'] = False
                    d['sql'] = m.group(1)
                    d['conn'] = m.group(2)
            elif firstWord == 'Inited':
                m = RE_INITASYNC.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad initasync msg:', msg
                else:
                    d['type'] = 'init'
                    d['async'] = True
                    d['sql'] = m.group(1)
                    d['conn'] = m.group(2)
            elif firstWord == 'sqlite3_trace':
                m = RE_EXEC.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad exec msg:', msg
                else:
                    d['type'] = 'exec'
                    d['sql'] = m.group(2)
                    d['conn'] = m.group(1)
            elif firstWord == 'Resetting':
                m = RE_RESET.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad reset msg:', msg
                else:
                    d['type'] = 'reset'
                    d['sql'] = m.group(1)
            elif firstWord == 'Finalizing':
                m = RE_FINALIZE.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad finalize msg:', repr(msg)
                else:
                    d['type'] = 'finalize'
                    d['sql'] = m.group(1)
                    d['gc'] = m.group(2) and True or False
            else:
                d['type'] = 'unknown'
                if VERBOSE:
                    print 'Weird mozStorage line', msg
            print d
            yield d
    

class CmdLine(object):
    usage = '''usage: %prog [options] mozStorage_nspr.log

    Process mozStorage NSPR log output.
    '''

    def buildParser(self):
        parser = optparse.OptionParser(usage=self.usage)

        parser.add_option('-v', '--verbose',
                          action='store_true', dest='verbose', default=False,
                          help='Output a lot of info about what we are doing.')

        parser.add_option('--db-path',
                          dest='db_path', default=None,
                          help=('Path to the database we care about from this '+
                                'log; we will filter connections to this name '+
                                'as well.'))

        parser.add_option('-o', '--output-dir',
                          dest='out_dir', default=None,
                          help='Directory to output results in.')

        return parser
    
    def run(self):
        global VERBOSE

        parser = self.buildParser()
        options, args = parser.parse_args()

        VERBOSE = options.verbose

        # create the output directory if it doesn't already exist
        if options.out_dir:
            if not os.path.exists(options.out_dir):
                os.mkdir(options.out_dir)
        
        db_path = options.db_path
        if db_path:
            db_filename = os.path.basename(db_path)
        else:
            db_filename = None

        for filename in args:
            parser = StorageLogParser()
            f = open(filename, 'rt')
            arr = list(parser.parse(f))
            f.close()


if __name__ == '__main__':
    cmdline = CmdLine()
    cmdline.run()
