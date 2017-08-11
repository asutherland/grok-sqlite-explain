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
# - type: the entry type
#
# Currently commented out, but you can put it back:
# - raw: the raw string payload of the message
#
# We define the following named types of these entries:
# - open: connection opened. { filename, conn }
# - close: connection closed. { filename, conn }
# - init: statement initialized. { async, sql, conn, stmt } where sql has the
#   parameter placeholders intact.
# - initAsync: the sqlite3_stmt created for an async mozStorage statement.
#   { sql, conn, stmt, native }
# - exec: statement executed (sqlite3_trace). { sql, conn } where sql has the
#   parameter placeholders replaced with the values.
# - profile: statement execution completed (sqlite3_profile) { sql, conn,
#   durationMS }.
# - reset: statement reset and bindings cleared.  Only present in DEBUG builds.
#   { sql, conn, stmt } where sql has the parameter placeholders
#   intact.
# - finalize: statement finalized. { sql, gc, conn, stmt }
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
from collections import OrderedDict
from datetime import datetime
import os, os.path
import re
import subprocess

import grokexplain

import json
import optparse

VERBOSE = False
PARANOIA = False

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

        # +3 gets us to the "D/whatever", +5 gets us to the "whatever"
        level = line[idxThreadEnd+3:idxThreadEnd+4]
        idxModuleSpace = line.find(' ', idxThreadEnd+5)
        module = line[idxThreadEnd+5:idxModuleSpace]
        msg = line[idxModuleSpace+1:].rstrip()

        #print repr((ts, tid, level, module, msg))
        yield ts, tid, level, module, msg


# Opening connection to 'places.sqlite' (7f39931861a0)
RE_OPEN = re.compile("^Opening connection to '(.+)' \((?:0x)?([0-9a-fA-F]+)\)$")
# Closing connection to 'cookies.sqlite'
RE_CLOSE = re.compile("^Closing connection to '(.+)' \((?:0x)?([0-9a-fA-F]+)\)$")
# Initialized statement 'SOME SQL WITH PARAMETER PLACEHOLDERS' (0x7f398ea28b20)
# NOTE! the pointer is the statement pointer, not the connection id!
RE_INIT = re.compile("^(Initialized|Inited async) statement '(.+)' \(conn 0x([0-9a-fA-F]+) stmt 0x([0-9a-fA-F]+)\)$",
                     re.DOTALL)
# sqlite3_trace on 7f399f719fe0 for 'SOME SQL WITH PARAMETERS FILLED'
RE_EXEC = re.compile("^sqlite3_trace on conn 0x([0-9a-fA-F]+) for '(.+)'$",
                     re.DOTALL)
RE_PROFILE = re.compile(
    "^sqlite3_profile on conn 0x([0-9a-fA-F]+) duration (\d+) for '(.+)'$",
    re.DOTALL)
# Resetting statement: 'SOME SQL WITH PARAMETER PLACEHOLDERS'
RE_RESET = re.compile("^Resetting statement: '(.+)' " +
                      "\(conn 0x([0-9a-fA-F]+) stmt 0x([0-9a-fA-F]+)\)$",
                      re.DOTALL)
# Finalizing statement 'SOME SQL W/PLACEHOLDERS'
# Finalizing statement 'SOME SQL W/PLACEHOLDERS' during garbage-collection
# Auto-finalizing SQL statement '...' (conn %p stmt %p)
RE_FINALIZE = re.compile(
  "^(?:Finalizing|Auto-finalizing) (async )?statement '(.+)'" +
  "( during garbage-collection)? " +
  "\(conn 0x([0-9a-fA-F]+) stmt 0x([0-9a-fA-F]+)\)$",
  re.DOTALL)

# Native async statement (conn %p stm %p) initialized '...' as %p
RE_NATIVE_ASYNC = re.compile(
  "^Native async statement \(conn 0x([0-9a-fA-F]+) stmt 0x([0-9a-fA-F]+)\)" +
  " initialized '(.+)' as 0x([0-9a-fA-F]+)$",
  re.DOTALL)

RE_TRACE_STMT = re.compile(
  "^TRACE_STMT on ([0-9a-fA-F]+): '(.+)'$",
  re.DOTALL)
RE_TRACE_TIME = re.compile(
  "^TRACE_TIME on ([0-9a-fA-F]+): (.+)ms$",
  re.DOTALL)

# !!! I haven't implemented parsing for these yet (not seeing them in usage):
# Vacuum failed with error: %d '%s'. Database was: '%s'
# Compilation failure: '...' could not be compiled due to an error: ...
# Cloned statement (conn %p stmt %p) to async native %p

class StorageLogParser(object):
    '''
    Generator style low-level parser.
    '''
    def parse(self, f):
        for ts, tid, level, module, msg in unwrap_nspr_log_lines(coalesce_indented_lines(f)):
            firstWord = msg[:msg.find(' ')]
            d = OrderedDict()
            d['ts'] = ts
            d['tid'] = tid
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
                    d['conn'] = m.group(2)
            elif firstWord == 'Initialized' or firstWord == 'Inited':
                m = RE_INIT.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad init msg:', msg
                else:
                    d['type'] = 'init'
                    d['async'] = m.group(1) != 'Initaizlied'
                    d['sql'] = m.group(2)
                    d['conn'] = m.group(3)
                    d['stmt'] = m.group(4)
            elif firstWord == 'Native':
                m = RE_NATIVE_ASYNC.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad async native init msg:', msg
                else:
                    d['type'] = 'asyncNative'
                    d['sql'] = m.group(3)
                    d['conn'] = m.group(1)
                    d['stmt'] = m.group(2)
                    d['native'] = m.group(4)
            # Think this is now TRACE_STMT, XXX remove
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
            # Think this is now TRACE_TIME, XXX remove
            elif firstWord == 'sqlite3_profile':
                m = RE_PROFILE.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad exec msg:', msg
                else:
                    d['type'] = 'profile'
                    d['sql'] = m.group(3)
                    d['conn'] = m.group(1)
                    d['durationMS'] = float(m.group(2)) / 1000000.0
            elif firstWord == 'Resetting':
                m = RE_RESET.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad reset msg:', msg
                else:
                    d['type'] = 'reset'
                    d['sql'] = m.group(1)
                    d['conn'] = m.group(2)
                    d['stmt'] = m.group(3)
            elif firstWord == 'Finalizing' or firstWord == 'Auto-finalizing':
                m = RE_FINALIZE.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad finalize msg:', repr(msg)
                else:
                    d['type'] = 'finalize'
                    d['conn'] = m.group(4)
                    d['stmt'] = m.group(5)
                    d['sql'] = m.group(2)
                    d['gc'] = m.group(3) and True or False
            elif firstWord == 'TRACE_STMT':
                m = RE_TRACE_STMT.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad TRACE_STMT msg:', repr(msg)
                else:
                    d['type'] = 'exec'
                    d['conn'] = m.group(1)
                    d['sql'] = m.group(2)
            elif firstWord == 'TRACE_TIME':
                m = RE_TRACE_STMT.match(msg)
                if not m:
                    d['type'] = 'bad'
                    if VERBOSE:
                        print 'Sad TRACE_TIME msg:', repr(msg)
                else:
                    d['type'] = 'profile'
                    d['conn'] = m.group(1)
                    d['durationMS'] = float(m.group(2))
            else:
                d['type'] = 'unknown'
                if VERBOSE:
                    print 'Weird mozStorage line', msg
            #print d
            yield d

RE_PARAM = re.compile('[:?]')
def normalize_param_sql(sql):
    '''
    Given parameterized SQL, create a normalized-ish version that's suitable
    for using against populated sql using startswith.
    '''
    m = RE_PARAM.search(sql)
    if m:
        return sql[:m.start(0)]
    return sql

class StorageLogChewer(object):
    def __init__(self):
        # all connection sessions
        self.conn_sessions = []

    def filter_conn_sessions_by_path(self, filter_path):
        filter_path = os.path.normcase(os.path.abspath(filter_path))
        def checkit(cinfo):
            normed = os.path.normcase(os.path.abspath(cinfo['filename']))
            return normed == filter_path
        return filter(checkit, self.conn_sessions)

    def chew(self, parseGen):
        '''
        Consume a StorageLogParser's generator, aggregating information by
        connection pointer at the top level and summarizing executed statements
        within that.

        The most useful thing we do is (within a connection), we aggregate
        statements based on their unbound SQL.  We then hang the specific bound
        SQL executions off of that.  We can use the bound SQL for EXPLAINing
        without going crazy explaining a ton of things.  (Noting that if ANALYZE
        is used so the optimizer has more to go on, it may be appropriate to
        EXPLAIN everything and de-duplicate its output.)

        NOTE: Our "unique" values are pointers, which can inherently be reused
        once their lifecycle is over.
        '''
        # Maps live/active connection id's (the hex pointer strings) to our
        # connection info aggregate.  (see make_conn_info)
        conns_by_id = {}
        # Map connection ids to a list of presumed actively executing
        # statements.  Statements get added by 'exec' and removed by 'profile'.
        # We use a list because virtual tables (and maybe other things?) mean
        # that we can potentially have multiple SQL statements "in flight" on
        # a connection at the same time.
        active_statements_by_conn = {}

        def find_active_statement_by_param_sql(actives, param_sql):
            norm_sql = normalize_param_sql(param_sql)
            # (it's been a while... probably the wrong idiom?)
            for i in xrange(len(actives) - 1, 0, -1):
                active = actives[i]
                populated_sql = active['sql']
                #print 'CHECK', populated_sql.startswith(norm_sql), '!!!', norm_sql, '|||', populated_sql
                if populated_sql.startswith(norm_sql):
                    return actives.pop(i)
            return None

        def make_conn_info(connId, filename, d):
            '''helper that exists because we have to infer memory dbs'''
            cinfo = conns_by_id[connId] = OrderedDict()
            cinfo['filename'] = filename
            cinfo['id'] = connId
            # all logged entries for this conn.
            cinfo['entries'] = []
            cinfo['execs'] = []
            cinfo['uniqueExecs'] = {}
            self.conn_sessions.append(cinfo)
            return cinfo

        for d in parseGen:
            dtype = d['type']

            connId = d.get('conn')
            if connId:
                cinfo = conns_by_id.get(connId)
                if cinfo is None:
                    cinfo = make_conn_info(connId, d['filename'], d)
                cinfo['entries'].append(d)
            else:
                connId = None
                cinfo = None

            if dtype == 'open':
                # Nothing interesting to do for open since we're already
                # implicitly doing everything.
                pass
            elif dtype == 'close':
                # It's no longer alive after this!  (Although it wouldn't
                # be surprising for complicated threading/life-cycle things
                # to result in some spurious re-inferring of a live connection.
                del conns_by_id[connId]
            elif dtype == 'init':
                # The init pointer is describing the pointer of the statement
                # right now, so this is entirely useless.  We can improve things
                # by fixing the mozStorage logging.
                pass
            elif dtype == 'exec':
                # exec/profile are keyed only by connection
                active_statements = active_statements_by_conn.get(connId)
                if active_statements is None:
                    active_statements = active_statements_by_conn[connId] = []
                active_statements.append(d)
                # avoid unbounded growth of this tracking
                if len(active_statements) > 10:
                    active_statements.pop(0)
            elif dtype == 'profile':
                param_sql = d['sql']
                active_statements = active_statements_by_conn.get(connId)
                if active_statements is None:
                    if VERBOSE:
                        print 'Weird profile mismatch on conn', connId
                    continue
                active = find_active_statement_by_param_sql(active_statements,
                                                            param_sql)
                if active is None:
                    # this is potentially quite likely, so no logging unless
                    # we want extreme logging
                    if PARANOIA:
                        print 'profile without active', d
                    continue

                # update the exec with info on this
                execs = cinfo['execs']
                execInfo = OrderedDict()
                execInfo['unboundSQL'] = param_sql
                execInfo['boundSQL'] = active['sql']
                execInfo['startTS'] = active['ts']
                execInfo['endTS'] = d['ts']
                execInfo['durationMS'] = d['durationMS']
                execs.append(execInfo)
                # and the unique execs (this is what we use for EXPLAIN, mainly)
                uniqueExecs = cinfo['uniqueExecs']
                uniqueExec = uniqueExecs.get(param_sql)
                if uniqueExec is None:
                    uniqueExec = uniqueExecs[param_sql] = OrderedDict()
                    uniqueExec['unboundSQL'] = param_sql
                    uniqueExec['execs'] = []

                uniqueExecInst = OrderedDict()
                uniqueExecInst['boundSQL'] = active['sql']
                uniqueExecInst['startTS'] = active['ts']
                uniqueExecInst['endTS'] = d['ts']
                uniqueExecInst['durationMS'] = d['durationMS']

                uniqueExec['execs'].append(uniqueExecInst)

def dump_db_schema(sqlite_path, db_path):
    args = [
        sqlite_path,
        db_path,
        '.schema'
        ]
    pope = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=False)
    pope.stdin.close()
    lines = []
    for line in pope.stdout:
        lines.append(line)
    stderr = pope.stderr.read()
    return lines

def run_explain(sqlite_path, db_path, sql):
    sanitized_sql = sql.replace('\n', ' ')
    args = [
        sqlite_path,
        '-separator', '<|>',
        db_path,
        'EXPLAIN ' + sanitized_sql,
        ]
    pope = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=False)
    pope.stdin.close()
    rows = []
    for line in pope.stdout:
        rows.append(line.rstrip().split('<|>'))
    stderr = pope.stderr.read()
    return rows

class ExplainProcessor(object):
    '''
    Process connection sessions to generate
    '''
    def __init__(self, conn_sessions, out_dir, db_path, sqlite_path):
        self.conn_sessions = conn_sessions
        self.out_dir = out_dir
        self.db_path = db_path
        self.sqlite_path = sqlite_path

        self.next_id = 1
        self.explanations = []
        self.already_explained = set()

    def processSession(self, cinfo, sg):
        explanations = self.explanations
        already_explained = self.already_explained
        for uexec in cinfo['uniqueExecs'].itervalues():
            unbound_sql = uexec['unboundSQL']
            if unbound_sql in already_explained:
                continue
            already_explained.add(unbound_sql)

            first_exec = uexec['execs'][0]
            first_bound_sql = first_exec['boundSQL']
            rows = run_explain(self.sqlite_path, self.db_path, first_bound_sql)

            use_id = self.next_id
            self.next_id += 1
            explain_info = OrderedDict()
            explain_info['id'] = use_id
            explain_info['unboundSQL'] = unbound_sql
            explain_info['usedBoundSQL'] = first_bound_sql
            explain_info['rows'] = rows
            explanations.append(explain_info)

            eg = grokexplain.ExplainGrokker()
            eg.parseExplainStringRows(rows, sg)
            eg.performFlow()

            # it's possible this was a super-boring thing, in which case we
            #

            filename_prefix = 'blocks-%d' % (use_id,)
            grokexplain.output_blocks(eg, first_bound_sql, self.out_dir,
                                      filename_prefix)
            explain_info['dot'] = filename_prefix + '.dot'
            explain_info['png'] = filename_prefix + '.png'



    def processAll(self):
        # Since we have the database available, we can automatically dump the
        # schema.
        sg = grokexplain.SchemaGrokker()
        sg.grok(dump_db_schema(self.sqlite_path, self.db_path))

        print repr(sg.tables)

        for session in self.conn_sessions:
            self.processSession(session, sg)


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

        # This is a sucky way to handle actions, but until we have more stuff
        # to do, it works.  If omitted we're just going to dump the structured
        # JSON output to stdout.
        parser.add_option('--explain',
                          action='store_true', dest='do_explain',
                          default=False,
                          help='Automatically EXPLAIN unique SQL statements')

        parser.add_option('-o', '--output-dir',
                          dest='out_dir', default='/tmp/explained',
                          help='Directory to output results in.')

        parser.add_option('--sqlite',
                          dest='sqlite', default=None,
                          help='SQLite executable to use, preferably debug')

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

        if options.sqlite:
            sqlite_path = options.sqlite
        else:
            # How the author rolls...
            sqlite_path = os.path.expanduser('~/bin/sqlite3')
            if not os.path.exists(sqlite_path):
                # How suckers roll... (system SQLite almost certainly isn't a
                # debug build, unfortunately)
                sqlite_path = '/usr/bin/sqlite3'

        db_path = options.db_path
        if db_path:
            db_path = os.path.abspath(db_path)

        for filename in args:
            parser = StorageLogParser()
            chewer = StorageLogChewer()
            f = open(filename, 'rt')
            chewer.chew(parser.parse(f))
            f.close()

            if options.db_path:
                conn_sessions = chewer.filter_conn_sessions_by_path(db_path)
            else:
                conn_sessions = chewer.conn_sessions

            if options.do_explain:
                eproc = ExplainProcessor(conn_sessions, options.out_dir,
                                         db_path, sqlite_path)
                eproc.processAll()
                explained_path = os.path.join(options.out_dir, 'explained.json')
                explained_file = open(explained_path, 'wt')
                json.dump(eproc.explanations, explained_file, indent=2)
                explained_file.close()
            else:
                print json.dumps(conn_sessions, indent=2)


if __name__ == '__main__':
    cmdline = CmdLine()
    cmdline.run()
