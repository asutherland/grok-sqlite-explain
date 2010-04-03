# Attempt to understand what is happening in a SQLite EXPLAIN-ation.
# Our primary concern is data-flow, not control flow.
#
# Andrew Sutherland <asutherland@asutherland.org>
# Mozilla Messaging, Inc.


import pygraphviz
import cStringIO as StringIO
import os
import optparse

class TableSchema(object):
    '''
    Table meta-data; the name of a table and its columns.
    '''
    def __init__(self, name, colnames):
        self.name = name
        self.columns = colnames
        

class SchemaGrokker(object):
    '''
    Parses a schema dump like "sqlite3 DATABASE .schema" outputs for info
    about tables and virtual tables.
    '''
    def __init__(self):
        self.tables = {}
        self.virtualTables = {}

    def grok(self, file_or_lines):
        for line in file_or_lines:
            if line.startswith('CREATE TABLE'):
                name = line.split(' ')[2]
                if '_' in name:
                    # HACK virtual table fallout detection
                    continue

                insideParens = line[line.find('(')+1:line.rfind(';')-1]
                columnNames = []
                for coldef in insideParens.split(', '):
                    columnNames.append(coldef.split()[0])
                table = TableSchema(name, columnNames)
                self.tables[name] = table
            if line.startswith('CREATE VIRTUAL TABLE'):
                name = line.split(' ')[3]
                if '_' in name:
                    # HACK virtual table fallout detection
                    continue

                insideParens = line[line.find('(')+1:line.rfind(';')-1]
                columnNames = []
                for coldef in insideParens.split(', ')[1:]:
                    columnNames.append(coldef.split()[0])
                columnNames.append('everything?')
                columnNames.append('docid')
                table = TableSchema(name, columnNames)
                self.virtualTables[name] = table


class Table(object):
    def __init__(self, **kwargs):
        self.name = kwargs.pop('name')
        self.columns = kwargs.pop('columns', 0)
        # meh, this should probably be a single mode
        self.ephemeral = kwargs.pop('ephemeral', False)
        self.virtual = kwargs.pop('virtual', False)
        self.pseudo = kwargs.pop('pseudo', False)
        self.openedAt = kwargs.pop('openedAt', None)
        self.closedAt = kwargs.pop('closedAt', None)
        self.schema = kwargs.pop('schema', None)

        if self.schema:
            self.name = self.schema.name

        # a table is just a table
        self.on = None

    def __str__(self):
        return '%s, %d columns' % (
            self.name,
            self.columns)

class Index(object):
    def __init__(self, **kwargs):
        self.on = kwargs.pop('table', None)
        self.name = kwargs.pop('name')
        self.columns = kwargs.pop('columns')
        self.openedAt = kwargs.pop('openedAt', None)
        self.closedAt = kwargs.pop('closedAt', None)
        self.schema = kwargs.pop('schema', None)

    def __str__(self):
        return 'Index on [%s], %d columns' % (self.on, self.columns,)

class Cursor(object):
    def __init__(self, **kwargs):
        self.handle = kwargs.pop('handle')
        # the goal of id is to be unique for the entire body.  of course,
        #  we can't guarantee this, so we just copy it over.  but hopefully
        #  we keep the concept and its usage distinct...
        self.id = self.handle
        self.on = kwargs.pop('on')
        self.writable = kwargs.pop('writable', True)
        self.openedAt = kwargs.pop('openedAt', None)
        self.closedAt = kwargs.pop('closedAt', None)

        self.writesAffectedBy = set()
        self.seeksAffectedBy = set()

    def __str__(self):
        return 'Cursor %d on %s' % (self.handle, self.on,)

class RegStates(object):
    def __init__(self, copyFrom=None):
        #: maps register number to a set of cursors that have impacted this reg
        self.regCursorImpacts = {}
        #: maps register number to a set of values that this register may
        #:  contain
        self.regValues = {}
        if copyFrom:
            for reg, cursors in copyFrom.regCursorImpacts.items():
                self.regCursorImpacts[reg] = cursors.copy()
                if VERBOSE:
                    print ' copying cursors', reg, cursors
            for reg, values in copyFrom.regValues.items():
                if VERBOSE:
                    print ' copying values', reg, values
                self.regValues[reg] = values.copy()

    def getRegCursorImpacts(self, reg):
        # I think there is defaultdict stuff we could use now...
        if reg in self.regCursorImpacts:
            return self.regCursorImpacts[reg]
        else:
            return set()

    def setRegCursorImpacts(self, reg, cursors):
        self.regCursorImpacts[reg] = cursors.copy()

    def addRegValue(self, reg, value):
        if reg in self.regValues:
            values = self.regValues[reg]
        else:
            values = self.regValues[reg] = set()
        alreadyPresent = value in values
        values.add(value)
        return not alreadyPresent

    def setRegValue(self, reg, value):
        self.regValues[reg] = set([value])

    def getRegValues(self, reg):
        if reg in self.regValues:
            return self.regValues[reg]
        else:
            return set()

    def checkDelta(self, other):
        # -- cursor impact
        # update regs the other guy also has
        for reg, cursors in self.regCursorImpacts.items():
            if reg in other.regCursorImpacts:
                # difference if they don't match
                if cursors != other.regCursorImpacts[reg]:
                    return True
            else: # difference if he didn't have it
                return True
        # different if he has something we don't have
        for reg, cursors in other.regCursorImpacts.items():
            if not reg in self.regCursorImpacts:
                return True

        # -- values
        for reg, values in self.regValues.items():
            if reg in other.regValues:
                # diff if they don't match
                if values != other.regValues[reg]:
                    return True
            else: # difference if he didn't have it
                return True
        # different if he has something we don't have
        for reg, values in other.regValues.items():
            if not reg in self.regValues:
                return True

    def copy(self):
        return RegStates(self)

    def update(self, other):
        '''
        Update the states of our registers with the states of the registers in
        the other RegStates object.
        '''
        # -- cursor impact
        # update regs the other guy also has
        for reg, cursors in self.regCursorImpacts.items():
            # this sorta defeats our getitem magic...
            if reg in other.regCursorImpacts:
                cursors.update(other.regCursorImpacts[reg])
        # copy regs we don't have (but the other guy does)
        for reg, cursors in other.regCursorImpacts.items():
            if not reg in self.regCursorImpacts:
                self.regCursorImpacts[reg] = cursors.copy()

        # -- values
        for reg, values in self.regValues.items():
            # this sorta defeats our getitem magic...
            if reg in other.regValues:
                values.update(other.regValues[reg])
        # copy regs we don't have (but the other guy does)
        for reg, values in other.regValues.items():
            if not reg in self.regValues:
                self.regValues[reg] = values.copy()

    def __str__(self):
        return '\n  cursor impacts: %s\n  values: %s' % (
            repr(self.regCursorImpacts), repr(self.regValues))

    def graphStr(self):
        s = ''
        for reg, values in self.regValues.items():
            s += ' r%s: %s' % (reg, values)
        return s

class BasicBlock(object):
    def __init__(self, ops):
        self.ops = ops
        self.inRegs = RegStates()
        self.outRegs = RegStates()
        self.done = False

        # establish a back-link for us lazy-types
        for op in self.ops:
            op.block = self

    @property
    def id(self):
        return self.ops[0].addr

    @property
    def lastAddr(self):
        return self.ops[-1].addr

    @property
    def comeFrom(self):
        return self.ops[0].comeFrom

    @property
    def goTo(self):
        lastOp = self.ops[-1]
        # if we have explicit goto's, use them
        if lastOp.goTo:
            return lastOp.goTo
        # otherwise assume we should flow to the next guy if we're non-terminal
        if lastOp.terminate:
            return []
        else:
            return [self.ops[-1].addr + 1]

    def __str__(self):
        return 'Block id: %d last: %d comeFrom: %s goTo: %s' % (
            self.id, self.lastAddr, self.comeFrom, self.goTo)

class GenericOpInfo(object):
    '''
    Simple op meta-info.
    '''
    def __init__(self, addr, name, params, comment):
        self.addr = addr
        self.name = name
        self.params = params
        self.comment = comment

        #: opcode addresses that may jump/flow to this opcode
        self.comeFrom = []
        #: opcode addresses that we may transfer control to (jump or next)
        self.goTo = []
        #: database handle-y things created by this opcode
        self.births = []
        #: database handle-y things closed by this opcode
        self.kills = []

        #: register numbers that we (may) read from
        self.regReads = []
        #: register numbers that we (may) write to
        self.regWrites = []
        #: the immediate value this opcode uses (if it uses one).
        #:  We can't represent some immediates, so this may just be true
        self.usesImmediate = None
        #: the Cursor this opcode uses for read purposes, if any
        self.usesCursor = None
        #: the Cursor this opcode uses for write purposes, if any. implies uses
        self.writesCursor = None
        #: the Cursor this opcode performs a seek operation on
        self.seeksCursor = None
        #: the column numbers (on the cursor) this op accesses
        self.usesColumns = None
        #: does this opcode terminate program execution
        self.terminate = False
        #: does this opcode engage in dynamic jumps (and as such we should hint
        #:  to the basic block engine to create a block)
        self.dynamicGoTo = False
        #: used by Gosub to track the register it writes to
        self.dynamicWritePC = None

        self.affectedByCursors = set()

    def ensureJumpTargets(self, addrs, adjustment=1):
        '''
        Make sure this opcode knows that it can jump to the given addresses
        (probably with an adjustment).  Returns the set of targets that were
        unknown.
        '''
        unknown = set()
        for addr in addrs:
            realAddr = addr + adjustment
            if realAddr not in self.goTo:
                self.goTo.append(realAddr)
                unknown.add(realAddr)
        return unknown

    HIGHLIGHT_OPS = {'Yield': '#ff00ff',
                     'Gosub': '#ff00ff',
                     'Return': '#ff00ff'}
    def graphStr(self, schemaInfo):
        if self.usesCursor:
            s = "<font color='%s'>%d %s [%d]</font>" % (
                self.usesCursor.color, self.addr, self.name,
                self.usesCursor.handle)
        elif self.name in self.HIGHLIGHT_OPS:
            s = "%d <font color='%s'>%s</font>" % (
                self.addr, self.HIGHLIGHT_OPS[self.name], self.name)
        else:
            s = '%d %s' % (self.addr, self.name)

        if self.affectedByCursors:
            cursors = list(self.affectedByCursors)
            cursors.sort(lambda a, b: a.handle - b.handle)
            cursorStrings = []
            for cursor in cursors:
                if cursor == self.usesCursor:
                    continue
                cursorStrings.append(
                    "<font color='%s'>%d</font>" % (
                        cursor.color, cursor.handle))
            if cursorStrings:
                s += ' (' + ' '.join(cursorStrings) + ')'

        if self.usesCursor and self.births:
            if self.usesCursor in self.births:
                s += ' %s' % (self.usesCursor.on.name,)

        if self.usesImmediate is not None:
            s += ' imm %s' % (self.usesImmediate)

        if self.usesColumns:
            schema = self.usesCursor.on.schema
            if schema:
                colNames = []
                for colNum in self.usesColumns:
                    colNames.append(schema.columns[colNum])
                s += ' col %s' % (', '.join(colNames))

        if self.regReads:
            s += ' using r%s' % (', r'.join(map(str, self.regReads)),)
        if self.regWrites:
            s += ' to r%s' % (', r'.join(map(str, self.regWrites)),)

        if self.comment:
            s += " <font color='#888888'>%s</font>" % (self.comment,)

        return s

    def dump(self):
        if self.comeFrom:
            print '  ', self.comeFrom, '---->'
        print '%d %s' % (self.addr, self.name),
        print '   reads: %s writes: %s' % (self.regReads, self.regWrites)
        if self.goTo:
            print '   ---->', self.goTo

class ExplainGrokker(object):
    def __init__(self):
        self.ephemeralTables = []
        self.virtualTables = []
        #: maps "vtab:*:*" strings to Table instances...
        self.vtableObjs = {}
        self.realTables = []
        self.realTablesByName = {}
        self.pseudoTables = []

        self.allTables = []

        self.indices = []
        self.cursors = []
        self.code = []
        self.cursorByHandle = {}

        self.resultRowOps = []

    def _newEphemeralTable(self, **kwargs):
        table = Table(ephemeral=True, openedAt=self.op, **kwargs)
        self.ephemeralTables.append(table)
        self.allTables.append(table)
        self.op.births.append(table)
        return table

    def _newVirtualTable(self, vtabkey, **kwargs):
        table = Table(virtual=True, openedAt=self.op, **kwargs)
        self.vtableObjs[vtabkey] = table
        self.virtualTables.append(table)
        self.allTables.append(table)
        self.op.births.append(table)
        return table

    def _newRealTable(self, name, **kwargs):
        if name in self.realTablesByName:
            table = self.realTablesByName[name]
        else:
            table = Table(name=name, openedAt=self.op, **kwargs)
            self.realTables.append(table)
            self.realTablesByName[name] = table
            self.allTables.append(table)
        self.op.births.append(table)
        return table

    def _newPseudoTable(self, **kwargs):
        table = Table(pseudo=True,openedAt=self.op, **kwargs)
        self.pseudoTables.append(table)
        self.allTables.append(table)
        self.op.births.append(table)
        return table

    def _parseKeyinfo(self, indexDetails):
        keyparts = indexDetails[8:-1].split(',')
        numColumns = int(keyparts[0])
        return numColumns

    def _newIndexOn(self, indexDetails, **kwargs):
        # indexDetails is of the form: "keyinfo(%d,"...
        numColumns = self._parseKeyinfo(indexDetails)
        index = Index(columns=numColumns, openedAt=self.op,
                      **kwargs)
        self.indices.append(index)
        self.op.births.append(index)
        return index

    def _newCursor(self, handle, thing, **kwargs):
        if handle in self.cursorByHandle:
            # R/W change is okay
            cursor = self.cursorByHandle[handle]
            if cursor.on != thing:
                raise Exception('ERROR! Cursor handle collision!')
        else:
            cursor = Cursor(handle=handle, on=thing, openedAt=self.op, **kwargs)
            self.cursors.append(cursor)
            self.cursorByHandle[handle] = cursor
        self.op.births.append(cursor)
        self.op.usesCursor = cursor
        self.op.affectedByCursors.add(cursor)
        return cursor

    def _getCursor(self, handle, write=False, seek=False):
        cursor = self.cursorByHandle[handle]
        self.op.usesCursor = cursor
        self.op.writesCursor = write
        self.op.seeksCursor = seek
        self.op.affectedByCursors.add(cursor)
        return cursor

    def _getVtable(self, vtabkey, write=False):
        '''
        Given a P4-resident "vtab:*:*" 
        '''
        if vtabkey in self.cursorByHandle:
            cursor = self.cursorByHandle[vtabkey]
        else:
            cursor = Cursor(handle=vtabkey, on=None)
            self.cursorByHandle[vtabkey] = cursor
            
        self.op.usesCursor = cursor
        self.op.writesCursor = write
        self.op.affectedByCursors.add(cursor)
        return cursor
        

    def _killThing(self, thing):
        self.op.kills.append(thing)
        thing.closedAt = self.op

        if thing.on:
            self._killThing(thing.on)

    def _killCursor(self, handle):
        if handle not in self.cursorByHandle:
            print 'Warning; tried to close a non-open cursor; might be our bad'
            return
        cursor = self._getCursor(handle)
        self._killThing(cursor)

    def _op_OpenCommon(self, params, writable):
        # if P4 is a keyinfo, then it's an index and comment is the name of the
        #  index.
        # if P4 is not a keyinfo, then it's the number of columns in the table
        #  and comment is the name of the table.
        cursorNum = params[0]

        if isinstance(params[3], basestring):
            indexDetails = params[3]
            cursorOn = self._newIndexOn(indexDetails,
                                        name=self.op.comment)
        else:
            cursorOn = self._newRealTable(self.op.comment,
                                          columns=params[3])

        self._newCursor(cursorNum, cursorOn, writable=writable)

    def _op_OpenRead(self, params):
        self._op_OpenCommon(params, False)

    def _op_OpenWrite(self, params):
        self._op_OpenCommon(params, True)

    def _op_OpenPseudo(self, params):
        cursorNum = params[0]
        table = self._newPseudoTable(
            name=("pseudo%d" % (cursorNum,)),
            columns=self.nextOpInfo)
        self._newCursor(cursorNum, table, copy=params[1]==0)

    def _op_VOpen(self, params):
        # p1: cursor number
        # p4: vtable structure
        cursorNum = params[0]
        table = self._newVirtualTable(params[3],
            name=("virtual%d" % (cursorNum,)))
        self._newCursor(cursorNum, table)

    def _op_OpenEphemeral(self, params):
        cursorNum = params[0]
        numColumns = params[1]
        indexDetails = params[3]
        table = self._newEphemeralTable(
            name=("ephemeral%d" % (cursorNum,)),
            columns=numColumns)
        if indexDetails:
            cursorOn = self._newIndexOn(indexDetails,
                                        table=table,
                                        name="eindex%d" % (cursorNum,))
        else:
            cursorOn = table
        self._newCursor(cursorNum, cursorOn)

    def _op_Permute(self, params):
        # it just prints "intarray" at least for non-debug.  not very helpful!
        pass

    def _op_Compare(self, params):
        # P1..(P1+P3-1)
        self.op.regReads.extend([params[0] + x for x in range(params[2])])
        # P2..(P2+P3-1)
        self.op.regReads.extend([params[1] + x for x in range(params[2])])
        # uh, we don't use this yet.
        self._parseKeyinfo(params[3])
        # we contaminate the jump decision...
        self.op.regWrites.append('for_jump')

    def _condJump(self, regs, target):
        if regs:
            if isinstance(regs, list):
                self.op.regReads.extend(regs)
            else:
                self.op.regReads.append(regs)
        self.op.goTo.append(self.op.addr + 1)
        self.op.goTo.append(target)
        self.op.usesImmediate = target

    def _jump(self, target):
        self.op.goTo.append(target)
        self.op.usesImmediate = target

    def _op_Goto(self, params):
        self._jump(params[1])

    def _op_Jump(self, params):
        # we base our decision on the result of the last compare
        self.op.regReads.append('for_jump')
        self._jump(params[0])
        self._jump(params[1])
        self._jump(params[2])
        self.op.usesImmediate = None # too many for now... XXX

    def _op_Gosub(self, params):
        self.op.regWrites.append(params[0])
        self.op.dynamicWritePC = params[0]
        self._jump(params[1])
        if NO_YIELDS:
            self.op.goTo.append(self.op.addr + 1)

    def _op_Yield(self, params):
        self.op.regReads.append(params[0])
        self.op.regWrites.append(params[0])
        if not NO_YIELDS:
            self.op.dynamicWritePC = params[0]
            # we won't know where out goTo goes to until dataflow analysis, nor
            #  where we would 'come from' to the next opcode.  But we do know that
            #  after us is a basic block break, so let's hint that.
            self.op.dynamicGoTo = params[0]
            # do not arbitrarily flow to the next dude!
            self.op.terminate = True

    def _op_Return(self, params):
        # just like for Yield, we have no idea where we are going until
        #  dataflow.
        self.op.regReads.append(params[0])
        self.op.dynamicGoTo = params[0]

    def _op_NullRow(self, params):
        # moves us to a no-op row
        self._getCursor(params[0], False, True)

    def _op_Seek(self, params):
        self._getCursor(params[0], False, True)
        self.op.regReads.append(params[1])

    def _op_SeekCommon(self, params, comparison):
        cursor = self._getCursor(params[0], False, True)
        if isinstance(cursor.on, Table):
            self.op.regReads.append(params[2])
        else:
            for x in range(params[3]):
                self.op.regReads.append(params[2] + x)
        if params[1]:
            self._condJump(None, params[1])

    def _op_SeekLt(self, params):
        self._op_SeekCommon(params, '<')
    def _op_SeekLe(self, params):
        self._op_SeekCommon(params, '<=')
    def _op_SeekGe(self, params):
        self._op_SeekCommon(params, '>=')
    def _op_SeekGt(self, params):
        self._op_SeekCommon(params, '>')

    def _op_IdxCommon(self, params, comparison):
        self._getCursor(params[0])
        indexKey_regs = [params[2] + x for x in range(params[3])]
        self._condJump(indexKey_regs, params[1])
    
    def _op_IdxLT(self, params):
        self._op_IdxCommon(params, '<')
    def _op_IdxGE(self, params):
        self._op_IdxCommon(params, '>=')
    
    def _op_IdxRowid(self, params):
        self._getCursor(params[0])
        self.op.regWrites.append(params[1])
    def _op_Rowid(self, params):
        self._op_IdxRowid(params)
    def _op_NewRowid(self, params):
        self._getCursor(params[0])
        self.op.regReads.append(params[2])
        self.op.regWrites.extend(params[1:3])

    def _op_RowSetAdd(self, params):
        # reg[p2] => reg[p1]
        self.op.regReads.append(params[1])
        self.op.regWrites.append(params[0])

    def _op_RowSetRead(self, params):
        # extract from reg[p1] => reg[p3], or conditional jump to p2
        self._condJump(params[0], params[1])
        self.op.regWrites.append(params[2])

    def _op_NotExists(self, params):
        self._getCursor(params[0], False, True)
        self._condJump(params[2], params[1])

    def _op_Found(self, params):
        self._getCursor(params[0], False, True)
        self._condJump(params[2], params[1])
    def _op_NotFound(self, params):
        self._op_Found(params)

    def _op_VBegin(self, params):
        # This is used in conjunction with VUpdate without any VOpen.  Although
        # there is no real cursor that results from this, the usage is akin to a
        # cursor...
        self._getVtable(params[3])

    def _op_VUpdate(self, params):
        # performs virtual table INSERT and/or DELETE
        # p1 is whether we should update the last_insert_rowid value
        # p2 is the number of arguments
        # p3 is the register index of the first argument (of which we have p2
        #  arguments)
        # p4 is the vtable we are operating on ("vtab:*:*")
        self.op.regReads.extend([params[2] + x for x in range(params[1])])
        self._getVtable(params[3], True)

    def _op_VFilter(self, params):
        self._getCursor(params[0], False, True)
        # +1 is actually argc, which we can't see with a bit'o'legwork
        # TODOMAYBE: fancy legwork if we can statically know the argc
        self.op.regReads.extend([params[2], params[2]+1,
                                 # however, we do know it must be >= 1
                                 params[2] + 2])
        self._condJump(None, params[1])

    def _op_VNext(self, params):
        self._getCursor(params[0], False, True)
        self._condJump(None, params[1])
    def _op_Next(self, params):
        self._op_VNext(params)
    def _op_Prev(self, params):
        self._op_VNext(params)

    def _op_Last(self, params):
        self._getCursor(params[0], False, True)
        if params[1]:
            self._condJump(None, params[1])
    def _op_Rewind(self, params):
        self._op_Last(params)
    _op_Sort = _op_Rewind

    def _op_Column(self, params):
        self._getCursor(params[0])
        self.op.usesColumns = [params[1]]
        self.op.regWrites.append(params[2])
    def _op_VColumn(self, params):
        self._op_Column(params)

    def _op_Affinity(self, params):
        '''
        This sets the data type of a bunch of registers...
        Treating this as a nop since it doesn't really do anything.
        '''
        pass

    def _op_MakeRecord(self, params):
        self.op.regReads.extend([params[0] + x for x in range(params[1])])
        # writes to reg p3
        self.op.regWrites.append(params[2])

    def _op_ResultRow(self, params):
        self.op.regReads.extend([params[0] + x for x in range(params[1])])
        self.resultRowOps.append(self.op)

    def _op_Insert(self, params):
        # P4 is the table...
        self._getCursor(params[0], True)
        self.op.regReads.extend([params[1], params[2]])

    def _op_IdxInsert(self, params):
        self._getCursor(params[0], True)
        self.op.regReads.append(params[1])

    def _op_Delete(self, params):
        # a delete is a write...
        self._getCursor(params[0], True)

    def _op_IdxDelete(self, params):
        # delete from cursor P1
        # index key is packed into p2...p2+p3-1p
        self._getCursor(params[0], True)
        self.op.regReads.extend([params[1] + x for x in range(params[2])])

    def _op_Sequence(self, params):
        self._getCursor(params[0])
        self.op.regWrites.append(params[1])

    def _op_Close(self, params):
        self._killCursor(params[0])

    def _op_IsNull(self, params):
        if params[2]:
            regs = [params[0] + x for x in range(params[2])]
        else:
            regs = params[0]
        self._condJump(regs, params[1])

    def _op_NotNull(self, params):
        self._condJump([params[0]], params[1])

    def _op_MustBeInt(self, params):
        self.op.regReads.append(params[0])
        self.op.goTo.append(self.op.addr + 1)
        if params[1]:
            self.op.goTo.append(params[1])
        else:
            # you know what?  we don't care about exceptions.  screw them.
            #self.op.goTo.append('SQLITE_MISMATCH')
            pass

    def _op_If(self, params):
        self._condJump([params[0]], params[1])
    def _op_IfNot(self, params):
        self._condJump([params[0]], params[1])
    def _op_IfPos(self, params):
        self._condJump([params[0]], params[1])

    def _op_Eq(self, params):
        self._condJump([params[0], params[2]], params[1])
    def _op_Ne(self, params):
        self._condJump([params[0], params[2]], params[1])
    def _op_Lt(self, params):
        self._condJump([params[0], params[2]], params[1])
    def _op_Le(self, params):
        self._condJump([params[0], params[2]], params[1])
    def _op_Gt(self, params):
        self._condJump([params[0], params[2]], params[1])
    def _op_Ge(self, params):
        self._condJump([params[0], params[2]], params[1])

    def _op_IfZero(self, params):
        self._condJump([params[0]], params[1])

    def _op_Variable(self, params):
        # bound parameters P1..P1+P3-1 transferred to regs P2...P2+P3-1
        # when P3==1, P4 should hold the name.
        self.op.regWrites.extend([params[1] + x for x in range(params[2])])
        if params[2] == 1 and params[3]:
            # just put the binding name in the commment; it visually works
            self.op.comment = params[3]

    def _op_Move(self, params):
        self.op.regReads.extend([params[0] + x for x in range(params[2])])
        self.op.regWrites.extend([params[1] + x for x in range(params[2])])

    def _op_Copy(self, params, shallow=False):
        self.op.regReads.append(params[0])
        self.op.regWrites.append(params[1])

    def _op_SCopy(self, params):
        self._op_Copy(params, True)

    def _op_AddImm(self, params):
        self.op.regReads.append(params[0])
        self.op.regWrites.append(params[0])
        self.op.usesImmediate = params[1]

    def _op_String(self, params):
        self.op.regWrites.append(params[1])
        self.op.usesImmediate = params[3]

    def _op_String8(self, params):
        self._op_String(params)

    def _op_Integer(self, params):
        self.op.regWrites.append(params[1])
        self.op.usesImmediate = params[0]

    def _op_Int64(self, params):
        self.op.regWrites.append(params[1])
        self.op.usesImmediate = params[3]

    def _op_Blob(self, params):
        self.op.regWrites.append(params[1])
        self.op.usesImmediate = '(blob)'

    def _op_Null(self, params):
        self.op.regWrites.append(params[1])

    def _op_Halt(self, params):
        self.op.terminate = True

    def _op_HaltIfNull(self, params):
        # halts if reg[P3] is null.
        self.op.regReads.append(params[2])

    def _op_VerifyCookie(self, params):
        # schema delta check.  no one cares.
        pass

    def _op_Noop(self, params):
        # used as a jump target apparently
        pass

    def _op_TableLock(self, params):
        # Table locks are boring
        pass

    def _op_Transaction(self, params):
        # Transactions are currently boring.
        pass

    def _op_Trace(self, params):
        # Trace is used for EXPLAIN and is a no-op.
        pass

    def parseExplainTextFile(self, file_or_lines, schemaInfo=None):
        '''
        Process the text-formatted results of an EXPLAIN query like what would
        would get from invoking "sqlite DATABASE 'EXPLAIN ...'".
        '''
        self.schemaInfo = schemaInfo or SchemaGrokker()

        def chewParams(params):
            params[0] = int(params[0])
            params[1] = int(params[1])
            params[2] = int(params[2])
            if params[3] == '':
                pass
            elif params[3] == 'NULL':
                params[3] = None
            elif params[3].isdigit():
                params[3] = int(params[3])
            return params

        for line in file_or_lines:
            bits = line.split('|')
            addr = int(bits[0])
            opcode = bits[1]
            params = chewParams(bits[2:7])
            comment = bits[7].strip()

            # opcode renaming compensation...
            if opcode.startswith('Move') and len(opcode) > 4:
                opcode = opcode.replace('Move', 'Seek')

            self.op = GenericOpInfo(addr, opcode, params, comment)
            self.code.append(self.op)

            handler = getattr(self, "_op_" + opcode, None)
            if handler:
                handler(params)
            else:
                print 'Ignoring opcode', opcode

    
    def parseJsonOpcodesList(self, opcodeList):
        '''
        Process a python list where each entry in the list is itself a list
        representing a row of EXPLAIN output.  The columns are then:
        [addr (int), opcode (string), p1 (int), p2 (int), p3 (int), p4 (string),
        p5 (int), comment (string or None)].

        In a SQLite build built with -DDEBUG, the comment will be the name of
        the column where appropriate.  This eliminates the need to have
        metadata available.
        '''
        self.schemaInfo = SchemaGrokker()

        for row in opcodeList:
            addr = row[0]
            opcode = row[1]
            params = row[2:7] # p1 - p5
            comment = row[7]

            if params[3].isdigit():
                params[3] = int(params[3])

            # opcode renaming compensation...
            if opcode.startswith('Move') and len(opcode) > 4:
                opcode = opcode.replace('Move', 'Seek')

            self.op = GenericOpInfo(addr, opcode, params, comment)
            self.code.append(self.op)

            handler = getattr(self, "_op_" + opcode, None)
            if handler:
                handler(params)
            else:
                print 'Ignoring opcode', opcode
            

    def performFlow(self):
        needReflow = True
        while needReflow:
            print '--- dataflow pass happening ---'
            self.figureBasicBlocks()
            needReflow = self.dataFlow()

    def figureBasicBlocks(self):
        # build comeFrom links
        for op in self.code:
            for addr in op.goTo:
                # ignore exception magic
                if isinstance(addr, basestring):
                    continue
                if VERBOSE:
                    print 'at op', op.addr, 'goTo', addr
                targ_op = self.code[addr]
                if op.addr not in targ_op.comeFrom:
                    targ_op.comeFrom.append(op.addr)
                elif VERBOSE:
                    print 'not adding?'

        self.basicBlocks = {}
        self.basicBlocksByEnd = {}

        # build the blocks
        block_ops = []
        for op in self.code:
            # if we come from somewhere, then we start a new block (and create
            #  a basic block for any opcodes queued in block_ops)
            if op.comeFrom:
                if block_ops:
                    block = BasicBlock(block_ops)
                    if VERBOSE:
                        print 'new', block
                    self.basicBlocks[block.id] = block
                    self.basicBlocksByEnd[block.lastAddr] = block
                block_ops = []
            block_ops.append(op)
            # if this op jumps places, then close out this set of block_ops
            #  into a basic block.
            if op.goTo or op.dynamicGoTo != False:
                block = BasicBlock(block_ops)
                if VERBOSE:
                    print 'new', block
                self.basicBlocks[block.id] = block
                self.basicBlocksByEnd[block.lastAddr] = block
                block_ops = []

    def colorCursors(self):
        TANGO_COLORS = ['#73d216', '#3465a4', '#75507b', '#cc0000',
                                   '#f57900', '#c17d11', '#cc00cc',
                        '#4e9a06', '#204a87', '#5c3566', '#a40000',
                        '#c4a000', '#ce5c00', '#8f5902']
        # could do the hue thing...
        for i, cursor in enumerate(self.cursors):
            cursor.color = TANGO_COLORS[i % len(TANGO_COLORS)]

    def _graphvizRenderHelper(self, g, outpath):
        '''
        pygraphviz does not support HTML strings, which precludes our making
        pretty pretty labels directly.  So, we horribly write the dot file
        to disk then munge it to have HTML strings in it.  Brilliant, no?

        Your labels that should be HTML labels should be bracketed in << and
        >>.
        '''
        f = open(outpath, 'w')
        g.write(f)
        f.close()

        f = open(outpath, 'r')
        #tmpf.seek(0, 0)
        buf = f.read()
        #buf = buf.replace('\\\n>', '>\\\n').replace('\\\n"', '"\\\n')
        buf = buf.replace('\\\n', '')
        buf = buf.replace('"<<', '<').replace('>>"', '>').replace('\\n', '<br align="left"/>')
        buf = buf.replace('\\\\\nn', '\\\n<br align="left"/>')
        f.close()
        f = open(outpath, 'w')
        f.write(buf)
        f.close()

    def diagBasicBlocks(self, outpath):
        self.colorCursors()

        g = pygraphviz.AGraph(directed=True, strict=False)
        for block in self.basicBlocks.values():
            ltext = ("<<" +
              (DEBUG and (block.inRegs.graphStr() + '\\n') or '') +
              '\\n'.join(
                [op.graphStr(self.schemaInfo) for op in block.ops]) +
              (DEBUG and ('\\n' + block.outRegs.graphStr()) or '') +
              "\\n>>")
            g.add_node(block.id, label=ltext)

        for block in self.basicBlocks.values():
            for addr in block.goTo:
                if isinstance(addr, basestring):
                    continue
                target_block = self.basicBlocks[addr]
                attrs = {}
                if target_block.id == block.lastAddr + 1:
                    attrs['color'] = 'gray'
                g.add_edge(block.id, target_block.id, **attrs)

        g.node_attr['shape'] = 'box'
        g.node_attr['fontsize'] = '8'

        self._graphvizRenderHelper(g, outpath)

    def dataFlow(self):
        todo = [self.basicBlocks[0]]

        # dynamic jumps can require the CFG to be rebuilt, although we try and
        #  avoid that where possible.  This variable tracks this, which requires
        #  us to re-build our CFG and re-process things.  Thankfully all our
        #  results that have already been computed should still be accurate, we
        #  just might need to flow things even further.
        cfgInvalid = False

        def goToBlocks(block):
            for addr in block.goTo:
                yield self.basicBlocks[addr]

        def comeFromBlocks(block):
            for addr in block.comeFrom:
                if not addr in self.basicBlocksByEnd:
                    print 'PROBLEM! no such comeFrom on addr', addr, 'for block:'
                    print block
                yield self.basicBlocksByEnd[addr]

        def originBlocksDone(block):
            for addr in block.comeForm:
                if not self.basicBlocksByEnd[addr].done:
                    return False
            return True

        def ensureJumpTargets(op, addrs, adjustment=1):
            # This returns the (adjusted) addresses of jumps that are new to
            #  us.  We need to check to make sure there is actually a basic
            #  block that starts there.  If not, we need to mark the CFG
            #  invalid and requiring a re-processing.
            # Also, we need to fix up comeFrom
            if VERBOSE:
                print 'adding dynamic jumps to', addrs, 'from', op
            unknownRealAddrs = op.ensureJumpTargets(addrs, adjustment)
            for unknownRealAddr in unknownRealAddrs:
                if unknownRealAddr in self.basicBlocks:
                    other = self.basicBlocks[unknownRealAddr]
                    if not op.addr in other.comeFrom:
                        other.comeFrom.append(op.addr)
                else:
                    print '!' * 80
                    print 'WARNING, CFG split at', unknownRealAddr, 'required'
                    print 'Jerky opcode is', op
                    print '!' * 80
                    cfgInvalid = True
            return unknownRealAddrs

        def flowBlock(block):
            changes = False

            for parent in comeFromBlocks(block):
                block.inRegs.update(parent.outRegs)
            if VERBOSE:
                print 'inRegs', block.inRegs
            curRegs = block.inRegs.copy()
            if VERBOSE:
                print 'curRegs', curRegs

            for op in block.ops:
                # affect the operation for its input regs
                for reg in op.regReads:
                    op.affectedByCursors.update(curRegs.getRegCursorImpacts(reg))

                if op.writesCursor: # implies usesCursor
                    op.usesCursor.writesAffectedBy.update(op.affectedByCursors)
                if op.seeksCursor:
                    op.usesCursor.seeksAffectedBy.update(op.affectedByCursors)

                # affect the output registers
                for reg in op.regWrites:
                    if curRegs.getRegCursorImpacts(reg) != op.affectedByCursors:
                        if VERBOSE:
                            print 'change', reg, curRegs.getRegCursorImpacts(reg), op.affectedByCursors
                        curRegs.setRegCursorImpacts(reg, op.affectedByCursors)
                        #changes = True

                # stuff for Gosub, Yield, Return
                if op.dynamicGoTo:
                    if ensureJumpTargets(op, curRegs.getRegValues(op.dynamicGoTo)):
                        # jump target changes are mutations of the CFG and
                        #  require change processing!
                        changes = True
                if op.dynamicWritePC:
                    curRegs.setRegValue(op.dynamicWritePC, op.addr)

            if block.outRegs.checkDelta(curRegs):
                changes = True
                block.outRegs = curRegs
            if VERBOSE:
                print 'outRegs', block.outRegs

            return changes

        while todo:
            block = todo.pop()
            if VERBOSE:
                print '................'
                print 'processing block', block
            # if a change happened, the block is not done, and his kids are not
            #  done (and need to be processed)
            if flowBlock(block):
                block.done = False
                if VERBOSE:
                    print 'Changes on', block
                for child in goToBlocks(block):
                    child.done = False
                    if not child in todo:
                        todo.insert(0, child)
            # no changes, so mark us done but schedule our children if they
            #  are not done.
            else:
                block.done = True
                for child in goToBlocks(block):
                    if not child.done:
                        if not child in todo:
                            todo.insert(0, child)

        return cfgInvalid

    def diagDataFlow(self, outpath):
        self.colorCursors()

        g = pygraphviz.AGraph(directed=True)
        for cursor in self.cursors:
            label = "<<<font color='%s'>%s</font>>>" % (
                cursor.color, cursor)
            g.add_node(cursor.id, label=label)
        for cursor in self.cursors:
            for originCursor in cursor.writesAffectedBy:
                g.add_edge(originCursor.id, cursor.id)
            for originCursor in cursor.seeksAffectedBy:
                g.add_edge(originCursor.id, cursor.id, color="#cccccc")

        for result_op in self.resultRowOps:
            for cursor in result_op.affectedByCursors:
                g.add_edge(cursor.id, "Results")

        g.node_attr['fontsize'] = '10'
        self._graphvizRenderHelper(g, outpath)


    def dump(self):
        print 'Code:'
        for op in self.code:
            op.dump()

        print 'Tables:'
        for table in self.allTables:
            print '  ', table

class CmdLine(object):
    usage = '''usage: %prog [options] explained.json/explained.txt

    We process SQLite EXPLAIN output in order to visualize it using graphviz.
    We require that your SQLite was built with -DDEBUG because then we get
    useful schema meta-data from the comment column and we really enjoy that.
    '''

    def buildParser(self):
        parser = optparse.OptionParser(usage=self.usage)

        parser.add_option('-d', '--debug',
                          action='store_true', dest='debug', default=False,
                          help='Dump registers at block entry/exit')
        parser.add_option('-y', '--yields',
                          action='store_true', dest='yields', default=False,
                          help='Process yields for control/dataflow analysis.')
        parser.add_option('-v', '--verbose',
                          action='store_true', dest='verbose', default=False,
                          help='Output a lot of info about what we are doing.')

        return parser
    
    def run(self):
        global DEBUG, NO_YIELDS, VERBOSE

        parser = self.buildParser()
        options, args = parser.parse_args()

        DEBUG = options.debug
        NO_YIELDS = not options.yields
        VERBOSE = options.verbose

        for filename in args:
            if filename.endswith('.json'):
                import json
                f = open(filename, 'r')
                obj = json.load(f)
                f.close()

                for query in obj["queries"]:
                    print 'PROCESSING', query["sql"]
                    eg = ExplainGrokker()
                    eg.parseJsonOpcodesList(query["operations"])
                    eg.performFlow()
        


if __name__ == '__main__':

    cmdline = CmdLine()
    cmdline.run()
    
    import sys
    sys.exit(0)

    sg = SchemaGrokker()
    sf = open('/tmp/schemainfo.txt')
    sg.grok(sf)
    sf.close()

    eg = ExplainGrokker()
    f = open('/tmp/explained.txt')
    eg.grok(f, sg)
    eg.dump()
    f.close()

    eg.diagBasicBlocks('/tmp/blocks.dot')
    eg.diagDataFlow('/tmp/dataflow.dot')
