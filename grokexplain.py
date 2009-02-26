# Attempt to understand what is happening in a SQLite EXPLAIN-ation.
# Our primary concern is data-flow, not control flow.
#
# Andrew Sutherland <asutherland@asutherland.org>
# Mozilla Messaging, Inc.

import pygraphviz
import cStringIO as StringIO
import os

class TableSchema(object):
    def __init__(self, name, colnames):
        self.name = name
        self.columns = colnames
        

class SchemaGrokker(object):
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
        self.on = kwargs.pop('table')
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
                print ' copying cursors', reg, cursors
            for reg, values in copyFrom.regValues.items():
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

    def graphStr(self, schemaInfo):
        if self.usesCursor:
            s = "<font color='%s'>%d %s [%d]</font>" % (
                self.usesCursor.color, self.addr, self.name,
                self.usesCursor.handle)
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
        self.realTables = []
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

    def _newVirtualTable(self, **kwargs):
        # TODO: have some means of figuring out what table it is other
        #  than by default!
        if len(self.schemaInfo.virtualTables) == 1:
            kwargs['schema'] = self.schemaInfo.virtualTables.values()[0]

        table = Table(virtual=True, openedAt=self.op, **kwargs)
        self.virtualTables.append(table)
        self.allTables.append(table)
        self.op.births.append(table)
        return table

    def _newRealTable(self, **kwargs):
        # TODO: have some means of figuring out what table it is other
        #  than by default!
        if len(self.schemaInfo.tables) == 1:
            kwargs['schema'] = self.schemaInfo.tables.values()[0]

        table = Table(openedAt=self.op, **kwargs)
        self.realTables.append(table)
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

    def _newIndexOn(self, table, indexDetails, **kwargs):
        # indexDetails is of the form: "keyinfo(%d,"...
        numColumns = self._parseKeyinfo(indexDetails)
        index = Index(table=table, columns=numColumns, openedAt=self.op,
                      **kwargs)
        self.indices.append(index)
        self.op.births.append(index)
        return index

    def _newCursor(self, handle, thing, **kwargs):
        if handle in self.cursorByHandle:
            raise Exception('ERROR! Cursor handle collision; need more clever!')
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
        cursorNum = params[0]
        indexDetails = params[3]

        table = self._newRealTable(
            name=("table%d:%d" % (cursorNum, params[1])),
            columns = self.nextOpInfo)
        if indexDetails:
            cursorOn = self._newIndexOn(table, indexDetails,
                                        name="index%d" % (cursorNum,))
        else:
            cursorOn = table

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

    def _op_SetNumColumns(self, params):
        self.nextOpInfo = params[1]

    def _op_VOpen(self, params):
        cursorNum = params[0]
        table = self._newVirtualTable(
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
            cursorOn = self._newIndexOn(table, indexDetails,
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

    def _jump(self, target):
        self.op.goTo.append(target)

    def _op_Goto(self, params):
        self._jump(params[1])
        self.op.usesImmediate = params[1]

    def _op_Jump(self, params):
        # we base our decision on the result of the last compare
        self.op.regReads.append('for_jump')
        self._jump(params[0])
        self._jump(params[1])
        self._jump(params[2])

    def _op_Gosub(self, params):
        self.op.regWrites.append(params[0])
        self.op.dynamicWritePC = params[0]
        self.op.usesImmediate = params[1]
        self._jump(params[1])

    def _op_Yield(self, params):
        self.op.regReads.append(params[0])
        self.op.regWrites.append(params[0])
        self.op.dynamicWritePC = params[0]
        # we won't know where out goTo goes to until dataflow analysis, nor
        #  where we would 'come from' to the next opcode.  But we do know that
        #  after us is a basic block break, so let's hint that.
        self.op.dynamicGoTo = params[0]

    def _op_Return(self, params):
        # just like for Yield, we have no idea where we are going until
        #  dataflow.
        self.op.regReads.append(params[0])
        self.op.dynamicGoTo = params[0]

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

    def _op_NotExists(self, params):
        self._getCursor(params[0], False, True)
        self._condJump(params[2], params[1])

    def _op_Found(self, params):
        self._getCursor(params[0], False, True)
        self._condJump(params[2], params[1])
    def _op_NotFound(self, params):
        self._op_Found(params)

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

    def _op_MakeRecord(self, params):
        self.op.regReads.extend([params[0] + x for x in range(params[1])])
        # writes to reg p3
        self.op.regWrites.append(params[2])

    def _op_ResultRow(self, params):
        self.op.regReads.extend([params[0] + x for x in range(params[1])])
        self.resultRowOps.append(self.op)

    def _op_Insert(self, params):
        self._getCursor(params[0], True)
        self.op.regReads.extend([params[1], params[2]])

    def _op_IdxInsert(self, params):
        self._getCursor(params[0], True)
        self.op.regReads.append(params[1])

    def _op_Delete(self, params):
        # a delete is a write...
        self._getCursor(params[0], True)

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

    def grok(self, file_or_lines, schemaInfo=None):
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
                print 'at op', op.addr, 'goTo', addr
                targ_op = self.code[addr]
                if op.addr not in targ_op.comeFrom:
                    targ_op.comeFrom.append(op.addr)
                else:
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
                    print 'new', block
                    self.basicBlocks[block.id] = block
                    self.basicBlocksByEnd[block.lastAddr] = block
                block_ops = []
            block_ops.append(op)
            # if this op jumps places, then close out this set of block_ops
            #  into a basic block.
            if op.goTo or op.dynamicGoTo != False:
                block = BasicBlock(block_ops)
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
            ltext = "<<" + '\\n'.join(
                [op.graphStr(self.schemaInfo) for op in block.ops]) + "\\n>>"
            g.add_node(block.id, label=ltext)

        for block in self.basicBlocks.values():
            for addr in block.goTo:
                if isinstance(addr, basestring):
                    continue
                target_block = self.basicBlocks[addr]
                g.add_edge(block.id, target_block.id)

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
            print 'adding dynamic jumps to', addrs, 'from', op
            unknownRealAddrs = op.ensureJumpTargets(addrs, adjustment)
            for unknownRealAddr in unknownRealAddrs:
                if not unknownRealAddr in self.basicBlocks:
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
            print 'inRegs', block.inRegs
            curRegs = block.inRegs.copy()
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
                        print 'change', reg, curRegs.getRegCursorImpacts(reg), op.affectedByCursors
                        curRegs.setRegCursorImpacts(reg, op.affectedByCursors)
                        #changes = True

                # stuff for Gosub, Yield, Return
                if op.dynamicWritePC:
                    curRegs.addRegValue(op.dynamicWritePC, op.addr)
                if op.dynamicGoTo:
                    if ensureJumpTargets(op, curRegs.getRegValues(op.dynamicGoTo)):
                        # jump target changes are mutations of the CFG and
                        #  require change processing!
                        changes = True

            if block.outRegs.checkDelta(curRegs):
                changes = True
                block.outRegs = curRegs
            print 'outRegs', block.outRegs

            return changes

        while todo:
            block = todo.pop()
            print '................'
            print 'processing block', block
            # if a change happened, the block is not done, and his kids are not
            #  done (and need to be processed)
            if flowBlock(block):
                block.done = False
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


if __name__ == '__main__':
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
