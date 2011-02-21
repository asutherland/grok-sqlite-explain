# What is it?

It takes SQLite explanations and visualizes them.  If you have performance data
in the form of I/O counts and opcode execution counts, it can also show those.

See [this blog post](http://www.visophyte.org/blog/2010/04/06/performance-annotated-sqlite-explaination-visualizations-using-systemtap/).

# What ingredients are required?

you need pygraphviz... and graphviz!

# How do I make it go?

It now parses the command line and has stopped being dump.  Basically you just
point it at a ".json" file of the right type or a ".txt" file that is the
output of performing a "EXPLAIN SELECT" using the sqlite3 binary built with
"--enable-debug" and it does its thing.  If you are not fancy enough to build
sqlite with debug, the code still has some leftover logic that is capable of
parsing a schema dump, but you will need to turn that back on yourself or
ask nicely.

Invoke the script with "--help" to get more details.

Commands you might run to get that nougaty information include:

sqlite3-cvs global-messages-db.sqlite 'explain SELECT * FROM messages INNER JOIN messagesText ON messages.id = messagesText.rowid WHERE id IN (SELECT docid FROM messagesText WHERE subject MATCH "sutherland") AND deleted = 0 AND folderID IS NOT NULL AND messageKey IS NOT NULL ORDER BY date DESC LIMIT 100;' > /tmp/explained.txt

$ sqlite3-cvs global-messages-db.sqlite '.schema messages%' > /tmp/schemainfo.txt
