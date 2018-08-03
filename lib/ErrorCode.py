# -*- coding: utf-8 -*-
''' 
@Time    : 2018/7/18 14:57
@Author  : Great God
@File    : ErrorCode.py
@Software: PyCharm
'''


ErrorCode = {
    5:  None,       #Out of memory (Needed %u bytes)
    6:  None,       #Error on delete of '%s' (OS errno %d - %s)
    74: None,       # %s: Empty value for '%s' specified.
    78: None,       #Incorrect integer value: '%s'
    79: None,       # Incorrect unsigned integer value: '%s'
    80: None,       #option '%s': unsigned value %s adjusted to %s
    84: None,       #Invalid decimal value for option '%s'.
    1040:   True,   #Too many connections
    1044:   None,   #Access denied for user '%s'@'%s' to database '%s'
    1045:   None,   #Access denied for user '%s'@'%s' (using password: %s)
    1046:   None,   #No database selected
    1047:   None,   #Unknown command
    1048:   None,   #Column '%s' cannot be null
    1049:   None,   #Unknown database '%s'
    1051:   None,   #Unknown table '%s'
    1053:   True,   #Server shutdown in progress
    1054:   None,   #Unknown column '%s' in '%s'
    1058:   None,   #Column count doesn't match value count
    1060:   None,   #Duplicate column name '%s'
    1061:   None,   #Duplicate key name '%s'
    1062:   None,   #Duplicate entry '%s' for key %d
    1063:   None,   # Incorrect column specifier for column '%s'
    1067:   None,   #Invalid default value for '%s'
    1068:   None,   #Multiple primary key defined
    1071:   None,   #Specified key was too long; max key length is %d bytes
    1072:   None,   #Key column '%s' doesn't exist in table
    1077:   True,   #Normal shutdown
    1079:   True,   #Shutdown complete
    1088:   None,   #Records: %ld Duplicates: %ld
    1097:   None,   #Too many strings for column %s and SET
    1102:   None,   #Incorrect database name '%s'
    1103:   None,   #Incorrect table name '%s'
    1146:   None,   #Table '%s.%s' doesn't exist
    1149:   None,   #You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use
    1153:   None,   #Got a packet bigger than 'max_allowed_packet' bytes
    1162:   None,   #Result string is longer than 'max_allowed_packet' bytes
    1166:   None,   #Incorrect column name '%s'
    1176:   None,   #Key '%s' doesn't exist in table '%s'
    1179:   None,   # You are not allowed to execute this command in a transaction
    1192:   None,   #Can't execute the given command because you have active locked tables or an active transaction
    1227:   None,   # Access denied; you need (at least one of) the %s privilege(s) for this operation
    1264:   None,   #Out of range value for column '%s' at row %ld
    1305:   None,   # %s %s does not exist
    1334:   None,   #Failed to ALTER %s %s
    1366:   None,   #Incorrect %s value: '%s' for column '%s' at row %ld
    1382:   None,   # The '%s' syntax is reserved for purposes internal to the MySQL server
    1406:   None,   #Data too long for column '%s' at row %ld
    2013:   True,   #Lost connection to MySQL server during query
    2003:   True,   #Can't connect to MySQL server on '10.3.10.11' ([Errno 111] Connection refused)
    1213:   True,   #eadlock found when trying to get lock; try restarting transaction
    2006:   True,   #MySQL server has gone away (ConnectionResetError(104, 'Connection reset by peer'))
    }