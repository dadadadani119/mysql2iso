# -*- encoding: utf-8 -*-
'''
@author: Great God
'''

import struct,pymysql
from .Gtid import GtidSet

class ReplicationMysql:
    def __init__(self, server_id=None, log_file=None,
                 log_pos=None,mysql_connection=None,auto_position=None,gtid=None):

        self.auto_position = auto_position
        self.gtid = gtid
        self._log_file = log_file
        self._log_pos = log_pos
        self.block = True
        self.server_id = server_id if server_id != None else 133
        self.connection = mysql_connection

    def __checksum_enabled(self):
        """Return True if binlog-checksum = CRC32. Only for MySQL > 5.6"""
        with self.connection.cursor() as cur:
            cur.execute('SET SESSION wait_timeout = 2147483;')
            sql = 'SHOW GLOBAL VARIABLES LIKE "BINLOG_CHECKSUM";'
            cur.execute(sql)
            result = cur.fetchone()

        if result is None:
            return False
        if 'Value' in result and result['Value'] is None:
            return False
        return True

    def __set_checksum(self):
        with self.connection.cursor() as cur:
            cur.execute("set @master_binlog_checksum= @@global.binlog_checksum;")

    def GetFile(self):
        with self.connection.cursor() as cur:
            sql = "show master status;"
            cur.execute(sql)
            result = cur.fetchone()
            return result['File'], result['Position']

    def PackeByte(self):
        '''
        Format for mysql packet position
        file_length: 4bytes
        dump_type: 1bytes
        position: 4bytes
        flags: 2bytes  
            0: BINLOG_DUMP_BLOCK
            1: BINLOG_DUMP_NON_BLOCK
        server_id: 4bytes
        log_file
        :return: 
        '''
        COM_BINLOG_DUMP = 0x12

        if self._log_file is None:
            if self._log_pos is None:
                self._log_file, self._log_pos = self.GetFile()
            else:
                self._log_file, _ = self.GetFile()
        elif self._log_file and self._log_pos is None:
            self._log_pos = 4

        prelude = struct.pack('<i', len(self._log_file) + 11) \
                  + struct.pack("!B", COM_BINLOG_DUMP)

        prelude += struct.pack('<I', self._log_pos)
        if self.block:
            prelude += struct.pack('<h', 0)
        else:
            prelude += struct.pack('<h', 1)

        prelude += struct.pack('<I', self.server_id)
        prelude += self._log_file.encode()
        return prelude

    def GtidPackeByte(self):
        '''
        Format for mysql packet master_auto_position

        All fields are little endian
        All fields are unsigned

        Packet length   uint   4bytes
        Packet type     byte   1byte   == 0x1e
        Binlog flags    ushort 2bytes  == 0 (for retrocompatibilty)
        Server id       uint   4bytes
        binlognamesize  uint   4bytes
        binlogname      str    Nbytes  N = binlognamesize
                                       Zeroified
        binlog position uint   4bytes  == 4
        payload_size    uint   4bytes

        What come next, is the payload, where the slave gtid_executed
        is sent to the master
        n_sid           ulong  8bytes  == which size is the gtid_set
        | sid           uuid   16bytes UUID as a binary
        | n_intervals   ulong  8bytes  == how many intervals are sent for this gtid
        | | start       ulong  8bytes  Start position of this interval
        | | stop        ulong  8bytes  Stop position of this interval

        A gtid set looks like:
          19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10,
          1c2aad49-ae92-409a-b4df-d05a03e4702e:42-47:80-100:130-140

        In this particular gtid set, 19d69c1e-ae97-4b8c-a1ef-9e12ba966457:1-3:8-10
        is the first member of the set, it is called a gtid.
        In this gtid, 19d69c1e-ae97-4b8c-a1ef-9e12ba966457 is the sid
        and have two intervals, 1-3 and 8-10, 1 is the start position of the first interval
        3 is the stop position of the first interval.
        '''
        COM_BINLOG_DUMP_GTID = 0x1e

        gtid_set = GtidSet(self.gtid)
        encoded_data_size = gtid_set.encoded_length

        header_size = (2 +  # binlog_flags
                       4 +  # server_id
                       4 +  # binlog_name_info_size
                       4 +  # empty binlog name
                       8 +  # binlog_pos_info_size
                       4)  # encoded_data_size

        prelude = b'' + struct.pack('<i', header_size + encoded_data_size) \
                  + struct.pack("!B",COM_BINLOG_DUMP_GTID)

        # binlog_flags = 0 (2 bytes)
        prelude += struct.pack('<H', 0)
        # server_id (4 bytes)
        prelude += struct.pack('<I', self.server_id)
        # binlog_name_info_size (4 bytes)
        prelude += struct.pack('<I', 3)
        # empty_binlog_name (4 bytes)
        prelude += b'\0\0\0'
        # binlog_pos_info (8 bytes)
        prelude += struct.pack('<Q', 4)

        # encoded_data_size (4 bytes)
        prelude += struct.pack('<I', gtid_set.encoded_length)
        # encoded_data
        prelude += gtid_set.encoded()
        return prelude

    def ReadPack(self):
        if self.auto_position:
            _packet = self.GtidPackeByte()
        else:
            _packet = self.PackeByte()
        if self.__checksum_enabled():
            self.__set_checksum()

        if pymysql.__version__ < "0.6":
            self.connection.wfile.write(_packet)
            self.connection.wfile.flush()
        else:
            self.connection._write_bytes(_packet)
            self.connection._next_seq_id = 1

        return self.connection

        '''
        while True:
            try:
                if pymysql.__version__ < "0.6":
                    pkt = self.connection.read_packet()
                else:
                    pkt = self.connection._read_packet()

                self.UnPack(pkt)
            except:
                self.connection.close()
                break
        '''

