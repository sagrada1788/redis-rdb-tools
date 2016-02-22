import struct
import io
import sys
import datetime
import re

try :
    from StringIO import StringIO
except ImportError:
    from io import StringIO
    
REDIS_RDB_6BITLEN = 0
REDIS_RDB_14BITLEN = 1
REDIS_RDB_32BITLEN = 2
REDIS_RDB_ENCVAL = 3

REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
REDIS_RDB_OPCODE_EXPIRETIME = 253
REDIS_RDB_OPCODE_SELECTDB = 254
REDIS_RDB_OPCODE_EOF = 255

REDIS_RDB_TYPE_STRING = 0
REDIS_RDB_TYPE_LIST = 1
REDIS_RDB_TYPE_SET = 2
REDIS_RDB_TYPE_ZSET = 3
REDIS_RDB_TYPE_HASH = 4
REDIS_RDB_TYPE_HASH_ZIPMAP = 9
REDIS_RDB_TYPE_LIST_ZIPLIST = 10
REDIS_RDB_TYPE_SET_INTSET = 11
REDIS_RDB_TYPE_ZSET_ZIPLIST = 12
REDIS_RDB_TYPE_HASH_ZIPLIST = 13

REDIS_RDB_ENC_INT8 = 0
REDIS_RDB_ENC_INT16 = 1
REDIS_RDB_ENC_INT32 = 2
REDIS_RDB_ENC_LZF = 3

DATA_TYPE_MAPPING = {
    0 : "string", 1 : "list", 2 : "set", 3 : "sortedset", 4 : "hash", 
    9 : "hash", 10 : "list", 11 : "set", 12 : "sortedset", 13 : "hash"}

class RdbCallback:
    """
    A Callback to handle events as the Redis dump file is parsed.
    This callback provides a serial and fast access to the dump file.
    
    """
    def start_rdb(self):
        """
        Called once we know we are dealing with a valid redis dump file
        
        """
        pass
        
    def start_database(self, db_number):
        """
        Called to indicate database the start of database `db_number` 
        
        Once a database starts, another database cannot start unless 
        the first one completes and then `end_database` method is called
        
        Typically, callbacks store the current database number in a class variable
        
        """     
        pass
    
    def set(self, key, value, expiry, info):
        """
        Callback to handle a key with a string value and an optional expiry
        
        `key` is the redis key
        `value` is a string or a number
        `expiry` is a datetime object. None and can be None
        `info` is a dictionary containing additional information about this object.
        
        """
        pass
    
    def start_hash(self, key, length, expiry, info):
        """Callback to handle the start of a hash
        
        `key` is the redis key
        `length` is the number of elements in this hash. 
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_hash`, the method `hset` will be called with this `key` exactly `length` times.
        After that, the `end_hash` method will be called.
        
        """
        pass
    
    def hset(self, key, field, value, info):
        """
        Callback to insert a field=value pair in an existing hash
        
        `key` is the redis key for this hash
        `field` is a string
        `value` is the value to store for this field
        
        """
        pass
    
    def end_hash(self, key):
        """
        Called when there are no more elements in the hash
        
        `key` is the redis key for the hash
        
        """
        pass
    
    def start_set(self, key, cardinality, expiry, info):
        """
        Callback to handle the start of a hash
        
        `key` is the redis key
        `cardinality` is the number of elements in this set
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_set`, the  method `sadd` will be called with `key` exactly `cardinality` times
        After that, the `end_set` method will be called to indicate the end of the set.
        
        Note : This callback handles both Int Sets and Regular Sets
        
        """
        pass

    def sadd(self, key, member, info):
        """
        Callback to inser a new member to this set
        
        `key` is the redis key for this set
        `member` is the member to insert into this set
        
        """
        pass
    
    def end_set(self, key):
        """
        Called when there are no more elements in this set 
        
        `key` the redis key for this set
        
        """
        pass
    
    def start_list(self, key, length, expiry, info):
        """
        Callback to handle the start of a list
        
        `key` is the redis key for this list
        `length` is the number of elements in this list
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_list`, the method `rpush` will be called with `key` exactly `length` times
        After that, the `end_list` method will be called to indicate the end of the list
        
        Note : This callback handles both Zip Lists and Linked Lists.
        
        """
        pass
    
    def rpush(self, key, value) :
        """
        Callback to insert a new value into this list
        
        `key` is the redis key for this list
        `value` is the value to be inserted
        
        Elements must be inserted to the end (i.e. tail) of the existing list.
        
        """
        pass
    
    def end_list(self, key):
        """
        Called when there are no more elements in this list
        
        `key` the redis key for this list
        
        """
        pass
    
    def start_sorted_set(self, key, length, expiry, info):
        """
        Callback to handle the start of a sorted set
        
        `key` is the redis key for this sorted
        `length` is the number of elements in this sorted set
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.
        
        After `start_sorted_set`, the method `zadd` will be called with `key` exactly `length` times. 
        Also, `zadd` will be called in a sorted order, so as to preserve the ordering of this sorted set.
        After that, the `end_sorted_set` method will be called to indicate the end of this sorted set
        
        Note : This callback handles sorted sets in that are stored as ziplists or skiplists
        
        """
        pass
    
    def zadd(self, key, score, member):
        """Callback to insert a new value into this sorted set
        
        `key` is the redis key for this sorted set
        `score` is the score for this `value`
        `value` is the element being inserted
        """
        pass
    
    def end_sorted_set(self, key):
        """
        Called when there are no more elements in this sorted set
        
        `key` is the redis key for this sorted set
        
        """
        pass
    
    def end_database(self, db_number):
        """
        Called when the current database ends
        
        After `end_database`, one of the methods are called - 
        1) `start_database` with a new database number
            OR
        2) `end_rdb` to indicate we have reached the end of the file
        
        """
        pass
    
    def end_rdb(self):
        """Called to indicate we have completed parsing of the dump file"""
        pass

class RdbParser :
    """
    A Parser for Redis RDB Files
    
    This class is similar in spirit to a SAX parser for XML files.
    The dump file is parsed sequentially. As and when objects are discovered,
    appropriate methods in the callback are called. 
        
    Typical usage :
        callback = MyRdbCallback() # Typically a subclass of RdbCallback
        parser = RdbParser(callback)
        parser.parse('/var/redis/6379/dump.rdb')
    
    filter is a dictionary with the following keys
        {"dbs" : [0, 1], "keys" : "foo.*", "types" : ["hash", "set", "sortedset", "list", "string"]}
        
        If filter is None, results will not be filtered
        If dbs, keys or types is None or Empty, no filtering will be done on that axis

    ## mi add ##
    ignore is a list with the following items
        ["real_value", "real_field"]
    ############
    """
    def __init__(self, callback, filters = None, ignore = None) :
        """
            `callback` is the object that will receive parse events
        """
        self._callback = callback
        self._key = None
        self._expiry = None
        self.init_filter(filters)
        self.init_ignore(ignore)

    def parse(self, filename):
        """
        Parse a redis rdb dump file, and call methods in the 
        callback object during the parsing operation.
        """
        with open(filename, "rb") as f:
            self.verify_magic_string(f.read(5))
            self.verify_version(f.read(4))
            self._callback.start_rdb()
            
            is_first_database = True
            db_number = 0
            while True :
                self._expiry = None
                data_type, orig_data_type = read_unsigned_char(f)
                
                if data_type == REDIS_RDB_OPCODE_EXPIRETIME_MS :
                    expiry, orig_expiry = read_unsigned_long(f)
                    self._expiry = to_datetime(expiry * 1000)
                    self._orig_expiry = orig_data_type + orig_expiry
                    data_type, orig_data_type = read_unsigned_char(f)
                    self._orig_data_type = orig_data_type
                elif data_type == REDIS_RDB_OPCODE_EXPIRETIME :
                    expiry, orig_expiry = read_unsigned_int(f)
                    self._expiry = to_datetime(expiry * 1000000)
                    self._orig_expiry = orig_data_type + orig_expiry
                    data_type, orig_data_type = read_unsigned_char(f)
                    self._orig_data_type = orig_data_type
                else:
                    self._orig_expiry = None
                    self._orig_data_type = orig_data_type
                
                if data_type == REDIS_RDB_OPCODE_SELECTDB :
                    if not is_first_database :
                        self._callback.end_database(db_number)
                    is_first_database = False
                    db_number, orig_db_number = self.read_length(f)
                    _info = {'orig_db_number': orig_db_number}
                    self._callback.start_database(db_number, _info)
                    continue
                
                if data_type == REDIS_RDB_OPCODE_EOF :
                    _info = {'orig_end_db': orig_data_type}
                    self._callback.end_database(db_number, _info)
                    self._callback.end_rdb()
                    break

                if self.matches_filter(db_number) :
                    self._key, self._orig_key = self.read_string(f, is_key = True)
                    if self.matches_filter(db_number, self._key, data_type):
                        self.read_object(f, data_type)
                    else:
                        self.skip_object(f, data_type)
                else :
                    self.skip_key_and_object(f, data_type)

    def read_length_with_encoding(self, f) :
        length = 0
        is_encoded = False
        bts = []
        data, orig_data = read_unsigned_char(f)
        bts.append(orig_data)
        enc_type = (data & 0xC0) >> 6
        if enc_type == REDIS_RDB_ENCVAL :
            is_encoded = True
            length = data & 0x3F
        elif enc_type == REDIS_RDB_6BITLEN :
            length = data & 0x3F
        elif enc_type == REDIS_RDB_14BITLEN :
            data1, orig_data1 = read_unsigned_char(f)
            bts.append(orig_data1)
            length = ((data&0x3F)<<8)|data1
        else :
            length, orig_length = ntohl(f)
            bts.append(orig_length)
        return (length, is_encoded, bts)

    def read_length(self, f) :
        tup = self.read_length_with_encoding(f)
        return tup[0], ''.join(tup[2])

    def read_string(self, f, is_key = False) :
        tup = self.read_length_with_encoding(f)
        length = tup[0]
        is_encoded = tup[1]
        bts = tup[2]
        val = None
        if is_encoded :
            if length == REDIS_RDB_ENC_INT8 :
                val, orig_val = read_signed_char(f)
                bts.append(orig_val)
            elif length == REDIS_RDB_ENC_INT16 :
                val, orig_val = read_signed_short(f)
                bts.append(orig_val)
            elif length == REDIS_RDB_ENC_INT32 :
                val, orig_val = read_signed_int(f)
                bts.append(orig_val)
            elif length == REDIS_RDB_ENC_LZF :
                clen, orig_clen = self.read_length(f)
                l, orig_l = self.read_length(f)
                orig_val = f.read(clen)
                if is_key:
                    val = self.lzf_decompress(orig_val , l)
                elif not is_key and not self._ignore_real_value:
                    val = self.lzf_decompress(orig_val , l)
                #
                bts.append(orig_clen)
                bts.append(orig_l)
                bts.append(orig_val)
        else :
            val = f.read(length)
            bts.append(val)
        return val, ''.join(bts)

    # Read an object for the stream
    # f is the redis file 
    # enc_type is the type of object
    def read_object(self, f, enc_type) :
        if enc_type == REDIS_RDB_TYPE_STRING :
            val, orig_val = self.read_string(f)
            info = {'encoding': 'string',
                    'orig_data_type': self._orig_data_type,
                    'orig_expiry': self._orig_expiry,
                    'orig_key': self._orig_key,
                    'orig_val': orig_val}
            self._callback.set(self._key, val, self._expiry, info)
        elif enc_type == REDIS_RDB_TYPE_LIST :
            # A redis list is just a sequence of strings
            # We successively read strings from the stream and create a list from it
            # The lists are in order i.e. the first string is the head, 
            # and the last string is the tail of the list
            length, orig_length = self.read_length(f)
            info = {'encoding': 'linkedlist',
                    'orig_data_type': self._orig_data_type,
                    'orig_expiry': self._orig_expiry,
                    'orig_key': self._orig_key,
                    'orig_length': orig_length
                    }
            self._callback.start_list(self._key, length, self._expiry, info)
            for count in xrange(0, length) :
                val, orig_val = self.read_string(f)
                _info = {'orig_val': orig_val}
                self._callback.rpush(self._key, val, _info)
            self._callback.end_list(self._key)
        elif enc_type == REDIS_RDB_TYPE_SET :
            # A redis list is just a sequence of strings
            # We successively read strings from the stream and create a set from it
            # Note that the order of strings is non-deterministic
            length, orig_length = self.read_length(f)
            info = {'encoding': 'hashtable',
                    'orig_data_type': self._orig_data_type,
                    'orig_expiry': self._orig_expiry,
                    'orig_key': self._orig_key,
                    'orig_length': orig_length
                    }
            self._callback.start_set(self._key, length, self._expiry, info)
            for count in xrange(0, length) :
                val, orig_val = self.read_string(f)
                _info = {'orig_val': orig_val}
                self._callback.sadd(self._key, val, _info)
            self._callback.end_set(self._key)
        elif enc_type == REDIS_RDB_TYPE_ZSET :
            length, orig_length = self.read_length(f)
            info = {'encoding':'skiplist',
                    'orig_data_type': self._orig_data_type,
                    'orig_expiry': self._orig_expiry,
                    'orig_key': self._orig_key,
                    'orig_length': orig_length
                    }
            self._callback.start_sorted_set(self._key, length, self._expiry, info)
            for count in xrange(0, length) :
                val, orig_val = self.read_string(f)
                dbl_length, orig_dbl_length = read_unsigned_char(f)
                score = f.read(dbl_length)
                _info = {'orig_length': orig_length,
                         'orig_val': orig_val,
                         'orig_dbl_length': orig_dbl_length,
                         'orig_score': score
                         }
                if isinstance(score, str):
                    score = float(score)
                self._callback.zadd(self._key, score, val, _info)
            self._callback.end_sorted_set(self._key)
        elif enc_type == REDIS_RDB_TYPE_HASH :
            length, orig_length = self.read_length(f)
            info = {'encoding': 'hashtable',
                    'orig_data_type': self._orig_data_type,
                    'orig_expiry': self._orig_expiry,
                    'orig_length': orig_length,
                    'orig_key': self._orig_key
                    }
            self._callback.start_hash(self._key, length, self._expiry, info)
            for count in xrange(0, length) :
                field, orig_field = self.read_string(f)
                value, orig_value = self.read_string(f)
                _info = {'orig_field': orig_field,
                         'orig_value': orig_value
                         }
                self._callback.hset(self._key, field, value, _info)
            self._callback.end_hash(self._key)
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP :
            self.read_zipmap(f)
        elif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST :
            self.read_ziplist(f)
        elif enc_type == REDIS_RDB_TYPE_SET_INTSET :
            self.read_intset(f)
        elif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST :
            self.read_zset_from_ziplist(f)
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST :
            self.read_hash_from_ziplist(f)
        else :
            raise Exception('read_object', 'Invalid object type %d for key %s' % (enc_type, self._key))

    def skip_key_and_object(self, f, data_type):
        self.skip_string(f)
        self.skip_object(f, data_type)

    def skip_string(self, f):
        tup = self.read_length_with_encoding(f)
        length = tup[0]
        is_encoded = tup[1]
        bytes_to_skip = 0
        if is_encoded :
            if length == REDIS_RDB_ENC_INT8 :
                bytes_to_skip = 1
            elif length == REDIS_RDB_ENC_INT16 :
                bytes_to_skip = 2
            elif length == REDIS_RDB_ENC_INT32 :
                bytes_to_skip = 4
            elif length == REDIS_RDB_ENC_LZF :
                clen = self.read_length(f)
                l = self.read_length(f)
                bytes_to_skip = clen
        else :
            bytes_to_skip = length
        
        skip(f, bytes_to_skip)

    def skip_object(self, f, enc_type):
        skip_strings = 0
        if enc_type == REDIS_RDB_TYPE_STRING :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST :
            skip_strings = self.read_length(f)
        elif enc_type == REDIS_RDB_TYPE_SET :
            skip_strings = self.read_length(f)
        elif enc_type == REDIS_RDB_TYPE_ZSET :
            skip_strings = self.read_length(f) * 2
        elif enc_type == REDIS_RDB_TYPE_HASH :
            skip_strings = self.read_length(f) * 2
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_SET_INTSET :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST :
            skip_strings = 1
        else :
            raise Exception('read_object', 'Invalid object type %d for key %s' % (enc_type, self._key))
        for x in xrange(0, skip_strings):
            self.skip_string(f)


    def read_intset(self, f) :
        raw_string, orig_raw_string = self.read_string(f, is_key = True)
        buff = StringIO(raw_string)
        encoding = read_unsigned_int(buff)[0]
        num_entries = read_unsigned_int(buff)[0]
        info = {'encoding':'intset', 
                'sizeof_value':len(raw_string),
                'orig_data_type': self._orig_data_type,
                'orig_expiry': self._orig_expiry,
                'orig_key': self._orig_key,
                'orig_raw_string': orig_raw_string
                }
        self._callback.start_set(self._key, num_entries, self._expiry, info)
        for x in xrange(0, num_entries) :
            if encoding == 8 :
                entry = read_unsigned_long(buff)[0]
            elif encoding == 4 :
                entry = read_unsigned_int(buff)[0]
            elif encoding == 2 :
                entry = read_unsigned_short(buff)[0]
            else :
                raise Exception('read_intset', 'Invalid encoding %d for key %s' % (encoding, self._key))
            self._callback.sadd(self._key, entry, None)
        self._callback.end_set(self._key)

    def read_ziplist(self, f) :
        raw_string, orig_raw_string = self.read_string(f, is_key = True)
        buff = StringIO(raw_string)
        zlbytes = read_unsigned_int(buff)[0]
        tail_offset = read_unsigned_int(buff)[0]
        num_entries = read_unsigned_short(buff)[0]
        info = {'encoding':'ziplist', 
                'sizeof_value':len(raw_string),
                'orig_data_type': self._orig_data_type,
                'orig_expiry': self._orig_expiry,
                'orig_key': self._orig_key,
                'orig_raw_string': orig_raw_string
                }
        self._callback.start_list(self._key, num_entries, self._expiry, info)
        for x in xrange(0, num_entries) :
            val = self.read_ziplist_entry(buff)
            self._callback.rpush(self._key, val)
        zlist_end = read_unsigned_char(buff)[0]
        if zlist_end != 255 : 
            raise Exception('read_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_list(self._key)

    def read_zset_from_ziplist(self, f) :
        raw_string, orig_raw_string = self.read_string(f, is_key = True)
        buff = StringIO(raw_string)
        zlbytes = read_unsigned_int(buff)[0]
        tail_offset = read_unsigned_int(buff)[0]
        num_entries = read_unsigned_short(buff)[0]
        if (num_entries % 2) :
            raise Exception('read_zset_from_ziplist', "Expected even number of elements, but found %d for key %s" % (num_entries, self._key))
        num_entries = num_entries /2
        info = {'encoding':'ziplist', 
                'sizeof_value':len(raw_string),
                'orig_data_type': self._orig_data_type,
                'orig_expiry': self._orig_expiry,
                'orig_key': self._orig_key,
                'orig_raw_string': orig_raw_string
                }
        self._callback.start_sorted_set(self._key, num_entries, self._expiry, info)
        for x in xrange(0, num_entries) :
            member = self.read_ziplist_entry(buff)
            score = self.read_ziplist_entry(buff)
            if isinstance(score, str) :
                score = float(score)
            self._callback.zadd(self._key, score, member)
        zlist_end = read_unsigned_char(buff)
        if zlist_end != 255 : 
            raise Exception('read_zset_from_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_sorted_set(self._key)

    def read_hash_from_ziplist(self, f) :
        raw_string, orig_raw_string = self.read_string(f, is_key = True)
        buff = StringIO(raw_string)
        zlbytes, _ = read_unsigned_int(buff)
        tail_offset, _ = read_unsigned_int(buff)
        num_entries, _ = read_unsigned_short(buff)
        if (num_entries % 2) :
            raise Exception('read_hash_from_ziplist', "Expected even number of elements, but found %d for key %s" % (num_entries, self._key))
        num_entries = num_entries /2
        info = {'encoding':'ziplist', 
                'sizeof_value':len(raw_string),
                'orig_data_type': self._orig_data_type,
                'orig_expiry': self._orig_expiry,
                'orig_key': self._orig_key,
                'orig_raw_string': orig_raw_string
                }
        self._callback.start_hash(self._key, num_entries, self._expiry, info)
        for x in xrange(0, num_entries) :
            field, value = None, None
            if not self._ignore_real_field:
                field = self.read_ziplist_entry(buff)
            if not self._ignore_real_value:
                value = self.read_ziplist_entry(buff)
            self._callback.hset(self._key, field, value, None)
        if not self._ignore_real_field or not self._ignore_real_value:
            zlist_end, _ = read_unsigned_char(buff)
            if zlist_end != 255 : 
                raise Exception('read_hash_from_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_hash(self._key)
    
    
    def read_ziplist_entry(self, f) :
        length = 0
        value = None
        prev_length = read_unsigned_char(f)[0]
        if prev_length == 254 :
            prev_length = read_unsigned_int(f)[0]
        entry_header = read_unsigned_char(f)[0]
        if (entry_header >> 6) == 0 :
            length = entry_header & 0x3F
            value = f.read(length)
        elif (entry_header >> 6) == 1 :
            length = ((entry_header & 0x3F) << 8) | read_unsigned_char(f)[0]
            value = f.read(length)
        elif (entry_header >> 6) == 2 :
            length = read_big_endian_unsigned_int(f)[0]
            value = f.read(length)
        elif (entry_header >> 4) == 12 :
            value = read_signed_short(f)[0]
        elif (entry_header >> 4) == 13 :
            value = read_signed_int(f)[0]
        elif (entry_header >> 4) == 14 :
            value = read_signed_long(f)[0]
        elif (entry_header == 240) :
            value = read_24bit_signed_number(f)[0]
        elif (entry_header == 254) :
            value = read_signed_char(f)[0]
        elif (entry_header >= 241 and entry_header <= 253) :
            value = entry_header - 241
        else :
            raise Exception('read_ziplist_entry', 'Invalid entry_header %d for key %s' % (entry_header, self._key))
        return value
        
    def read_zipmap(self, f) :
        raw_string, orig_raw_string = self.read_string(f)
        buff = io.BytesIO(bytearray(raw_string))
        num_entries = read_unsigned_char(buff)[0]
        info = {'encoding':'zipmap', 
                'sizeof_value':len(raw_string),
                'orig_data_type': self._orig_data_type,
                'orig_expiry': self._orig_expiry,
                'orig_key': self._orig_key,
                'orig_raw_string': orig_raw_string
                }
        self._callback.start_hash(self._key, num_entries, self._expiry, info)
        while True :
            next_length = self.read_zipmap_next_length(buff)
            if next_length is None :
                break
            key = buff.read(next_length)
            next_length = self.read_zipmap_next_length(buff)
            if next_length is None :
                raise Exception('read_zip_map', 'Unexepcted end of zip map for key %s' % self._key)        
            free = read_unsigned_char(buff)[0]
            value = buff.read(next_length)
            try:
                value = int(value)
            except ValueError:
                pass
            
            skip(buff, free)
            self._callback.hset(self._key, key, value)
        self._callback.end_hash(self._key)

    def read_zipmap_next_length(self, f) :
        num = read_unsigned_char(f)[0]
        if num < 254:
            return num
        elif num == 254:
            return read_unsigned_int(f)[0]
        else:
            return None

    def verify_magic_string(self, magic_string) :
        if magic_string != 'REDIS' :
            raise Exception('verify_magic_string', 'Invalid File Format')

    def verify_version(self, version_str) :
        version = int(version_str)
        if version < 1 or version > 6 : 
            raise Exception('verify_version', 'Invalid RDB version number %d' % version)

    def init_filter(self, filters):
        self._filters = {}
        if not filters:
            filters={}

        if not 'dbs' in filters:
            self._filters['dbs'] = None
        elif isinstance(filters['dbs'], int):
            self._filters['dbs'] = (filters['dbs'], )
        elif isinstance(filters['dbs'], list):
            self._filters['dbs'] = [int(x) for x in filters['dbs']]
        else:
            raise Exception('init_filter', 'invalid value for dbs in filter %s' %filters['dbs'])
        
        if not ('keys' in filters and filters['keys']):
            self._filters['keys'] = re.compile(".*")
        else:
            self._filters['keys'] = re.compile(filters['keys'])

        if not 'types' in filters:
            self._filters['types'] = ('set', 'hash', 'sortedset', 'string', 'list')
        elif isinstance(filters['types'], str):
            self._filters['types'] = (filters['types'], )
        elif isinstance(filters['types'], list):
            self._filters['types'] = [str(x) for x in filters['types']]
        else:
            raise Exception('init_filter', 'invalid value for types in filter %s' %filters['types'])

    def init_ignore(self, ignore):
        if not ignore:
            return

        if 'real_value' in ignore:
            self._ignore_real_value = True
        else:
            self._ignore_real_value = False

        if 'real_field' in ignore:
            self._ignore_real_field = True
        else:
            self._ignore_real_field = True

    def matches_filter(self, db_number, key=None, data_type=None):
        if self._filters['dbs'] and (not db_number in self._filters['dbs']):
            return False
        if key and (not self._filters['keys'].match(str(key))):
            return False

        if data_type is not None and (not self.get_logical_type(data_type) in self._filters['types']):
            return False
        return True
    
    def get_logical_type(self, data_type):
        return DATA_TYPE_MAPPING[data_type]
        
    # we can use https://github.com/teepark/python-lzf
    # it is faster than pure python function below
    def lzf_decompress(self, compressed, expected_length):
        in_stream = bytearray(compressed)
        in_len = len(in_stream)
        in_index = 0
        out_stream = bytearray()
        out_index = 0
    
        while in_index < in_len :
            ctrl = in_stream[in_index]
            if not isinstance(ctrl, int) :
                raise Exception('lzf_decompress', 'ctrl should be a number %s for key %s' % (str(ctrl), self._key))
            in_index = in_index + 1
            if ctrl < 32 :
                for x in xrange(0, ctrl + 1) :
                    out_stream.append(in_stream[in_index])
                    #sys.stdout.write(chr(in_stream[in_index]))
                    in_index = in_index + 1
                    out_index = out_index + 1
            else :
                length = ctrl >> 5
                if length == 7 :
                    length = length + in_stream[in_index]
                    in_index = in_index + 1
                
                ref = out_index - ((ctrl & 0x1f) << 8) - in_stream[in_index] - 1
                in_index = in_index + 1
                for x in xrange(0, length + 2) :
                    out_stream.append(out_stream[ref])
                    ref = ref + 1
                    out_index = out_index + 1
        if len(out_stream) != expected_length :
            raise Exception('lzf_decompress', 'Expected lengths do not match %d != %d for key %s' % (len(out_stream), expected_length, self._key))
        return str(out_stream)

def skip(f, free):
    if free :
        f.read(free)

def ntohl(f) :
    val, orig_val = read_unsigned_int(f)
    new_val = 0
    new_val = new_val | ((val & 0x000000ff) << 24)
    new_val = new_val | ((val & 0xff000000) >> 24)
    new_val = new_val | ((val & 0x0000ff00) << 8)
    new_val = new_val | ((val & 0x00ff0000) >> 8)
    return new_val, orig_val

#def to_datetime(usecs_since_epoch):
#    seconds_since_epoch = usecs_since_epoch / 1000000
#    useconds = usecs_since_epoch % 1000000
#    dt = datetime.datetime.utcfromtimestamp(seconds_since_epoch)
#    delta = datetime.timedelta(microseconds = useconds)
#    return dt + delta
 
def to_datetime(usecs_since_epoch):
    seconds_since_epoch = usecs_since_epoch / 1000000
    return seconds_since_epoch
    
def read_signed_char(f) :
    data = f.read(1)
    return struct.unpack('b', data)[0], data
    
def read_unsigned_char(f) :
    data = f.read(1)
    return struct.unpack('B', data)[0], data

def read_signed_short(f) :
    data = f.read(2)
    return struct.unpack('h', data)[0], data
        
def read_unsigned_short(f) :
    data = f.read(2)
    return struct.unpack('H', data)[0], data

def read_signed_int(f) :
    data = f.read(4)
    return struct.unpack('i', data)[0], data
    
def read_unsigned_int(f) :
    data = f.read(4)
    return struct.unpack('I', data)[0], data

def read_big_endian_unsigned_int(f):
    data = f.read(4)
    return struct.unpack('>I', data)[0], data

def read_24bit_signed_number(f):
    data = f.read(3)
    s = '0' + data
    num = struct.unpack('i', s)[0]
    return num >> 8, data
    
def read_signed_long(f) :
    data = f.read(8)
    return struct.unpack('q', data)[0], data
    
def read_unsigned_long(f) :
    data = f.read(8)
    return struct.unpack('Q', data)[0], data

def string_as_hexcode(string) :
    for s in string :
        if isinstance(s, int) :
            print(hex(s))
        else :
            print(hex(ord(s)))



