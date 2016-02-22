# coding:utf-8
import parser
import time

class WriteRdbCallback(parser.RdbCallback) :
    def __init__(self, f_name='./new_dump.rdb'):
        self.f_name = f_name

    def write_rdb(self, data):
        self.f.write(data)
    
    # write rdb file standard format
    def write_magic_num(self):
        self.f.write('REDIS0006')
    
    def write_db_number(self, num):
        self.f.write('\xfe')
        self.f.write(num)
    
    # write end of rdb and close file
    def write_rdb_eof(self):
        self.f.write('\xff')
        self.f.close()
    ########################################################

    def start_rdb(self):
        self.f = open(self.f_name, 'wb')
        self.write_magic_num()
    
    def start_database(self, db_number, info):
        orig_db_number = info['orig_db_number']
        self.write_db_number(orig_db_number)
    
    def set(self, key, value, expiry, info):
        is_write_str = False
        _str_data = ''
        if info['orig_expiry']:
            _str_data += info['orig_expiry']
        _str_data += info['orig_data_type']
        _str_data += info['orig_key']
        _str_data += info['orig_val']
        if True:
            is_write_str = True
        if is_write_str:
            self.write_rdb(_str_data)

    def start_hash(self, key, length, expiry, info):
        self.is_write_hash = False
        self._hash_data = ''
        if info['orig_expiry']:
            self._hash_data += info['orig_expiry']
        self._hash_data += info['orig_data_type']
        self._hash_data += info['orig_key']
        if 'orig_raw_string' in info:
            self._hash_data += info['orig_raw_string']
        else:
            self._hash_data += info['orig_length']
    
    def hset(self, key, field, value, info):
        self.is_write_hash = True
        if info:
            self._hash_data += info['orig_field']
            self._hash_data += info['orig_value']
    
    def end_hash(self, key):
        if self.is_write_hash:
            self.write_rdb(self._hash_data)
        self.is_write_hash = False
        self._hash_data = ''
    
    def start_set(self, key, cardinality, expiry, info):
        self.is_write_set = False
        self._set_data = ''
        if info['orig_expiry']:
            self._hash_data += info['orig_expiry']
        self._hash_data += info['orig_data_type']
        self._hash_data += info['orig_key']
        if 'orig_raw_string' in info:
            self._hash_data += info['orig_raw_string']
        else:
            self._hash_data += info['orig_length']
 
    def sadd(self, key, member, info):
        self.is_write_set = True
        if info:
            self._set_data += info['orig_val']
 
    def end_set(self, key):
        if self.is_write_set:
            self.write_rdb(self._set_data)
        self.is_write_set = False
        self._set_data = ''
    
    def start_list(self, key, length, expiry, info):
        pass
    
    def rpush(self, key, value) :
        pass
    
    def end_list(self, key):
        pass
    
    def start_sorted_set(self, key, length, expiry, info):
        pass
    
    def zadd(self, key, score, member, info):
        pass
    
    def end_sorted_set(self, key):
        pass
    
    def end_database(self, db_number, info):
        self.write_rdb(info['orig_end_db'])
    
    def end_rdb(self):
        self.write_rdb_eof()

if __name__ == '__main__':

    start_time = time.clock()

    file_name = './old_dump.rdb'
    
    # 可以自定义新rdb文件名
    # f_name = './new_dump.rdb'
    # write_rdb_callback = WriteRdbCallback(f_name)

    write_rdb_callback = WriteRdbCallback()

    ignore = ["real_value", "real_field"]
    rdb_parser = parser.RdbParser(write_rdb_callback, ignore = ignore)

    rdb_parser.parse(file_name)

    finish_time = time.clock()

    print 'used:', finish_time - start_time

