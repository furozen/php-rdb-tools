<?php
/**
 * Created by Andy Malahovsky
 * Date: 02.09.14
 * Time: 16:13
 */

class RDBParserConst
{

    const REDIS_RDB_6BITLEN = 0;
    const REDIS_RDB_14BITLEN = 1;
    const REDIS_RDB_32BITLEN = 2;
    const REDIS_RDB_ENCVAL = 3;

    const REDIS_RDB_OPCODE_EXPIRETIME_MS = 252;
    const REDIS_RDB_OPCODE_EXPIRETIME = 253;
    const REDIS_RDB_OPCODE_SELECTDB = 254;
    const REDIS_RDB_OPCODE_EOF = 255;

    const REDIS_RDB_TYPE_STRING = 0;
    const REDIS_RDB_TYPE_LIST = 1;
    const REDIS_RDB_TYPE_SET = 2;
    const REDIS_RDB_TYPE_ZSET = 3;
    const REDIS_RDB_TYPE_HASH = 4;
    const REDIS_RDB_TYPE_HASH_ZIPMAP = 9;
    const REDIS_RDB_TYPE_LIST_ZIPLIST = 10;
    const REDIS_RDB_TYPE_SET_INTSET = 11;
    const REDIS_RDB_TYPE_ZSET_ZIPLIST = 12;
    const REDIS_RDB_TYPE_HASH_ZIPLIST = 13;

    const REDIS_RDB_ENC_INT8 = 0;
    const REDIS_RDB_ENC_INT16 = 1;
    const REDIS_RDB_ENC_INT32 = 2;
    const REDIS_RDB_ENC_LZF = 3;

    public static $DATA_TYPE_MAPPING = array(
        0 => "string",
        1 => "list",
        2 => "set",
        3 => "sortedset",
        4 => "hash",
        9 => "hash",
        10 => "list",
        11 => "set",
        12 => "sortedset",
        13 => "hash");
}

/**
 * A Callback to handle events as the Redis dump file is parsed.
 * This callback provides a serial and fast access to the dump file.
 */
class RdbCallback
{

    public function  start_rdb() {

        //Called once we know we are dealing with a valid redis dump file

    }

    /**
     * Called to indicate database the start of database `db_number`
     *
     * Once a database starts, another database cannot start unless
     * the first one completes and then `end_database` method is called
     *
     * Typically, callbacks store the current database number in a class variable*/
    public function start_database($db_number) {


    }

    /**
     * Callback to handle a key with a string value and an optional expiry
     *
     * `key` is the redis key
     * `value` is a string or a number
     * `expiry` is a datetime object. None and can be None
     * `info` is a dictionary containing additional information about this object.
     */
    public function set($key, $value, $expiry, $info) {

    }

    /**Callback to handle the start of a hash
     *
     * `key` is the redis key
     * `length` is the number of elements in this hash.
     * `expiry` is a `datetime` object. None means the object does not expire
     * `info` is a dictionary containing additional information about this object.
     *
     * After `start_hash`, the method `hset` will be called with this `key` exactly `length` times.
     * After that, the `end_hash` method will be called.
     */
    public function start_hash($key, $length, $expiry, $info) {

    }

    /**
     * Callback to insert a field=value pair in an existing hash
     *
     * `key` is the redis key for this hash
     * `field` is a string
     * `value` is the value to store for this field
     */
    public function hset($key, $field, $value) {

    }

    /**
     * Called when there are no more elements in the hash
     *
     * `key` is the redis key for the hash
     */
    public function end_hash($key) {

    }

    /**
     * Callback to handle the start of a hash
     *
     * `key` is the redis key
     * `cardinality` is the number of elements in this set
     * `expiry` is a `datetime` object. None means the object does not expire
     * `info` is a dictionary containing additional information about this object.
     *
     * After `start_set`, the  method `sadd` will be called with `key` exactly `cardinality` times
     * After that, the `end_set` method will be called to indicate the end of the set.
     *
     * Note : This callback handles both Int Sets and Regular Sets
     */
    public function start_set($key, $cardinality, $expiry, $info) {

    }

    /**
     * Callback to inser a new member to this set
     *
     * `key` is the redis key for this set
     * `member` is the member to insert into this set
     */
    public function sadd($key, $member) {

    }

    /**
     * Called when there are no more elements in this set
     *
     * `key` the redis key for this set
     */
    public function end_set($key) {

    }

    /**
     * Callback to handle the start of a list
     *
     * `key` is the redis key for this list
     * `length` is the number of elements in this list
     * `expiry` is a `datetime` object. None means the object does not expire
     * `info` is a dictionary containing additional information about this object.
     *
     * After `start_list`, the method `rPush` will be called with `key` exactly `length` times
     * After that, the `end_list` method will be called to indicate the end of the list
     *
     * Note : This callback handles both Zip Lists and Linked Lists.
     */
    public function start_list($key, $length, $expiry, $info) {

    }

    /**
     * Callback to insert a new value into this list
     *
     * `key` is the redis key for this list
     * `value` is the value to be inserted
     *
     * Elements must be inserted to the end (i.e. tail) of the existing list.
     */
    public function rPush($key, $value) {

    }

    /**
     * Called when there are no more elements in this list
     *
     * `key` the redis key for this list
     */
    public function end_list($key) {

    }

    /**
     * Callback to handle the start of a sorted set
     *
     * `key` is the redis key for this sorted
     * `length` is the number of elements in this sorted set
     * `expiry` is a `datetime` object. None means the object does not expire
     * `info` is a dictionary containing additional information about this object.
     *
     * After `start_sorted_set`, the method `zadd` will be called with `key` exactly `length` times.
     * Also, `zadd` will be called in a sorted order, so as to preserve the ordering of this sorted set.
     * After that, the `end_sorted_set` method will be called to indicate the end of this sorted set
     *
     * Note : This callback handles sorted sets in that are stored as ziplists or skiplists
     */
    public function start_sorted_set($key, $length, $expiry, $info) {

    }

    /**Callback to insert a new value into this sorted set
     *
     * `key` is the redis key for this sorted set
     * `score` is the score for this `value`
     * `value` is the element being inserted
     * */
    public function zadd($key, $score, $member) {

    }

    /**
     * Called when there are no more elements in this sorted set
     *
     * `key` is the redis key for this sorted set
     */
    public function end_sorted_set($key) {

    }

    /**
     * Called when the current database ends
     *
     * After `end_database`, one of the methods are called -
     * 1) `start_database` with a new database number
     * OR
     * 2) `end_rdb` to indicate we have reached the end of the file
     */
    public function end_database($db_number) {

    }

    /**Called to indicate we have completed parsing of the dump file/**
     * pass
     */
    public function end_rdb() {
    }
}

class RdbParser
{
    /**
     * A Parser for Redis RDB Files
     *
     * This class is similar in spirit to a SAX parser for XML files.
     * The dump file is parsed sequentially. As and when objects are discovered,
     * appropriate methods in the callback are called.
     *
     * Typical usage :
     * callback = MyRdbCallback() # Typically a subclass of RdbCallback
     * parser = RdbParser(callback)
     * parser.parse('/var/redis/6379/dump.rdb')
     *
     * filter is a dictionary with the following keys
     * {"dbs" : [0, 1], "keys" : "foo.*", "types" : ["hash", "set", "sortedset", "list", "string"]}
     *
     * If filter is None, results will not be filtered
     * If dbs, keys or types is None or Empty, no filtering will be done on that axis
     */
    /**
     * @var RdbCallback
     */
    public $_callback;
    public $_key;
    public $_expiry;
    public $_filters;

    public function __construct($callback, $filters = '') {
        /**
         * `callback` is the object that will receive parse events
         */

        $this->_callback = $callback;
        $this->_key = '';
        $this->_expiry = '';
        $this->init_filter($filters);
    }

    /**
     * Parse a redis rdb dump file, and call methods in the
     * callback object during the parsing operation.
     */
    public function parse($filename) {
        $f = fopen($filename, 'rb');
        if (feof($f)) return false;

        $this->verify_magic_string(fread($f, 5));
        $this->verify_version(fread($f, 4));
        $this->_callback->start_rdb();

        $is_first_database = true;
        $db_number = 0;
        while (true) {
            $this->_expiry = '';
            $data_type = read_unsigned_char($f);

            if ($data_type == RDBParserConst::REDIS_RDB_OPCODE_EXPIRETIME_MS) {
                $this->_expiry = to_datetime(read_unsigned_long($f) * 1000);
                $data_type = read_unsigned_char($f);
            } elseif ($data_type == RDBParserConst::REDIS_RDB_OPCODE_EXPIRETIME) {
                $this->_expiry = to_datetime(read_unsigned_int($f) * 1000000);
                $data_type = read_unsigned_char($f);
            }
            if ($data_type == RDBParserConst::REDIS_RDB_OPCODE_SELECTDB) {
                if (!$is_first_database) {
                    $this->_callback->end_database($db_number);
                }
                $is_first_database = false;
                $db_number = $this->read_length($f);
                $this->_callback->start_database($db_number);
                continue;
            }

            if ($data_type == RDBParserConst::REDIS_RDB_OPCODE_EOF) {
                $this->_callback->end_database($db_number);
                $this->_callback->end_rdb();
                break;
            }

            if ($this->matches_filter($db_number)) {
                $this->_key = $this->read_string($f);

                if ($this->matches_filter($db_number, $this->_key, $data_type)) {
                    $this->read_object($f, $data_type);
                } else {
                    $this->skip_object($f, $data_type);
                }
            } else {
                $this->skip_key_and_object($f, $data_type);
            }
        }
    }

    public function read_length_with_encoding($f) {
        $length = 0;
        $is_encoded = false;
        $bytes = array();
        $read_unsigned_char = read_unsigned_char($f);
        $bytes[] =  $read_unsigned_char;
        $enc_type = ($bytes[0] & 0xC0) >> 6;
        if ($enc_type == RDBParserConst::REDIS_RDB_ENCVAL) {
            $is_encoded = true;
            $length = $bytes[0] & 0x3F;
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_6BITLEN) {
            $length = $bytes[0] & 0x3F;
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_14BITLEN) {
            $bytes[] =  read_unsigned_char($f);
            $length = (($bytes[0] & 0x3F) << 8) | $bytes[1];
        } else {
            $length = ntohl($f);
        }
        return array($length, $is_encoded);
    }

    public function read_length($f) {
        $read_length_with_encoding = $this->read_length_with_encoding($f);
        return $read_length_with_encoding[0];
    }

    public function read_string($f) {
        $tup = $this->read_length_with_encoding($f);
        $length = $tup[0];
        $is_encoded = $tup[1];
        $val = '';
        if ($is_encoded) {
            if ($length == RDBParserConst::REDIS_RDB_ENC_INT8) {
                $val = read_signed_char($f);
            } elseif ($length == RDBParserConst::REDIS_RDB_ENC_INT16) {
                $val = read_signed_short($f);
            } elseif ($length == RDBParserConst::REDIS_RDB_ENC_INT32) {
                $val = read_signed_int($f);
            } elseif ($length == RDBParserConst::REDIS_RDB_ENC_LZF) {
                $clen = $this->read_length($f);
                $l = $this->read_length($f);
                $val = $this->lzf_decompress(fread($f, $clen), $l);
            }
        } else {
            $val = fread($f, $length);
        }
        return $val;
    }

    /**
     * # Read an object for the stream
     * # f is the redis file
     * # enc_type is the type of object
     */
    public function read_object($f, $enc_type) {
        if ($enc_type == RDBParserConst::REDIS_RDB_TYPE_STRING) {
            $val = $this->read_string($f);
            $this->_callback->set($this->_key, $val, $this->_expiry, $info = array('encoding' => 'string'));

        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_LIST) {
            # A redis list is just a sequence of strings
            # We successively read strings from the stream and create a list from it
            # The lists are in order i.e. the first string is the head,
            # and the last string is the tail of the list
            $length = $this->read_length($f);
            $this->_callback->start_list($this->_key, $length, $this->_expiry, $info = array('encoding' => 'linkedlist'));
            for ($count = 0; $count < $length; $count++) {
                $val = $this->read_string($f);
                $this->_callback->rPush($this->_key, $val);
            }
            $this->_callback->end_list($this->_key);
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_SET) {
            # A redis list is just a sequence of strings
            # We successively read strings from the stream and create a set from it
            # Note that the order of strings is non-deterministic
            $length = $this->read_length($f);
            $this->_callback->start_set($this->_key, $length, $this->_expiry, $info = array('encoding' => 'hashtable'));
            for ($count = 0; $count < $length; $count++) {
                $val = $this->read_string($f);
                $this->_callback->sadd($this->_key, $val);
            }
            $this->_callback->end_set($this->_key);
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_ZSET) {
            $length = $this->read_length($f);
            $this->_callback->start_sorted_set($this->_key, $length, $this->_expiry, $info = array('encoding' => 'skiplist'));
            for ($count = 0; $count < $length; $count++) {
                $val = $this->read_string($f);
                $dbl_length = read_unsigned_char($f);
                $score = fread($f, $dbl_length);

                if(is_string($score)){
                    $score = (float)$score;
                }

                $this->_callback->zadd($this->_key, $score, $val);
            }
            $this->_callback->end_sorted_set($this->_key);
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_HASH) {
            $length = $this->read_length($f);
            $this->_callback->start_hash($this->_key, $length, $this->_expiry, $info = array('encoding' => 'hashtable'));
            for ($count = 0; $count < $length; $count++) {
                $field = $this->read_string($f);
                $value = $this->read_string($f);
                $this->_callback->hset($this->_key, $field, $value);
            }
            $this->_callback->end_hash($this->_key);
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_HASH_ZIPMAP) {
            $this->read_zipmap($f);
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_LIST_ZIPLIST) {
            $this->read_ziplist($f);
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_SET_INTSET) {
            $this->read_intset($f);
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_ZSET_ZIPLIST) {
            $this->read_zset_from_ziplist($f);
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_HASH_ZIPLIST) {
            $this->read_hash_from_ziplist($f);
        } else {
            throw new Exception('read_object : Invalid object type ' . $enc_type . ' for key %s' . $this->_key);
        }
    }


    public function skip_key_and_object($f, $data_type) {
        $this->skip_string($f);
        $this->skip_object($f, $data_type);
    }

    public function skip_string($f) {
        $tup = $this->read_length_with_encoding($f);
        $length = $tup[0];
        $is_encoded = $tup[1];
        $bytes_to_skip = 0;
        if ($is_encoded) {
            if ($length == RDBParserConst::REDIS_RDB_ENC_INT8) {
                $bytes_to_skip = 1;
            } elseif ($length == RDBParserConst::REDIS_RDB_ENC_INT16) {
                $bytes_to_skip = 2;
            } elseif ($length == RDBParserConst::REDIS_RDB_ENC_INT32) {
                $bytes_to_skip = 4;
            } elseif ($length == RDBParserConst::REDIS_RDB_ENC_LZF) {
                $clen = $this->read_length($f);
                $l = $this->read_length($f);
                $bytes_to_skip = $clen;
            }
        } else {
            $bytes_to_skip = $length;
        }
        skip($f, $bytes_to_skip);
    }

    public function skip_object($f, $enc_type) {
        $skip_strings = 0;
        if ($enc_type == RDBParserConst::REDIS_RDB_TYPE_STRING) {
            $skip_strings = 1;
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_LIST) {
            $skip_strings = $this->read_length($f);
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_SET) {
            $skip_strings = $this->read_length($f);
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_ZSET) {
            $skip_strings = $this->read_length($f) * 2;
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_HASH) {
            $skip_strings = $this->read_length($f) * 2;
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_HASH_ZIPMAP) {
            $skip_strings = 1;
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_LIST_ZIPLIST) {
            $skip_strings = 1;
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_SET_INTSET) {
            $skip_strings = 1;
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_ZSET_ZIPLIST) {
            $skip_strings = 1;
        } elseif ($enc_type == RDBParserConst::REDIS_RDB_TYPE_HASH_ZIPLIST) {
            $skip_strings = 1;
        } else {
            throw new Exception('read_object :Invalid object type ' . $enc_type . ' for key %s' . $this->_key);
        }
        for ($count = 0; $count < $skip_strings; $count++) {

            $this->skip_string($f);
        }
    }

    public function read_intset($f) {
        $raw_string = $this->read_string($f);
        $buff = $this->getByteIO($raw_string);
        $encoding = read_unsigned_int($buff);
        $num_entries = read_unsigned_int($buff);
        $this->_callback->start_set($this->_key, $num_entries, $this->_expiry, $info = array('encoding' => 'intset', 'sizeof_value' => strlen($raw_string)));
        for ($x = 0; $x < $num_entries; $x++) {

            if ($encoding == 8) {
                $entry = read_unsigned_long($buff);
            } elseif ($encoding == 4) {
                $entry = read_unsigned_int($buff);
            } elseif ($encoding == 2) {
                $entry = read_unsigned_short($buff);
            } else {
                throw new Exception('read_intset :Invalid encoding ' . $encoding . ' for key %s' . $this->_key);
            }
            $this->_callback->sadd($this->_key, $entry);
        }
        $this->_callback->end_set($this->_key);
    }

    public function read_ziplist($f) {
        $raw_string = $this->read_string($f);
        $buff = $this->getByteIO($raw_string);
        $zlbytes = read_unsigned_int($buff);
        $tail_offset = read_unsigned_int($buff);
        $num_entries = read_unsigned_short($buff);
        $this->_callback->start_list($this->_key, $num_entries, $this->_expiry, $info = array('encoding' => 'ziplist', 'sizeof_value' => strlen($raw_string)));
        for ($x = 0; $x < $num_entries; $x++) {
            $val = $this->read_ziplist_entry($buff);
            $this->_callback->rPush($this->_key, $val);
        }
        $zlist_end = read_unsigned_char($buff);
        if ($zlist_end != 255) {
            throw new Exception('read_ziplist : Invalid zip list end - '.$zlist_end.' for key %s' . $this->_key);
        }
        $this->_callback->end_list($this->_key);
    }

    public function read_zset_from_ziplist($f) {
        $raw_string = $this->read_string($f);
        $buff = $this->getByteIO($raw_string);
        $zlbytes = read_unsigned_int($buff);
        $tail_offset = read_unsigned_int($buff);
        $num_entries = read_unsigned_short($buff);
        if ($num_entries % 2) {
            throw new Exception('read_zset_from_ziplist : Expected even number of elements, but found '.$num_entries.' for key %s' . $this->_key);
        }
        $num_entries = $num_entries / 2;
        $this->_callback->start_sorted_set($this->_key, $num_entries, $this->_expiry, $info = array('encoding' => 'ziplist', 'sizeof_value' => strlen($raw_string)));
        for ($x = 0; $x < $num_entries; $x++) {
            $member = $this->read_ziplist_entry($buff);
            $score = $this->read_ziplist_entry($buff);

            if(is_string($score)){
                $score = (float)$score;
            }

            $this->_callback->zadd($this->_key, $score, $member);
        }
        $zlist_end = read_unsigned_char($buff);
        if ($zlist_end != 255) {
            throw new Exception('read_ziplist : Invalid zip list end - '.$zlist_end.' for key ' . $this->_key);
        }
        $this->_callback->end_sorted_set($this->_key);
    }

    public function read_hash_from_ziplist($f) {
        $raw_string = $this->read_string($f);
        $buff = $this->getByteIO($raw_string);
        $zlbytes = read_unsigned_int($buff);
        $tail_offset = read_unsigned_int($buff);
        $num_entries = read_unsigned_short($buff);
        if ($num_entries % 2) {
            throw new Exception('read_hash_from_ziplist : Expected even number of elements, but found '.$num_entries.' for key ' . $this->_key);
        }

        $num_entries = $num_entries / 2;
        $this->_callback->start_hash($this->_key, $num_entries, $this->_expiry, $info = array('encoding' => 'ziplist', 'sizeof_value' => strlen($raw_string)));
        for ($x = 0; $x < $num_entries; $x++) {
            $field = $this->read_ziplist_entry($buff);
            $value = $this->read_ziplist_entry($buff);
            $this->_callback->hset($this->_key, $field, $value);
        }
        $zlist_end = read_unsigned_char($buff);
        if ($zlist_end != 255) {
            throw new Exception('read_hash_from_ziplist : Invalid zip list end - '.$zlist_end.' for key %s' . $this->_key);
        }

        $this->_callback->end_hash($this->_key);
    }

    public function read_ziplist_entry($f) {
        $length = 0;
        $value = '';
        $prev_length = read_unsigned_char($f);
        if ($prev_length == 254) {
            $prev_length = read_unsigned_int($f);
        }
        $entry_header = read_unsigned_char($f);
        if (($entry_header >> 6) == 0) {
            $length = $entry_header & 0x3F;
            $value = fread($f, $length);
        } elseif (($entry_header >> 6) == 1) {
            $length = (($entry_header & 0x3F) << 8) | read_unsigned_char($f);
            $value = fread($f, $length);
        } elseif (($entry_header >> 6) == 2) {
            $length = read_big_endian_unsigned_int($f);
            $value = fread($f, $length);
        } elseif (($entry_header >> 4) == 12) {
            $value = read_signed_short($f);
        } elseif (($entry_header >> 4) == 13) {
            $value = read_signed_int($f);
        } elseif (($entry_header >> 4) == 14) {
            $value = read_signed_long($f);
        } elseif ($entry_header == 240) {
            $value = read_24bit_signed_number($f);
        } elseif ($entry_header == 254) {
            $value = read_signed_char($f);
        } elseif ($entry_header >= 241 and $entry_header <= 253) {
            $value = $entry_header - 241;
        } else {

            throw new Exception('read_ziplist_entry : Invalid zip list end - '.$entry_header.' for key %s' . $this->_key);

        }
        return $value;
    }

    public function read_zipmap($f) {
        $raw_string = $this->read_string($f);
        $buff = $this->getByteIO($raw_string);
        $num_entries = read_unsigned_char($buff);
        $this->_callback->start_hash($this->_key, $num_entries, $this->_expiry, $info = array('encoding' => 'zipmap', 'sizeof_value' => strlen($raw_string)));
        while (true) {
            $next_length = $this->read_zipmap_next_length($buff);
            if (empty($next_length)) {
                break;
            }
            $key = fread($buff,$next_length);
            $next_length = $this->read_zipmap_next_length($buff);
            if (empty($next_length)) {
                throw new Exception('read_zip_map :Unexepcted end of zip map for key ' . $this->_key);
            }

            $free = read_unsigned_char($buff);
            $value = fread($buff,$next_length);
            if(is_numeric(($value)))
                $value = (int)$value;


            skip($buff, $free);
            $this->_callback->hset($this->_key, $key, $value);
        }
        $this->_callback->end_hash($this->_key);
    }

    public function read_zipmap_next_length($f) {
        $num = read_unsigned_char($f);
        if ($num < 254)
            return $num;
        elseif ($num == 254)
            return read_unsigned_int($f);
        else
            return '';
    }

    public function verify_magic_string($magic_string) {
        if ($magic_string != 'REDIS') {
            throw new Exception('verify_magic_string :Invalid File Format');
        }
    }

    public function verify_version($version_str) {
        $version = (int)$version_str;
        if ($version < 1 or $version > 6) {
            throw new  Exception('verify_version :Invalid RDB version number ' . $version);
        }
    }

    public function init_filter($filters) {
        $this->_filters = array();
        if (empty($filters)) {
            $filters = array();
        }
        if (empty($filters['dbs'])) {
            $this->_filters['dbs'] = array();
        } elseif (is_numeric($filters['dbs'])) {
            $this->_filters['dbs'] = array($filters['dbs']);
        } elseif (is_string($filters['dbs'])) {
            $this->_filters['dbs'] = explode(',', $filters['dbs']);
        } elseif (is_array($filters['dbs'])) {
            $this->_filters['dbs'] = $filters['dbs'];
        } else {
            throw new Exception('init_filter :invalid value for dbs in filter  ' . print_r($filters['dbs'],1));
        }


        if (empty($filters['keys'])) {
            $this->_filters['keys'] = ".*";
        } else {
            $this->_filters['keys'] = $filters['keys'];
        }


        if (empty($filters['types'])) {
            $this->_filters['types'] = array('set', 'hash', 'sortedset', 'string', 'list');
        } elseif (is_string($filters['types'])) {
            $this->_filters['types'] = explode(',', $filters['types']);
        }elseif(is_array($filters['types'])){
            $this->_filters['types'] = $filters['types'];
        } else {
            throw new Exception('init_filter :invalid value for types in filter ' . print_r($filters['types'],1));
        }
    }

    public function matches_filter($db_number, $key = '', $data_type = '') {

        if (!empty($this->_filters['dbs']) && !in_array($db_number, $this->_filters['dbs'])) {
            return false;
        }

        if (!empty($key) && ! preg_match('/'.$this->_filters['keys'].'/',$key )) {
            return false;
        }
        //if data_type is not None and (not self.get_logical_type(data_type) in self._filters['types']):
        if ($data_type!=='' && !in_array($this->get_logical_type($data_type), $this->_filters['types'])) {
            return false;
        }
        return true;
    }

    public function get_logical_type($data_type) {
        return RDBParserConst::$DATA_TYPE_MAPPING[$data_type];
    }

    public function lzf_decompress($compressed, $expected_length) {
        $in_stream = array_values(unpack('C*', $compressed));
        $in_len = count($in_stream);
        $in_index = 0;
        $out_stream = array();
        $out_index = 0;

        while ($in_index < $in_len) {
            $ctrl = $in_stream[$in_index];
            if (!is_numeric($ctrl)) {
                throw new Exception('lzf_decompress :ctrl should be a number ' . $ctrl . ' for key ' . $this->_key);
            }
            $in_index = $in_index + 1;
            if ($ctrl < 32) {
                for ($x = 0; $x < $ctrl + 1; $x++) {
                    $out_stream[] = $in_stream[$in_index];

                    $in_index = $in_index + 1;
                    $out_index = $out_index + 1;
                }
            } else {
                $length = $ctrl >> 5;
                if ($length == 7) {
                    $length = $length + $in_stream[$in_index];
                    $in_index = $in_index + 1;
                }
                $ref = $out_index - (($ctrl & 0x1f) << 8) - $in_stream[$in_index] - 1;
                $in_index = $in_index + 1;
                for ($x = 0; $x < $length + 2; $x++) {
                    $out_stream[] = $out_stream[$ref];
                    $ref = $ref + 1;
                    $out_index = $out_index + 1;
                }
            }
        }
        $out_string='';
        foreach($out_stream  as $ch){
            $out_string.=pack('C',$ch);
        }
        if (strlen($out_string) != $expected_length) {
            throw new Exception('lzf_decompress :Expected lengths do not match ' . strlen($out_stream) . ' != ' . $expected_length . ' for key ' . $this->_key);
        }
        return $out_string;
    }

    /**
     * @param $raw_string
     * @return resource
     */
    private function getByteIO($raw_string) {
        $buff = tmpfile();
        fwrite($buff, $raw_string);
        fseek($buff, 0);
        return $buff;
    }
}


function to_datetime($usecs_since_epoch) {
    $seconds_since_epoch = $usecs_since_epoch / 1000000;
    $useconds = $usecs_since_epoch % 1000000;
    $date = new MDateTime();


    $date->setTimestamp((int)$seconds_since_epoch);

    $date->microsec = $useconds;
    return $date;
}

function skip($f, $free) {
    if ((int)$free) fread($f, $free);
}

function ntohl($f) {
    $val = read_unsigned_int($f);
    $new_val = 0;
    $new_val = $new_val | (($val & 0x000000ff) << 24);
    $new_val = $new_val | (($val & 0xff000000) >> 24);
    $new_val = $new_val | (($val & 0x0000ff00) << 8);
    $new_val = $new_val | (($val & 0x00ff0000) >> 8);
    return $new_val;
}

function read_signed_char($f) {
    return unpack('c', fread($f, 1))[1];
}

function read_unsigned_char($f) {
    $unpack = unpack('C', fread($f, 1));
    $var = $unpack[1];
    return $var;
}

function read_signed_short($f) {
    return unpack('s', fread($f, 2))[1];
}

function read_unsigned_short($f) {
    return unpack('S', fread($f, 2))[1];
}

function read_signed_int($f) {
    return unpack('l', fread($f, 4))[1];
}

function read_unsigned_int($f) {
    return unpack('L', fread($f, 4))[1];
}

function read_big_endian_unsigned_int($f) {
    return unpack('N', fread($f, 4))[1];
}

function read_24bit_signed_number($f) {
    $s = '0' . fread($f, 3);
    $num = unpack('l', $s)[1];
    return $num >> 8;
}

function read_signed_long($f) {
    $a = read_unsigned_int($f);
    $b = read_signed_int($f);

    return $b << 32 | $a;
}

function read_unsigned_long($f) {
    //1671963072573000
    $a = read_unsigned_int($f);
    $b = read_unsigned_int($f);

    return $b << 32 | $a;
}

function string_as_hexcode($string) {
    for ($i = 0; $i < strlen($string); $i++) {
        if (is_numeric($string[$i])) {
            echo dechex($string[$i]);
        } else {
            echo dechex(ord($string[$i]));

        }
    }

}

class StringIO{

    public $pointer=0;
    private $buff;
    function __construct($string){
        $this->buff=unpack('C*', $string);
    }
    function read($num){
        $s='';
        for($i=0;$i<$num;$i++){

           $s.=$this->buff[$this->pointer];
           $this->pointer++;
        }
        return $s;
    }

}



class MDateTime extends DateTime
{
    public $microsec;

    public function __construct($time = 'now', DateTimeZone $timezone = null) {
        parent::__construct($time, new DateTimeZone('Europe/London'));
    }
}


class DebugCallback extends RdbCallback
{
    public function start_rdb() {
        print('[');
    }

    public function start_database($db_number) {
        print('{');
    }

    public function set($key, $value, $expiry) {
        printf('"%s" : "%s"', $key, $value);
    }

    public function start_hash($key, $length, $expiry) {
        printf('"%s" : {', $key);

    }

    public function hset($key, $field, $value) {
        printf('"%s" : "%s"', $field, $value);
    }

    public function end_hash($key) {
        print('}');
    }

    public function start_set($key, $cardinality, $expiry) {
        printf('"%s" : [', $key);
    }

    public function sadd($key, $member) {
        printf('"%s"', $member);
    }

    public function end_set($key) {
        print(']');
    }

    public function start_list($key, $length, $expiry) {
        printf('"%s" : [', $key);
    }

    public function rPush($key, $value) {
        printf('"%s"', $value);
    }

    public function end_list($key) {
        print(']');
    }

    public function start_sorted_set($key, $length, $expiry) {
        printf('"%s" : {', $key);
    }

    public function zadd($key, $score, $member) {
        printf('"%s" : "%s"', $member, $score);
    }

    public function end_sorted_set($key) {
        print('}');
    }

    public function end_database($db_number) {
        print('}');
    }

    public function end_rdb() {
        print(']');
    }
}