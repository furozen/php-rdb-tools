<?php
/**
 * Created by Andy Malahovsky
 * Date: 03.09.14
 * Time: 18:10
 */


require_once('simpletest/autorun.php');



require_once  realpath(__DIR__.'/../rdbtools/RDBParser.php');

class TestRDBParser extends UnitTestCase
{

    function test_empty_rdb() {
        $r = $this->load_rdb('empty_database.rdb');
        $this->assertTrue(in_array('start_rdb', $r->methods_called));
        $this->assertTrue(in_array('end_rdb', $r->methods_called));
        $this->assertEqual(count($r->databases), 0, $msg = "didn't expect any databases");
    }

    function test_multiple_databases() {
        $r = $this->load_rdb('multiple_databases.rdb');
        $this->assertTrue(count($r->databases), 2);
        $this->assertTrue(!in_array(1, $r->databases));
        $this->assertEqual($r->databases[0]["key_in_zeroth_database"], "zero");
        $this->assertEqual($r->databases[2]["key_in_second_database"], "second");
    }

    function test_keys_with_expiry() {
        $r = $this->load_rdb('keys_with_expiry.rdb');
        /**
         * @var MDateTime $expiry
         */
        $expiry = $r->expiry[0]['expires_ms_precision'];

        $this->assertEqual($expiry->format('Y'), 2022);
        $this->assertEqual($expiry->format('m'), 12);
        $this->assertEqual($expiry->format('d'), 25);
        $this->assertEqual($expiry->format('H'), 10);
        $this->assertEqual($expiry->format('i'), 11);
        $this->assertEqual($expiry->format('s'), 12);
        $this->assertEqual($expiry->microsec, 573000);

    }

    public function test_integer_keys() {
        $r = $this->load_rdb('integer_keys.rdb');
        $this->assertEqual($r->databases[0][125], "Positive 8 bit integer");
        $this->assertEqual($r->databases[0][0xABAB], "Positive 16 bit integer");
        $this->assertEqual($r->databases[0][0x0AEDD325], "Positive 32 bit integer");
    }


    public function test_negative_integer_keys() {
        $r = $this->load_rdb('integer_keys.rdb');
        $this->assertEqual($r->databases[0][-123], "Negative 8 bit integer");
        $this->assertEqual($r->databases[0][-0x7325], "Negative 16 bit integer");
        $this->assertEqual($r->databases[0][-0x0AEDD325], "Negative 32 bit integer");
    }

    public function test_string_key_with_compression() {
        $r = $this->load_rdb('easily_compressible_string_key.rdb');
        $key = str_pad('a', 200, "a");
        $value = "Key that redis should compress easily";
        $this->assertEqual($r->databases[0][$key], $value);
    }

    public function test_zipmap_thats_compresses_easily() {
        $r = $this->load_rdb('zipmap_that_compresses_easily.rdb');
        $this->assertEqual($r->databases[0]["zipmap_compresses_easily"]["a"], "aa");
        $this->assertEqual($r->databases[0]["zipmap_compresses_easily"]["aa"], "aaaa");
        $this->assertEqual($r->databases[0]["zipmap_compresses_easily"]["aaaaa"], "aaaaaaaaaaaaaa");


    }


    public function test_zipmap_that_doesnt_compress() {
        $r = $this->load_rdb('zipmap_that_doesnt_compress.rdb');
        $this->assertEqual($r->databases[0]["zimap_doesnt_compress"]["MKD1G6"], 2);
        $this->assertEqual($r->databases[0]["zimap_doesnt_compress"]["YNNXK"], "F7TI");
    }

    /***
     * See issue https://github.com/sripathikrishnan/redis-rdb-tools/issues/2
     * Values with length around 253/254/255 bytes are treated specially in the parser
     * This test exercises those boundary conditions
     *
     * In order to test a bug with large ziplists, it is necessary to start
     * Redis with "hash-max-ziplist-value 21000", create this rdb file,
     * and run the test. That forces the 20kbyte value to be stored as a
     * ziplist with a length encoding of 5 bytes.
     */
    public function test_zipmap_with_big_values() {

        $r = $this->load_rdb('zipmap_with_big_values.rdb');
        $this->assertEqual(strlen($r->databases[0]["zipmap_with_big_values"]["253bytes"]), 253);
        $this->assertEqual(strlen($r->databases[0]["zipmap_with_big_values"]["254bytes"]), 254);
        $this->assertEqual(strlen($r->databases[0]["zipmap_with_big_values"]["255bytes"]), 255);
        $this->assertEqual(strlen($r->databases[0]["zipmap_with_big_values"]["300bytes"]), 300);
        $this->assertEqual(strlen($r->databases[0]["zipmap_with_big_values"]["20kbytes"]), 20000);
    }

    /**
     * '''In redis dump version = 4, hashmaps are stored as ziplists'''
     */


    public function test_hash_as_ziplist() {

        $r = $this->load_rdb('hash_as_ziplist.rdb');
        $this->assertEqual($r->databases[0]["zipmap_compresses_easily"]["a"], "aa");
        $this->assertEqual($r->databases[0]["zipmap_compresses_easily"]["aa"], "aaaa");
        $this->assertEqual($r->databases[0]["zipmap_compresses_easily"]["aaaaa"], "aaaaaaaaaaaaaa");
    }

    public function test_dictionary() {
        $r = $this->load_rdb('dictionary.rdb');
        $this->assertEqual($r->lengths[0]["force_dictionary"], 1000);
        $this->assertEqual($r->databases[0]["force_dictionary"]["ZMU5WEJDG7KU89AOG5LJT6K7HMNB3DEI43M6EYTJ83VRJ6XNXQ"], "T63SOS8DQJF0Q0VJEZ0D1IQFCYTIPSBOUIAI9SB0OV57MQR1FI");
        $this->assertEqual($r->databases[0]["force_dictionary"]["UHS5ESW4HLK8XOGTM39IK1SJEUGVV9WOPK6JYA5QBZSJU84491"], "6VULTCV52FXJ8MGVSFTZVAGK2JXZMGQ5F8OVJI0X6GEDDR27RZ");
    }

    public function test_ziplist_that_compresses_easily() {
        $r = $this->load_rdb('ziplist_that_compresses_easily.rdb');
        $this->assertEqual($r->lengths[0]["ziplist_compresses_easily"], 6);
        $lengths = [6, 12, 18, 24, 30, 36];
        foreach ($lengths as $idx => $length) {
            $this->assertEqual($key = str_pad('a', $length, "a"), $r->databases[0]["ziplist_compresses_easily"][$idx]);
        }
    }

    public function test_ziplist_that_doesnt_compress() {
        $r = $this->load_rdb('ziplist_that_doesnt_compress.rdb');
        $this->assertEqual($r->lengths[0]["ziplist_doesnt_compress"], 2);
        $this->assertTrue(in_array("aj2410", $r->databases[0]["ziplist_doesnt_compress"]));
        $this->assertTrue(in_array("cc953a17a8e096e76a44169ad3f9ac87c5f8248a403274416179aa9fbd852344", $r->databases[0]["ziplist_doesnt_compress"]));
    }

    public function test_ziplist_with_integers() {
        $r = $this->load_rdb('ziplist_with_integers.rdb');

        $expected_numbers = [];
        for ($x = 0; $x < 13; $x++) $expected_numbers[] = $x;
        //    in orginal there is 0x7fffffffffffffff for 9223372036854775807. but  0x7fffffffffffffff in php -1
        $expected_numbers = array_merge($expected_numbers, [-2, 13, 25, -61, 63, 16380, -16000, 65535, -65523, 4194304, 0x7fffffffffffffff]);

        $this->assertEqual($r->lengths[0]["ziplist_with_integers"], count($expected_numbers));

        $ziplist_with_integers = $r->databases[0]["ziplist_with_integers"];

        foreach ($expected_numbers as $num) {

            if (!$this->assertTrue(in_array($num, $ziplist_with_integers), "Cannot find " . $num)) {
                $this->reporter->paintMessage("expect $num");
            }
        };
    }

    public function test_linkedlist() {
        $r = $this->load_rdb('linkedlist.rdb');
        $this->assertEqual($r->lengths[0]["force_linkedlist"], 1000);
        $this->assertTrue(in_array("JYY4GIFI0ETHKP4VAJF5333082J4R1UPNPLE329YT0EYPGHSJQ", $r->databases[0]["force_linkedlist"]));
        $this->assertTrue(in_array("TKBXHJOX9Q99ICF4V78XTCA2Y1UYW6ERL35JCIL1O0KSGXS58S", $r->databases[0]["force_linkedlist"]));
    }


    public function test_intset_16() {
        $r = $this->load_rdb('intset_16.rdb');
        $this->assertEqual($r->lengths[0]["intset_16"], 3);
        foreach ([0x7ffe, 0x7ffd, 0x7ffc] as $num)
            $this->assertTrue(in_array($num, $r->databases[0]["intset_16"]));
    }

    public function test_intset_32() {
        $r = $this->load_rdb('intset_32.rdb');
        $this->assertEqual($r->lengths[0]["intset_32"], 3);
        foreach ([0x7ffefffe, 0x7ffefffd, 0x7ffefffc] as $num)
            $this->assertTrue(in_array($num, $r->databases[0]["intset_32"]));
    }

    public function test_intset_64() {
        $r = $this->load_rdb('intset_64.rdb');
        $this->assertEqual($r->lengths[0]["intset_64"], 3);
        foreach ([0x7ffefffefffefffe, 0x7ffefffefffefffd, 0x7ffefffefffefffc] as $num)
            $this->assertTrue(in_array($num, $r->databases[0]["intset_64"]));
    }

    public function test_regular_set() {
        $r = $this->load_rdb('regular_set.rdb');
        $this->assertEqual($r->lengths[0]["regular_set"], 6);
        foreach (["alpha", "beta", "gamma", "delta", "phi", "kappa"] as $member)
            $this->assertTrue(in_array($member, $r->databases[0]["regular_set"]), '%s missing' . $member);
    }

    public function test_sorted_set_as_ziplist() {
        $r = $this->load_rdb('sorted_set_as_ziplist.rdb');
        $this->assertEqual($r->lengths[0]["sorted_set_as_ziplist"], 3);
        $zset = $r->databases[0]["sorted_set_as_ziplist"];
        $this->assertTrue(floateq($zset['8b6ba6718a786daefa69438148361901'], 1));
        $this->assertTrue(floateq($zset['cb7a24bb7528f934b841b34c3a73e0c7'], 2.37));
        $this->assertTrue(floateq($zset['523af537946b79c4f8369ed39ba78605'], 3.423));
    }


    public function test_utf8() {
        $r = $this->load_rdb('utftest.rdb');
        $this->assertEqual($r->databases[0]['RuStr'],"съешь же ещё этих мягких французских булок, да выпей чаю");
        $this->assertEqual($r->databases[0]['GrStr'],"ვიქსიკონი, თავისუფალი ლექსიკონი");
        $this->assertEqual($r->databases[0]['HindiStr'],"हिन्दी विक्षनरी");




    }


    public function test_filtering_by_keys() {
        $r = $this->load_rdb('parser_filters.rdb', ["keys" => "k[0-9]"]);
        $this->assertEqual($r->databases[0]['k1'], "ssssssss");
        $this->assertEqual($r->databases[0]['k3'], "wwwwwwww");
        $this->assertEqual(count($r->databases[0]), 2);
    }

    public function test_filtering_by_type() {
        $r = $this->load_rdb('parser_filters.rdb', ["types" => ["sortedset"]]);
        $this->assertTrue(isset($r->databases[0]['z1']));
        $this->assertTrue(isset($r->databases[0]['z2']));
        $this->assertTrue(isset($r->databases[0]['z3']));
        $this->assertTrue(isset($r->databases[0]['z4']));
        $this->assertEqual(count($r->databases[0]), 4);
    }

    public function test_filtering_by_database() {
        $r = $this->load_rdb('multiple_databases.rdb', ["dbs" => [2]]);
        $this->assertTrue(!isset($r->databases[0]['key_in_zeroth_database']));
        $this->assertTrue(isset($r->databases[2]['key_in_second_database']));
        $this->assertEqual(count($r->databases[0]), 0);
        $this->assertEqual(count($r->databases[2]), 1);
    }

    public function test_rdb_version_5_with_checksum() {
        $r = $this->load_rdb('rdb_version_5_with_checksum.rdb');
        $this->assertEqual($r->databases[0]['abcd'], 'efgh');
        $this->assertEqual($r->databases[0]['foo'], 'bar');
        $this->assertEqual($r->databases[0]['bar'], 'baz');
        $this->assertEqual($r->databases[0]['abcdef'], 'abcdef');
        $this->assertEqual($r->databases[0]['longerstring'], 'thisisalongerstring.idontknowwhatitmeans');

    }

    public function load_rdb($file_name, $filters = '') {
        $r = new MockRedis();
        $parser = new RdbParser($r, $filters);
        $parser->parse(realpath(__DIR__) . '/dumps/' . $file_name);
        return $r;
    }


}

function floateq($f1, $f2) {
    return abs($f1 - $f2) < 0.00001;
}


class MockRedis extends RdbCallback
{
    public $databases;
    public $lengths;
    public $expiry;
    public $methods_called;
    public $dbnum;

    public function __construct() {
        $this->databases = array();
        $this->lengths = array();
        $this->expiry = array();
        $this->methods_called = [];
        $this->dbnum = 0;
    }

    public function &currentdb() {
        return $this->databases[$this->dbnum];
    }

    public function store_expiry($key, $expiry) {
        $this->expiry[$this->dbnum][$key] = $expiry;
    }

    public function store_length($key, $length) {
        if (empty($this->lengths[$this->dbnum]))
            $this->lengths[$this->dbnum] = array();
        $this->lengths[$this->dbnum][$key] = $length;
    }

    public function get_length($key) {
        if (empty($this->lengths[$this->dbnum][$key]))
            throw new Exception('$key ' . $key . ' does not have a length');
        return $this->lengths[$this->dbnum][$key];
    }

    public function start_rdb() {
        $this->methods_called[] = 'start_rdb';
    }

    public function start_database($dbnum) {
        $this->dbnum = $dbnum;
        $this->databases[$dbnum] = array();
        $this->expiry[$dbnum] = array();
        $this->lengths[$dbnum] = array();
    }

    public function set($key, $value, $expiry, $info) {
        $this->currentdb()[$key] = $value;
        if ($expiry)
            $this->store_expiry($key, $expiry);
    }

    public function start_hash($key, $length, $expiry, $info) {

        $currentdb = $this->currentdb();
        if (!empty($currentdb[$key])) {
            throw new Exception('start_hash called with $key ' . $key . ' that already exists');
        } else {
            $this->currentdb()[$key] = array();
        }
        if ($expiry)
            $this->store_expiry($key, $expiry);
        $this->store_length($key, $length);
    }

    public function hset($key, $field, $value) {
        $currentdb = $this->currentdb();
        if (!isset($currentdb[$key]))
            throw new Exception('start_hash not called for $key = ' . $key);
        $this->currentdb()[$key][$field] = $value;
    }

    public function end_hash($key) {

        $currentdb = $this->currentdb();
        if (!isset($currentdb[$key]))
            throw new Exception('start_hash not called for $key = %s', $key);
        if (count($this->currentdb()[$key]) != $this->lengths[$this->dbnum][$key])
            throw new Exception(sprintf('Lengths mismatch on hash %s, expected length = %d, actual = %d', $key, $this->lengths[$this->dbnum][$key], count($this->currentdb()[$key])));
    }

    public function start_set($key, $cardinality, $expiry, $info) {
        $currentdb = $this->currentdb();
        if (!empty($currentdb[$key]))
            throw new Exception(sprintf('start_set called with $key %s that already exists', $key));
        else
            $this->currentdb()[$key] = [];
        if ($expiry)
            $this->store_expiry($key, $expiry);
        $this->store_length($key, $cardinality);
    }

    public function sadd($key, $member) {
        $currentdb = $this->currentdb();
        if (!isset($currentdb[$key]))
            throw new Exception(sprintf('start_set not called for $key = %s', $key));
        $this->currentdb()[$key][] = $member;
    }

    public function end_set($key) {
        $currentdb = $this->currentdb();
        if (!isset($currentdb[$key]))
            throw new Exception(sprintf('start_set not called for $key = %s', $key));
        if (count($this->currentdb()[$key]) != $this->lengths[$this->dbnum][$key])
            throw new Exception(sprintf('Lengths mismatch on set %s, expected length = %d, actual = %d', $key, $this->lengths[$this->dbnum][$key], count($this->currentdb()[$key])));
    }

    public function start_list($key, $length, $expiry, $info) {
        $currentdb = $this->currentdb();
        if (!empty($currentdb[$key]))
            throw new Exception(sprintf('start_list called with $key %s that already exists', $key));
        else
            $this->currentdb()[$key] = [];
        if ($expiry)
            $this->store_expiry($key, $expiry);
        $this->store_length($key, $length);
    }

    public function rpush($key, $value) {
        $currentdb = $this->currentdb();
        if (!isset($currentdb[$key]))
            throw new Exception(sprintf('start_list not called for $key = %s', $key));
        $this->currentdb()[$key][] = $value;
    }

    public function end_list($key) {
        $currentdb = $this->currentdb();
        if (!isset($currentdb[$key]))
            throw new Exception(sprintf('start_set not called for $key = %s', $key));
        if (count($this->currentdb()[$key]) != $this->lengths[$this->dbnum][$key])
            throw new Exception(sprintf('Lengths mismatch on list %s, expected length = %d, actual = %d', $key, $this->lengths[$this->dbnum][$key], count($this->currentdb()[$key])));
    }

    public function start_sorted_set($key, $length, $expiry, $info) {
        $currentdb = $this->currentdb();
        if (!empty($currentdb[$key]))
            throw new Exception(sprintf('start_sorted_set called with key \'%s\' that already exists', $key));
        else
            $this->currentdb()[$key] = array();
        if ($expiry)
            $this->store_expiry($key, $expiry);
        $this->store_length($key, $length);
    }

    public function zadd($key, $score, $member) {
        $currentdb = $this->currentdb();
        if (!isset($currentdb[$key]))
            throw new Exception(sprintf('start_sorted_set not called for $key = %s', $key));
        $this->currentdb()[$key][$member] = $score;
    }

    public function end_sorted_set($key) {
        $currentdb = $this->currentdb();
        if (!isset($currentdb[$key]))
            throw new Exception(sprintf('start_set not called for $key = %s', $key));
        if (count($this->currentdb()[$key]) != $this->lengths[$this->dbnum][$key])
            throw new Exception(sprintf('Lengths mismatch on sortedset %s, expected length = %d, actual = %d', $key, $this->lengths[$this->dbnum][$key], count($this->currentdb()[$key])));
    }

    public function end_database($dbnum) {
        if ($this->dbnum != $dbnum)
            throw new Exception(sprintf('start_database called with %d, but end_database called %d instead', $this->dbnum, $dbnum));
    }

    public function end_rdb() {
        $this->methods_called[] = 'end_rdb';
    }
}