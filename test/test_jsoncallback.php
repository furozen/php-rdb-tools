<?php
/**
 * Created by Andy Malahovsky
 * Date: 08.09.14
 * Time: 11:47
 */


require_once('simpletest/autorun.php');



require_once  realpath(__DIR__.'/../rdbtools/RDBParser.php');
require_once  realpath(__DIR__.'/../rdbtools/callbacks.php');

class TestJSONCallback extends UnitTestCase
{

    function test_empty_rdb() {
        $file_name='empty_database.rdb';
        $this->base_test($file_name);
    }

    function test_multiple_databases_rdb() {
        $file_name='multiple_databases.rdb';
        $this->base_test($file_name);
    }

    function test_expires_ms_precision_rdb() {
        $file_name='keys_with_expiry.rdb';
        $this->base_test($file_name);
    }

    function test_integer_keys_rdb() {
        $file_name='integer_keys.rdb';
        $this->base_test($file_name);
    }


    function test_easily_compressible_string_key_rdb() {
        $file_name='easily_compressible_string_key.rdb';
        $this->base_test($file_name);
    }

    function test_zipmap_that_compresses_easily_rdb() {
        $file_name='zipmap_that_compresses_easily.rdb';
        $this->base_test($file_name);
    }

    function test_zipmap_that_doesnt_compress_rdb() {
        $file_name='zipmap_that_doesnt_compress.rdb';
        $this->base_test($file_name);
    }

    function test_zipmap_with_big_values_rdb() {
        $file_name='zipmap_that_compresses_easily.rdb';
        $this->base_test($file_name);
    }

    function test_hash_as_ziplist() {
        $file_name='hash_as_ziplist.rdb';
        $this->base_test($file_name);
    }

    function test_dictionary() {
        $file_name='dictionary.rdb';
        $this->base_test($file_name);
    }

    function test_ziplist_that_compresses_easily() {
        $file_name='ziplist_that_compresses_easily.rdb';
        $this->base_test($file_name);
    }

    function test_ziplist_that_doesnt_compress() {
        $file_name='ziplist_that_doesnt_compress.rdb';
        $this->base_test($file_name);
    }

    function test_ziplist_with_integers() {
        $file_name='ziplist_with_integers.rdb';
        $this->base_test($file_name);
    }

    function test_linkedlist() {
        $file_name='linkedlist.rdb';
        $this->base_test($file_name);
    }

    function test_intset_16() {
        $file_name='intset_16.rdb';
        $this->base_test($file_name);
    }

    function test_intset_32() {
        $file_name='intset_32.rdb';
        $this->base_test($file_name);
    }


    function test_intset_64() {
        $file_name='intset_64.rdb';
        $this->base_test($file_name);
    }

    function test_regular_set() {
        $file_name='regular_set.rdb';
        $this->base_test($file_name);
    }

    function test_sorted_set_as_ziplist() {
        $file_name='sorted_set_as_ziplist.rdb';
        $this->base_test($file_name);
    }

    function test_utftest() {
        $file_name='utftest.rdb';
        $this->base_test($file_name);
    }

    function test_uncompressible_string_keys() {
        $file_name='uncompressible_string_keys.rdb';
        $this->base_test($file_name);
    }





    public function load_rdb($file_name, $filters = '') {
        $r = new JSONCallback($this->getOutFileName($file_name));
        $parser = new RdbParser($r, $filters);
        $parser->parse($this->getDumpsPath() . $file_name);
        fclose($r->_out);
        return $r;
    }

    /**
     * @return string
     */
    private function getDumpsPath()
    {
        return realpath(__DIR__) . '/dumps/';
    }

    /**
     * @param $file_name
     * @return string
     */
    private function getOutFileName($file_name)
    {
        return $this->getDumpsPath() . $file_name . '.t.json';
    }
    private function getExpectFileName($file_name)
    {
        return $this->getDumpsPath() . $file_name . '.json';
    }

    /**
     * @param $file_name
     */
    private function base_test($file_name)
    {
        $r = $this->load_rdb($file_name);
        $testResult = file_get_contents($this->getOutFileName($file_name));
        $expectResult = file_get_contents($this->getExpectFileName($file_name));
        if(!$this->assertEqual($testResult, $expectResult)){

            echo "<br>result<br><hr><br>";
            echo htmlspecialchars(substr($testResult,0,1000));
            echo "<br>expect<br><hr><br>";
            echo htmlspecialchars(substr($expectResult,0,1000));

        };
    }
}