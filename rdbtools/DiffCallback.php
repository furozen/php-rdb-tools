<?php
/**
 * Created by Andy Malahovsky
 * Date: 16.09.14
 * Time: 18:14
 */


class DiffCallback extends RdbCallback
{

    var $out;
    var $_index;
    var $_dbnum;

    function __construct($out)
    {
        if(!is_resource($out)){
            try{
                $out = fopen($out,'w');
            }catch (Exception $e){
                throw new Exception('It need resource or correct path',$e->getCode(),$e);
            }
        }

        $this->_out = $out;
        $this->_index = 0;
        $this->_dbnum = 0;
    }

    function start_rdb()
    {

    }

    function start_database($db_number)
    {
        $this->_dbnum = $db_number;
    }

    function end_database($db_number)
    {

    }

    function end_rdb()
    {

    }

    function set($key, $value, $expiry, $info)
    {

        fwrite($this->_out, sprintf('db=%d %s -> %s', $this->_dbnum, encode_key($key), encode_value($value)));
        $this->newline();
    }

    function start_hash($key, $length, $expiry, $info)
    {

    }

    function hset($key, $field, $value)
    {
        fwrite($this->_out, sprintf('db=%d %s . %s -> %s', $this->_dbnum, encode_key($key), encode_key($field), encode_value($value)));
        $this->newline();
    }

    function end_hash($key)
    {

    }

    function start_set($key, $cardinality, $expiry, $info)
    {

    }

    function sadd($key, $member)
    {
        fwrite($this->_out, sprintf('db=%d %s { %s }', $this->_dbnum, encode_key($key), encode_value($member)));
        $this->newline();
    }

    function end_set($key)
    {

    }

    function start_list($key, $length, $expiry, $info)
    {
        $this->_index = 0;
    }

    function rpush($key, $value)
    {
        fwrite($this->_out, sprintf('db=%d %s[%d] -> %s', $this->_dbnum, encode_key($key), $this->_index, encode_value($value)));
        $this->newline();
        $this->_index = $this->_index + 1;
    }

    function end_list($key)
    {

    }

    function start_sorted_set($key, $length, $expiry, $info)
    {
        $this->_index = 0;
    }

    function zadd($key, $score, $member)
    {
        if(is_float($score) && strpos($score,'.')===false){
            $score.='.0';
        }
        fwrite($this->_out, sprintf('db=%d %s[%d] -> {%s, score=%s}', $this->_dbnum, encode_key($key), $this->_index, encode_key($member), encode_value($score)));
        $this->newline();
        $this->_index = $this->_index + 1;
    }

    function end_sorted_set($key)
    {

    }

    function newline()
    {
         fwrite($this->_out,"\r\n");
    }
}