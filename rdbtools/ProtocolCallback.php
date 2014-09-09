<?php
/**
 * Created by Andy Malahovsky
 * Date: 08.09.14
 * Time: 17:06
 */

require_once "RDBParser.php";

function unicode($var){

    return $var;
}

class ProtocolCallback extends RdbCallback
{

    var $_expires=array();

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
        $this->reset();
    }

    function reset()
    {
        $this->_expires = [];
    }

    function set_expiry($key, $dt)
    {
        $this->_expires[$key] = $dt;
    }

    function get_expiry_seconds($key)
    {
        if ( isset($this->_expires[$key])) {
            return $this->_expires[$key]->format('U');
        }
        return null;
    }

    function expires($key)
    {
        return isset($this->_expires[$key]) ? $this->_expires[$key] : null;
    }

    function pre_expiry($key, $expiry)
    {
        if ($expiry)
            $this->set_expiry($key, $expiry);
    }

    function post_expiry($key)
    {
        if ($this->expires($key)) {
            $this->expireat($key, $this->get_expiry_seconds($key));
        }
    }

    function emit()
    {
        $args = func_get_args();

        fwrite($this->_out, "*" . count($args) . "\r\n");
        foreach ($args as $arg) {
            fwrite($this->_out, "$" . strlen($arg) . "\r\n");
            fwrite($this->_out, unicode($arg) . "\r\n");
        }
    }

    function start_database($db_number)
    {
        $this->reset();
        $this->select($db_number);

    }

    function set($key, $value, $expiry, $info)
    {
        $this->pre_expiry($key, $expiry);
        $this->emit('SET', $key, $value);
        $this->post_expiry($key);

        # Hash handling;
    }

    function start_hash($key, $length, $expiry, $info)
    {
        $this->pre_expiry($key, $expiry);
    }

    function hset($key, $field, $value)
    {
        $this->emit('HSET', $key, $field, $value);
    }

    function end_hash($key)
    {
        $this->post_expiry($key);

        # Set handling;
    }

    function start_set($key, $cardinality, $expiry, $info)
    {
        $this->pre_expiry($key, $expiry);
    }

    function sadd($key, $member)
    {
        $this->emit('SADD', $key, $member);
    }

    function end_set($key)
    {
        $this->post_expiry($key);

        # List handling;
    }

    function start_list($key, $length, $expiry, $info)
    {
        $this->pre_expiry($key, $expiry);
    }

    function rpush($key, $value)
    {
        $this->emit('RPUSH', $key, $value);
    }

    function end_list($key)
    {
        $this->post_expiry($key);

        # Sorted set handling;
    }

    function start_sorted_set($key, $length, $expiry, $info)
    {
        $this->pre_expiry($key, $expiry);
    }

    function zadd($key, $score, $member)
    {
        $this->emit('ZADD', $key, $score, $member);
    }

    function end_sorted_set($key)
    {
        $this->post_expiry($key);

        # Other misc commands;
    }

    function select($db_number)
    {
        $this->emit('SELECT', $db_number);
    }

    function expireat($key, $timestamp)
    {
        $this->emit('EXPIREAT', $key, $timestamp);
    }
}