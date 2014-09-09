<?php
/**
 * Created by Andy Malahovsky
 * Date: 05.09.14
 * Time: 17:40
 */
require_once "RDBParser.php";


function HAS_UTF8($s)
{
    return preg_match('/[\x80-\xff]/', $s);
}

$ESCAPE_DCT = [
    '\\' => '\\\\',
    '"' => '\\"',
    '\b' => '\\b',
    '\f' => '\\f',
    '\n' => '\\n',
    '\r' => '\\r',
    '\t' => '\\t',
    utf8_encode('\x07\xEC') => '\\u2028',
    utf8_encode('\x07\xED') => '\\u2029',
];

for ($i = 0; $i < 0x20; $i++) {
    $ESCAPE_DCT[chr($i)] = sprintf('\\u%04x', $i);
}

function _rplce($match)
{
    global $ESCAPE_DCT;

    $s = $match[1];
    if (isset($ESCAPE_DCT[$s])) return $ESCAPE_DCT[$s];

    mb_internal_encoding("UTF-8");
    $char = mb_substr($s, 0, 1);
    $n = bin2hex($char);
    if ($n < 0x10000)
        return sprintf('\\u%04x', $n);
    else {
        # surrogate pair
        $n -= 0x10000;
        $s1 = 0xd800 | (($n >> 10) & 0x3ff);
        $s2 = 0xdc00 | ($n & 0x3ff);
        return sprintf('\\u%04x\\u%04x', $s1, $s2);
    }
}

/**
 * Return an ASCII-only JSON representation of a Python string
 * @param $s
 * @return int|string
 */
function _encode_basestring_ascii($s)
{

    $s=json_encode($s);


    return $s;

}


function _encode($s, $quote_numbers = true)
{
    if ($quote_numbers)
        $qn = '"';
    else
        $qn = '';
    if (is_float($s)) {
        if ($s != $s)
            return "NaN";
        elseif ("$s" == 'INF')
            return "Infinity";
        elseif ("$s" == '-INF')
            return "-Infinity";
        else
            return $qn . $s . $qn;

    } elseif (is_numeric($s))
        return $qn . $s . $qn;
    else
        return _encode_basestring_ascii($s);
}

;


function  encode_key($s)
{
    return _encode($s, true);
}


function  encode_value($s)
{
    return _encode($s, false);
}


class JSONCallback extends RdbCallback
{


    public function __construct($out)
    {

        if(!is_resource($out)){
            try{
                $out = fopen($out,'w');
            }catch (Exception $e){
                throw new Exception('It need resource or correct path',$e->getCode(),$e);
            }
        }
        $this->_out = $out;
        $this->_is_first_db = true;
        $this->_has_databases = false;
        $this->_is_first_key_in_db = true;
        $this->_elements_in_key = 0;
        $this->_element_index = 0;
    }

    public function start_rdb()
    {
        fwrite($this->_out,'[');
    }

    public function start_database($db_number)
    {
        if (!$this->_is_first_db)
            fwrite($this->_out,'},');
        fwrite($this->_out,'{');
        $this->_is_first_db = false;
        $this->_has_databases = true;
        $this->_is_first_key_in_db = true;
    }

    public function end_database($db_number)
    {

    }

    public function end_rdb()
    {
        if ($this->_has_databases)
            fwrite($this->_out,'}');
        fwrite($this->_out,']');
    }

    public function _start_key($key, $length)
    {
        if (!$this->_is_first_key_in_db)
            fwrite($this->_out,',');
        fwrite($this->_out,"\r\n");
        $this->_is_first_key_in_db = false;
        $this->_elements_in_key = $length;
        $this->_element_index = 0;
    }

    public function _end_key($key)
    {

    }

    public function _write_comma()
    {
        if ($this->_element_index > 0 and $this->_element_index < $this->_elements_in_key)
            fwrite($this->_out,',');
        $this->_element_index = $this->_element_index + 1;
    }

    public function set($key, $value, $expiry, $info)
    {
        $this->_start_key($key, 0);
        fwrite($this->_out,sprintf('%s:%s', encode_key($key), encode_value($value)));
    }

    public function start_hash($key, $length, $expiry, $info)
    {
        $this->_start_key($key, $length);
        fwrite($this->_out,sprintf('%s:{', encode_key($key)));
    }

    public function hset($key, $field, $value)
    {
        $this->_write_comma();
        fwrite($this->_out,sprintf('%s:%s', encode_key($field), encode_value($value)));
    }

    public function end_hash($key)
    {
        $this->_end_key($key);
        fwrite($this->_out,'}');
    }

    public function start_set($key, $cardinality, $expiry, $info)
    {
        $this->_start_key($key, $cardinality);
        fwrite($this->_out,sprintf('%s:[', encode_key($key)));
    }

    public function sadd($key, $member)
    {
        $this->_write_comma();
        fwrite($this->_out,sprintf('%s', encode_value($member)));
    }

    public function end_set($key)
    {
        $this->_end_key($key);
        fwrite($this->_out,']');
    }

    public function start_list($key, $length, $expiry, $info)
    {
        $this->_start_key($key, $length);
        fwrite($this->_out,sprintf('%s:[', encode_key($key)));
    }

    public function rpush($key, $value)
    {
        $this->_write_comma();
        fwrite($this->_out,sprintf('%s', encode_value($value)));
    }

    public function end_list($key)
    {
        $this->_end_key($key);
        fwrite($this->_out,']');
    }

    public function start_sorted_set($key, $length, $expiry, $info)
    {
        $this->_start_key($key, $length);
        fwrite($this->_out,sprintf('%s:{', encode_key($key)));
    }

    public function zadd($key, $score, $member)
    {
        $this->_write_comma();
        fwrite($this->_out,sprintf('%s:%s', encode_key($member), encode_value($score)));
    }

    public function end_sorted_set($key)
    {
        $this->_end_key($key);
        fwrite($this->_out,'}');
    }
}
