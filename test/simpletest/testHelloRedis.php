<?php
/**
 * 
 * Author: Andy Malahovsky 
 * andrewm@smartinfosys.com 
 * Date: 10/10/11
 * Time: 1:13 PM
 *
 */

$options = array(
    'namespace' => 'Application_',
    'servers'   => array(
        'server1' =>array('host' => '127.0.0.1', 'port' => 6379)
    )
);

require_once  $_SERVER['ENGINE_ROOT'].'/sandbox/Rediska 0.5.5/library/Rediska.php';
$rediska = new Rediska($options);


$key = new Rediska_Key('keyName', array(
    'serverAlias' => 'server1'
));

// Set value
$key->setValue('value');

// Print value
print $key; #=> value





// Get length
print count($key); #=> 5

// Expire key after 5 minutes
$key->expire(5 * 60);

// Вы можете сохранять также масивы и объекты
$key->setValue(array('value', 'value2', 'value3'));

// Get value
$value = $key->getValue();

print_r($value);
// Delete key
$key->delete();
?>