
#!/usr/local/bin/php -q
<?php
error_reporting(E_ALL);

/* Allow the script to hang around waiting for connections. */
set_time_limit(0);

/* Turn on implicit output flushing so we see what we're getting
 * as it comes in. */
ob_implicit_flush();


$helps=array(
    'command'=>"Command to execute. Valid commands are json, diff, and protocol",
    'file'=>"Output file",
    'db'=>"Database Number. Multiple databases can be provided. If not specified, all databases will be included.",
    'key'=>"Keys to export. This can be a regular expression",
    'type'=>"Data types to include. Possible values are string, hash, set, sortedset, list. Multiple typees can be provided.
                    If not specified, all data types will be returned",

);
$opr='h';
$help='';
$longopts=['--help'];
foreach($helps as $key=>$val){
    $opt.=$key[0].':';
    $longopts[]=$key;
    $help.="\n -{$key[0]}, --$key  $val";
}


$opt = getopt($opt,$longopts);
if(!isset($opt['help']) || isset($opt['h'])){

    echo "\n usage: %prog [options] /path/to/dump.rdb";

 echo "\nExample : %prog --command json -k \"user.*\" /var/redis/6379/dump.rdb";
 echo "\n   valid options:";
 echo $help;
    die();


}

if (count($argv) == 0)
    die("Redis RDB file not specified");

$dump_file = $argv[1];

$command=!empty($opt['c'])?$opt['c']:(!empty($opt['command'])?$opt['command']:'undef');
$dbs=!empty($opt['d'])?$opt['d']:(!empty($opt['db'])?$opt['db']:'');
$keys=!empty($opt['k'])?$opt['k']:(!empty($opt['key'])?$opt['key']:'');
$types=!empty($opt['t'])?$opt['t']:(!empty($opt['type'])?$opt['type']:'');
$file=!empty($opt['f'])?$opt['f']:(!empty($opt['file'])?$opt['file']:'');

$filters = [];
if (!empty($dbs)){
   $filters['dbs'] = explode(' ,;',$dbs);
   foreach($filters['dbs'] as $val){
       if(!is_numeric($val)){
           die('Invalid database number '. $val);
       }

   }
}

if (!empty($keys)){
    $filters['keys'] = $keys;
}

$valid_types = ["hash", "set", "string", "list", "sortedset"];

if (!empty($types)){
    $filters['types'] = explode(' ,;',$types);
    foreach($filters['types'] as $val){
        if(!in_array($val,$valid_types)){
            die('Invalid database number '. $val);
        }

    }
}
$outputFilename = 'php://stdout';
if(!empty($file)){
    $outputFilename = $file;
}

switch($command){
    case 'diff':
        $callback =new  DiffCallback($outputFilename);
    break;
    case 'json' :
            $callback = new JSONCallback($outputFilename);
    break;
    case 'memory':
            $reporter =new PrintAllKeys($outputFilename);
            $callback =new MemoryCallback(reporter, 64);
        break;
    case 'protocol':
            $callback =new ProtocolCallback($outputFilename);
    break;
    default:
            die('Invalid Command '.$command);
}



$parser =new  RdbParser($callback, $filters);
$parser->parse($dump_file);

