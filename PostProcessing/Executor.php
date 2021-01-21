<?php


if (file_exists('../DevConfig.php')) {
    $config = include '../DevConfig.php';
} else {
    $config = include '../config.php';

}

require '../vendor/autoload.php';

use Carbon\Carbon;

include '../Database/database.php';
require 'Attendance.php';
require 'LineChart.php';
require 'GroupRank.php';
/**
 * $attendance->process($fromDate,$toDate,$limit,$iterations)
 * даты включительно;
 * limit - sql limit; 0 или null - без лимита
 * если кол-во итераций не задано посчитает все не обработанные записи;
 */
try {

    // php AfterGoogleMetLoad.php -- reprocess
    // php AfterGoogleMetLoad.php -- rollback
    $optind = null;
    $opts = getopt('a:b:', [], $optind);
    $options = array_slice($argv, $optind);


    $db = new DataConnectionManager($config);

//
//    $attendance = new Attendance(
//        $db,
//        500,
//        1
//    );

    $attendance  = new GroupRank($db);
    if (in_array('rollback',$options)) {
        $attendance->rollback();
        exit();
    }
    if (in_array('reprocess',$options)) {
        $attendance->reprocess() ;
        exit();
    }


    $attendance->rollback();
    $attendance->setLimit(5000)->setIterations()->process();
    exit();


} catch (Exception $exception) {
    exit($exception->getMessage());
}


