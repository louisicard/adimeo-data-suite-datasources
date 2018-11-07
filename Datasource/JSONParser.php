<?php

namespace AdimeoDataSuite\Datasource;

use AdimeoDataSuite\Exception\DatasourceExecutionException;
use AdimeoDataSuite\Model\Datasource;

class JSONParser extends Datasource
{

  function getOutputFields()
  {
    return ['line'];
  }

  function getSettingFields()
  {
    return array(
      'jsonFields' => array(
        'label' => 'JSON fields (comma separated)',
        'type' => 'string',
        'required' => true
      )
    );
  }

  function getExecutionArgumentFields()
  {
    return array(
      'filePath' => array(
        'label' => 'File path (can be an URL)',
        'type' => 'string',
        'required' => true
      )
    );
  }

  function execute($args)
  {
    if(isset($args['filePath'])) {
      $filePath = $args[0];
    }
    else {
      throw new DatasourceExecutionException('Missing file path!');
    }
    $count = 0;
    $fp = fopen($filePath, "r");
    if ($fp) {
      while (($line = fgets($fp)) !== false) {
        if($count >= $this->getSettings()['linesToSkip']){
          $line = trim($line);
          $this->getOutputManager()->writeln('Processing line ' . ($count + 1));
          $this->index(array('line' => $line));
        }
        $count++;
      }
      fclose($fp);
    } else {
      throw new DatasourceExecutionException('Error opening file "' . $this->getSettings()['filePath'] . '"');
    }
  }

  function getDisplayName()
  {
    return 'JSON parser';
  }

}