<?php

namespace AdimeoDataSuite\Datasource;

use AdimeoDataSuite\Exception\DatasourceExecutionException;
use AdimeoDataSuite\Model\Datasource;

class TextFileParser extends Datasource
{

  function getOutputFields()
  {
    return ['line'];
  }

  function getSettingFields()
  {
    return array(
      'filePath' => array(
        'label' => 'File path (can be an URL)',
        'type' => 'string',
        'required' => true
      ),
      'linesToSkip' => array(
        'label' => 'Lines to skip',
        'type' => 'integer',
        'required' => false,
        'default' => 0
      )
    );
  }

  function execute($args)
  {
    $filePath = $this->getSettings()['filePath'];
    if(isset($args[0]) && file_exists($args[0]) && is_file($args[0])) {
      $filePath = $args[0];
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
    return 'Text file Parser';
  }

}