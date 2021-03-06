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

  function getExecutionArgumentFields()
  {
    return array(
      'filePath' => array(
        'label' => 'File path (can be an URL)',
        'type' => 'string',
        'required' => true,
        'default_from_settings' => true
      )
    );
  }


  function execute($args)
  {
    $filePath = $args['filePath'];
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