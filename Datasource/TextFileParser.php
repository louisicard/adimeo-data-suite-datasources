<?php

namespace AdimeoDataSuite\Bundle\DatasourcesBundle\Datasource;

use AdimeoDataSuite\Bundle\CommonsBundle\Exception\DatasourceExecutionException;
use AdimeoDataSuite\Bundle\CommonsBundle\Model\Datasource;
use AdimeoDataSuite\Bundle\CommonsBundle\Model\OutputManager;

class TextFileParser extends Datasource
{
  private $filePath;
  private $linesToSkip;

  public function __construct($filePath, $linesToSkip = 0)
  {
    $this->filePath = $filePath;
    $this->linesToSkip = $linesToSkip;
  }

  function getOutputFields()
  {
    return ['line'];
  }

  function getSettingFields()
  {
    return array(
      'filePath' => array(
        'type' => 'string',
        'required' => true
      ),
      'linesToSkip' => array(
        'type' => 'integer',
        'required' => false,
        'default' => 0
      )
    );
  }

  function getExecutionArguments()
  {
    return array();
  }

  function execute($args, OutputManager $output)
  {
    $count = 0;
    $fp = fopen($this->filePath, "r");
    if ($fp) {
      while (($line = fgets($fp)) !== false) {
        if($count >= $this->linesToSkip){
          $line = trim($line);
          $output->writeln('Processing line ' . ($count + 1));
          $this->index(array('line' => $line));
        }
        $count++;
      }
      fclose($fp);
    } else {
      throw new DatasourceExecutionException('Error opening file "' . $this->filePath . '"');
    }
  }

  static function instantiate($settings)
  {
    return new TextFileParser($settings['filePath'], $settings['linesToSkip']);
  }

  function getName()
  {
    return 'Text file Parser';
  }

}