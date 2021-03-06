<?php

namespace AdimeoDataSuite\Datasource;

use AdimeoDataSuite\Exception\DatasourceExecutionException;
use AdimeoDataSuite\Model\Datasource;

class DirectoryScanner extends Datasource
{

  function getOutputFields()
  {
    return array('absolute_path', 'info');
  }

  function getSettingFields()
  {
    return array(
      'path' => array(
        'label' => 'Directory path',
        'type' => 'string',
        'required' => true
      )
    );
  }

  function getExecutionArgumentFields()
  {
    return array(
      'path' => array(
        'label' => 'Directory path',
        'type' => 'string',
        'required' => true,
        'default_from_settings' => true
      )
    );
  }

  function execute($args)
  {
    $path = $args['path'];
    if(file_exists($path) && is_dir($path)) {
      $path = realpath($path);
      $this->scanDirectory($path, function($file) {
        $this->index(array(
          'absolute_path' => $file,
          'info' => pathinfo($file)
        ));
      });
    }
    else {
      throw new DatasourceExecutionException($path . ' is not a valid directory');
    }
  }

  private function scanDirectory($path, $callable) {
    $content = scandir($path);
    foreach($content as $c) {
      if($c != '.' && $c != '..') {
        if (is_dir($path . DIRECTORY_SEPARATOR . $c)) {
          $this->scanDirectory($path . DIRECTORY_SEPARATOR . $c, $callable);
        } else {
          if($callable != null) {
            $callable($path . DIRECTORY_SEPARATOR . $c);
          }
        }
      }
    }
  }

  function getDisplayName()
  {
    return 'Directory scanner';
  }

}