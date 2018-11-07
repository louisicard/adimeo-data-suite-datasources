<?php

namespace AdimeoDataSuite\Datasource;

use AdimeoDataSuite\Exception\DatasourceExecutionException;
use AdimeoDataSuite\Model\Datasource;

class JSONParser extends Datasource
{

  function getOutputFields()
  {
    return array_map('trim', explode(',', $this->getSettings()['jsonFields']));
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
    $arrContextOptions=array(
      "ssl"=>array(
        "verify_peer"=>false,
        "verify_peer_name"=>false,
      ),
    );
    $json = file_get_contents($filePath, false, stream_context_create($arrContextOptions));
    if (!isset($json))
      throw new DatasourceExecutionException('Could not parse JSON file');
    $data = json_decode($json, true);
    if ($data == null) {
      $data = array();
    }
    $r = array();
    $fields = array_map('trim', explode(',', $this->getSettings()['jsonFields']));
    foreach ($data as $doc) {
      $tmp = array();
      foreach ($fields as $field) {
        if (isset($doc[$field]))
          $tmp[$field] = $doc[$field];
      }
      if (!empty($tmp))
        $r[] = $tmp;
    }
    foreach($r as $doc) {
      $this->index($doc);
    }
  }

  function getDisplayName()
  {
    return 'JSON parser';
  }

}