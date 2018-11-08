<?php

namespace AdimeoDataSuite\Datasource;

use AdimeoDataSuite\Model\Datasource;

class WebCrawler extends Datasource
{

  function getOutputFields()
  {
    return array(
      'title',
      'html',
      'url',
    );
  }

  function getSettingFields()
  {
    return array();
  }

  function getExecutionArgumentFields()
  {
    return array();
  }


  function execute($args)
  {
    //Nothing to do because it is based on call back
  }

  public function handleDataFromCallback($document){
    $this->index($document);
    $this->emptyBatchStack();
  }

  function getDisplayName()
  {
    return 'Web crawler';
  }

}