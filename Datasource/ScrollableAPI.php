<?php

namespace AdimeoDataSuite\Datasource;

use AdimeoDataSuite\Exception\DatasourceExecutionException;
use AdimeoDataSuite\Model\Datasource;
use GuzzleHttp\Client;

class ScrollableAPI extends Datasource
{

  function getOutputFields()
  {
    return array('doc');
  }

  function getSettingFields()
  {
    return array(
      'apiUrl' => array(
        'label' => 'API Url (use !from, !limit for scrolling parameters)',
        'type' => 'string',
        'required' => true
      ),
      'method' => array(
        'label' => 'Method (GET, POST, etc)',
        'type' => 'string',
        'required' => true
      ),
      'parameters' => array(
        'label' => 'Request body (use !from, !limit for scrolling parameters)',
        'type' => 'textarea',
        'required' => false
      ),
      'start' => array(
        'label' => 'Start at',
        'type' => 'integer',
        'required' => true
      ),
      'batchSize' => array(
        'label' => 'batch size (limit parameter)',
        'type' => 'integer',
        'required' => true
      ),
      'explodingCode' => array(
        'label' => 'Exploding code (PHP used to explode content into documents [$content is the string content returned by the API]. Must return an array.)',
        'type' => 'textarea',
        'required' => true
      )
    );
  }

  function getExecutionArgumentFields()
  {
    return array(
      'arg1' => array(
        'label' => 'Argument 1',
        'type' => 'string',
        'required' => false,
      ),
      'arg2' => array(
        'label' => 'Argument 2',
        'type' => 'string',
        'required' => false,
      ),
      'arg3' => array(
        'label' => 'Argument 3',
        'type' => 'string',
        'required' => false,
      )
    );
  }

  function execute($args)
  {
    $apiUrl = $this->getSettings()['apiUrl'];
    $method = $this->getSettings()['method'];
    $parameters = $this->getSettings()['parameters'];
    $start = $this->getSettings()['start'];
    $batchSize = $this->getSettings()['batchSize'];
    $explodingCode = $this->getSettings()['explodingCode'];
    while(!empty($docs = $this->getContentFromAPI($apiUrl, $method, $parameters, $start, $batchSize, $explodingCode, $args))) {
      foreach($docs as $doc) {
        $this->index(array(
          'doc' => $doc
        ));
      }
      $start += $batchSize;
    }

  }

  private function getContentFromAPI($apiUrl, $method, $parameters, $start, $batchSize, $explodingCode, $args) {
    $placeholders = array(
      '!from' => $start,
      '!limit' => $batchSize
    );
    foreach($args as $k => $arg) {
      $placeholders['!' . $k] = $arg;
    }
    $this->getOutputManager()->writeLn('Getting content from ' . $start);
    $url = $this->injectScrollingParameters($apiUrl, $placeholders);
    $params = $this->injectScrollingParameters($parameters, $placeholders);
    $client = new Client();
    $res = $client->request($method, $url, array(
      'body' => $params
    ));
    $callable = function($content, $code) {
      return eval($code);
    };
    if($res->getStatusCode() < 400) {
      $content = (string)$res->getBody();
      return $callable($content, $explodingCode);
    }
    throw new DatasourceExecutionException('Call failed (status code ' . $res->getStatusCode() . ')' . PHP_EOL . $res->getBody());
  }
  private function injectScrollingParameters($string, $params) {
    foreach($params as $k => $v) {
      $string = str_replace($k, $v, $string);
    }
    return $string;
  }

  function getDisplayName()
  {
    return 'Scrollable API';
  }

}