<?php

namespace AdimeoDataSuite\Datasource;

use AdimeoDataSuite\Exception\DatasourceExecutionException;
use AdimeoDataSuite\Model\Datasource;
use GuzzleHttp\Client;
use Symfony\Component\HttpFoundation\File\File;

class XMLParser extends Datasource
{

  function getOutputFields()
  {
    return array(
      'global_doc',
      'doc',
    );
  }

  function getSettingFields()
  {
    return array(
      'url' => array(
        'label' => 'XML File url',
        'type' => 'string',
        'required' => true
      ),
      'xpath' => array(
        'label' => 'XPath',
        'type' => 'string',
        'required' => true
      ),
      'xpathNamespaces' => array(
        'label' => 'XPath Namespaces to register',
        'type' => 'string',
        'required' => false
      ),
    );
  }

  function getExecutionArgumentFields()
  {
    return array(
      'url' => array(
        'label' => 'File (if not set in settings)',
        'type' => 'file',
        'required' => false
      )
    );
  }


  function execute($args)
  {
    $url = $this->getSettings()['url'];
    if(isset($args['url']) && !empty($args['url'])) {
      if($args['url'] instanceof File) {
        $url = 'file://' . $args['url']->getRealPath();
      }
      else {
        if (strpos($args['url'], '/') === 0) {
          $url = "file://" . $args['url'];
        } else {
          $url = $args['url'];
        }
      }
    }
    if(strpos($url, 'file://') === 0) {
      $content = file_get_contents($url);
    }
    else {
      $client = new Client();
      $response = $client->request('GET', $url);
      $content = $response->getBody();
    }
    $xml = simplexml_load_string($content);
    $count = 0;
    if($xml) {
      if(isset($this->getSettings()['xpathNamespaces']) && !empty($this->getSettings()['xpathNamespaces'])){
        $nss = explode(',', $this->getSettings()['xpathNamespaces']);
        foreach($nss as $ns){
          $prefix = substr($ns, 0, strpos($ns, ':'));
          $url = substr($ns, strpos($ns, ':') + 1);
          $xml->registerXpathNamespace($prefix , $url);
        }
      }
      $docs = $xml->xpath($this->getSettings()['xpath']);

      $this->getOutputManager()->writeln('Found ' . count($docs) . ' documents');

      foreach($docs as $doc){
        foreach($xml->getNamespaces(true) as $prefix => $ns){
          if(!empty($prefix)){
            $doc->addAttribute($prefix . ':ads', 'ads', $prefix);
          }
        }
        $this->index(array(
          'global_doc' => $xml,
          'doc' => simplexml_load_string($doc->asXML())
        ));
        $count++;
      }
    }
    else {
      throw new DatasourceExecutionException('Cannot load XML from ' . $url);
    }
    $this->getOutputManager()->writeln('Processed ' . $count . ' documents');
  }

  function getDisplayName()
  {
    return 'XML Parser';
  }

}