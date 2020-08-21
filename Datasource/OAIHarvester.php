<?php

namespace AdimeoDataSuite\Datasource;

use AdimeoDataSuite\Model\Datasource;
use GuzzleHttp\Client;
use GuzzleHttp\Cookie\CookieJar;
use GuzzleHttp\Cookie\SetCookie;

class OAIHarvester extends Datasource
{

  function getOutputFields()
  {
    return array(
      'identifier',
      'datestamp',
      'sets',
      'metadata',
    );
  }

  function getSettingFields()
  {
    return array(
      'oaiServerUrl' => array(
        'label' => 'OAI server URL',
        'type' => 'string',
        'required' => true
      ),
      'sets' => array(
        'label' => 'Sets to harvest (comma separated)',
        'type' => 'string',
        'required' => false
      ),
      'metaDataPrefix' => array(
        'label' => 'Metadata prefix',
        'type' => 'string',
        'required' => true
      )
    );
  }

  function getExecutionArgumentFields()
  {
    return array();
  }


  function execute($args)
  {
    $sets = array_map('trim', explode(',', $this->getSettings()['sets']));
    $count = 0;
    if (count($sets) > 0) {
      foreach ($sets as $set) {
        $count += $this->harvest($set);
      }
    } else {
      $this->harvest(NULL);
    }
    $this->getOutputManager()->writeLn('Found ' . $count . ' documents');
  }

  public function runCli($token){
    $sets = array_map('trim', explode(',', $this->getSettings()['sets']));
    if (count($sets) > 0) {
      foreach ($sets as $set) {
        $this->harvest($set, $token, 0, true);
      }
    } else {
      $this->harvest(NULL, $token, 0, true);
    }
    $this->emptyBatchStack();
  }

  private function harvest($set, $resumptionToken = null, $count = 0, $cli = false) {
    $doc = new \DOMDocument();
    if ($resumptionToken == null)
      $url = $this->getSettings()['oaiServerUrl'] . '?verb=ListRecords&metadataPrefix=' . $this->getSettings()['metaDataPrefix'] . ($set != NULL ? '&set=' . $set : '');
    else
      $url = $this->getSettings()['oaiServerUrl'] . '?verb=ListRecords&resumptionToken=' . urlencode($resumptionToken);

    $this->getOutputManager()->writeln('Harvesting url ' . $url);

    $content = $this->getContentFromUrl($url);
    $config = array(
      'indent'     => true,
      'input-xml'  => true,
      'output-xml' => true,
      'wrap'       => false,
      'output-encoding' => 'utf8',
      'numeric-entities' => true,
      'preserve-entities' => true,
      'quote-ampersand' => true,
    );
    if(isset($content['encoding'])){
      if($content['encoding'] == 'utf-8' || $content['encoding'] == 'utf8'){
        $config['input-encoding'] = 'utf8';
      }
    }
    $tidy = @tidy_parse_string($content['content'], $config);
    tidy_clean_repair($tidy);
    $string = (string)$tidy;
    //Fixing unclean entities
    $string = str_replace("&#", "__CL_AMP__", $string);
    $string = str_replace("&", "&#38;", $string);
    $string = str_replace("__CL_AMP__", "&#", $string);
    //End fixing
    $doc->loadXML($string);
    $xpath = new \DOMXPath($doc);
    $result = $xpath->query("//namespace::*");

    foreach ($result as $node) {
      /** @var \DOMNode $node */
      if ($node->nodeName == 'xmlns') {
        $xpath->registerNamespace('oai', $node->nodeValue);
        break;
      }
    }
    $items = $xpath->query('oai:ListRecords/oai:record');
    foreach ($items as $item) {
      $document = array();
      if ($xpath->query('oai:header/oai:identifier', $item)->length > 0)
        $document['identifier'] = $xpath->query('oai:header/oai:identifier', $item)->item(0)->textContent;
      if ($xpath->query('oai:header/oai:datestamp', $item)->length > 0)
        $document['datestamp'] = $xpath->query('oai:header/oai:datestamp', $item)->item(0)->textContent;
      if ($xpath->query('oai:header/oai:setSpec', $item)->length > 0){
        foreach($xpath->query('oai:header/oai:setSpec', $item) as $setSpec){
          $document['sets'][] = $setSpec->textContent;
        }
      }
      if ($xpath->query('oai:metadata/*', $item)->length > 0)
        $document['metadata'] = '<?xml version="1.0" encoding="' . $doc->encoding . '"?>' . simplexml_import_dom($xpath->query('oai:metadata/*', $item)->item(0))->asXML();

      $this->getOutputManager()->writeln(($count + 1) . ' / Harvesting doc "' . $document['identifier'] . '"');
      $this->getOutputManager()->writeln(sprintf('Memory usage (currently) %dKB/ (max) %dKB', round(memory_get_usage(true) / 1024), memory_get_peak_usage(true) / 1024));

      $this->index($document);
      unset($document);
      $count ++;
    }
    unset($items);
    if (isset($item))
      unset($item);
    if ($xpath->query('oai:ListRecords/oai:resumptionToken')->length > 0 && !empty($xpath->query('oai:ListRecords/oai:resumptionToken')->item(0)->textContent)) {
      $token = $xpath->query('oai:ListRecords/oai:resumptionToken')->item(0)->textContent;
      unset($result);
      unset($xpath);
      unset($doc);
      gc_enable();
      gc_collect_cycles();
      if(!$cli){
        return $this->harvest($set, $token, $count);
      }
      else{
        if($this->hasBatchExecution()) {
          $this->emptyBatchStack();
        }
        $this->getOutputManager()->writeln($token);
        exit(9);
      }
    }
    if($cli){
      if($this->hasBatchExecution()) {
        $this->emptyBatchStack();
      }
      exit(0);
    }
    return $count;
  }

  protected $cookies = '';

  private function getContentFromUrl($url) {
    $client = new Client(array(
      'verify' => false
    ));
    $jar = new CookieJar();
    if(!empty($this->cookies)) {
      $jar->setCookie(SetCookie::fromString($this->cookies));
    }
    $response = $client->request('GET', $url, ['cookies' => $jar]);
    if(!empty($response->getHeader('Set-Cookie'))){
      $this->cookies = implode('', $response->getHeader('Set-Cookie'));
    }

    $content = [];
    preg_match_all('!\<\?xml.*encoding="(?<encoding>[^"]*)!', substr(strtolower($response->getBody()), 0, 300), $matches);
    if(isset($matches['encoding']) && !empty($matches['encoding'])){
      $content['encoding'] = $matches['encoding'][0];
    }
    $content['content'] = $this->cleanUTF8String($response->getBody());
    return $content;
  }

  private function cleanUTF8String($str){
    return preg_replace('/[^\x{0009}\x{000a}\x{000d}\x{0020}-\x{D7FF}\x{E000}-\x{FFFD}]+/u', ' ', $str);
  }

  function getDisplayName()
  {
    return 'OAI Harvester';
  }

}