<?php

namespace AdimeoDataSuite\Datasource;

use AdimeoDataSuite\Model\Datasource;

class PDODatabase extends Datasource
{

  function getOutputFields()
  {
    return ['row'];
  }

  function getSettingFields()
  {
    return array(
      'driver' => array(
        'label' => 'PDO driver (E.g.: mysql, postgresl)',
        'type' => 'string',
        'required' => true
      ),
      'host' => array(
        'label' => 'Host',
        'type' => 'string',
        'required' => true
      ),
      'port' => array(
        'label' => 'Port',
        'type' => 'string',
        'required' => true
      ),
      'dbName' => array(
        'label' => 'Database name',
        'type' => 'string',
        'required' => true
      ),
      'username' => array(
        'label' => 'Username',
        'type' => 'string',
        'required' => true
      ),
      'password' => array(
        'label' => 'Password',
        'type' => 'string',
        'required' => true
      ),
      'batchSize' => array(
        'label' => 'Batch size (used in limit statement)',
        'type' => 'string',
        'required' => true
      ),
      'sql' => array(
        'label' => 'SQL query (!! use @limit and @offset variables for pagination !!)',
        'type' => 'textarea',
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
    try {
      $tries_dsn = 0;
      $retry_dsn = true;
      while ($tries_dsn == 0 || $retry_dsn) {
        try {
          $count = 0;
          $settings = $this->getSettings();
          $dsn = $settings['driver'] . ':host=' . $settings['host'] . ';port=' . $settings['port'] . ';dbname=' . $settings['dbName'] . ';charset=UTF8;';
          $pdo = $this->getPDOPool()->getHandler($dsn, $settings['username'], $settings['password']);

          $continue = true;
          $offset = 0;
          while ($continue) {
            $sql = $settings['sql'];
            $sql = str_replace('@limit', $settings['batchSize'], $sql);
            $sql = str_replace('@offset', $offset, $sql);

            $this->getOutputManager()->writeln('Executing SQL: ' . $sql);

            $tries = 0;
            $retry = true;
            while ($tries == 0 || $retry) {
              try {
                $res = $pdo->query($sql);
                $continue = false;
                while ($row = $res->fetch(\PDO::FETCH_ASSOC)) {
                  $continue = $this->hasPagination();
                  $count++;
                  $this->index(array(
                    'row' => $row
                  ));
                }
                $offset += $settings['batchSize'];
                $retry = false;
              } catch (\PDOException $ex) {
                $this->getOutputManager()->writeLn(get_class($this) . ' >> PDO Exception has been caught (' . $ex->getMessage() . ')');
                if ($tries > 20) {
                  $retry = false;
                  $this->getOutputManager()->writeLn(get_class($this) . ' >> This is over, I choose to die.');
                  return; //Kill the datasource
                } else {
                  $this->getOutputManager()->writeLn(get_class($this) . ' >> Retrying in 1 second...');
                  sleep(1); //Sleep for 1 second
                }
              } finally {
                $tries++;
              }
            }
          }
          $retry_dsn = false;
        } catch (\PDOException $ex) {
          $this->getOutputManager()->writeLn(get_class($this) . ' >> PDO Exception has been caught (' . $ex->getMessage() . ')');
          if ($tries_dsn > 20) {
            $retry_dsn = false;
            $this->getOutputManager()->writeLn(get_class($this) . ' >> This is over, I choose to die.');
            throw $ex;
          } else {
            $this->getOutputManager()->writeLn(get_class($this) . ' >> Retrying in 1 second...');
            sleep(1); //Sleep for 1 second
          }
        } finally {
          $tries_dsn++;
        }
      }
    } catch (\Exception $ex) {
      print $ex->getMessage();
    }
    if(isset($count))
      $this->getOutputManager()->writeLn('Found ' . $count . ' documents');

  }

  private function hasPagination()
  {
    $sql = $this->getSettings()['sql'];
    return strpos($sql, '@limit') !== FALSE && strpos($sql, '@offset') !== FALSE;
  }

  function getDisplayName()
  {
    return 'PDO Database';
  }

}