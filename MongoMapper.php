<?php

class MongoMapper {

    private $host = 'localhost';
    private $databaseName = '';
    private $username = '';
    private $password = '';
    private $fsync = true;
    private $journaling = true;
    private $useLegacyMongoDriver = 0;
    private static $instance = null;
    private static $writeConcern = null;
    public $collection = NULL;

    const INSERT_DOCUMENTS_ORDER = false;

    public function __construct() {
        $options = array('username' => $this->username,
            'password' => $this->password,
            'db' => $this->databaseName,
        );
        $options['fsync'] = $this->fsync;
        $options['socketTimeoutMS'] = 1000;

        if (empty(self::$instance)) {
            if ($this->useLegacyMongoDriver) {
                $mongo = new MongoClient('mongodb://' . $this->host, $options);
                self::$instance = $mongo->{MONGO_DATABASE};
            } else {
                self::$instance = new MongoDB\Driver\Manager('mongodb://' . $this->host, $options);
                self::$writeConcern = new MongoDB\Driver\WriteConcern(MongoDB\Driver\WriteConcern::MAJORITY, 1000, $this->journaling);
            }
        }
    }

    public function __get($name) {
        if (is_string($name)) {
            $this->collection = $name;
            return $this;
        }
    }

    public function find($conditions = array(), $fields = array()) {

        $rules = array();
        $rules['find'] = $this->collection;

        if (isset($conditions['$skip'])) {
            $rules['skip'] = (int) $conditions['$skip'];
            unset($conditions['$skip']);
        }
        if (isset($conditions['$limit'])) {
            $rules['limit'] = (int) $conditions['$limit'];
            unset($conditions['$limit']);
        }
        if (isset($conditions['$sort'])) {
            $rules['sort'] = $conditions['$sort'];
            unset($conditions['$sort']);
        }
        if (!empty($fields)) {
            foreach ($fields as $field) {
                $rules['projection'][$field] = 1;
            }
        }

        $rules['filter'] = $conditions;

        if (isset($conditions['_id'])) {
            $rules['filter']['_id'] = $this->sanitizeID($conditions['_id']);
            unset($conditions['_id']);
        }

        try {
            $query = new \MongoDB\Driver\Command($rules);

            $rows = self::$instance->executeCommand($this->database, $query);
            $documents = array();
            foreach ($rows as $row) {
                if (isset($row->result)) {
                    $documents = $row->result;
                } else {
                    $documents[] = $row;
                }
            }
            $docs = array();
            if (!empty($documents)) {
                foreach ($documents as $doc) {
                    $docs[] = $this->documentFormatter($doc);
                }
            }

            return $docs;
        } catch (Exception $ex) {
            $this->handleExceptions($ex);
        }
    }

    public function findOne($conditions = array(), $fields = array(), $options = array()) {
        try {
            $conditions['$limit'] = 1;
            $result = $this->find($conditions, $fields);
            if (!empty($result[0])) {
                return $result[0];
            }
            return array();
        } catch (Exception $ex) {
            $this->handleExceptions($ex);
        }
    }

    public function update($conditions = array(), $update = array(), $options = array()) {
        $status = 1;
        if (isset($options['multiple'])) {
            $options['multi'] = $options['multiple'];
            unset($options['multiple']);
        }
        try {
            if (!empty($conditions['_id'])) {
                if (is_object($conditions['_id']) && get_class($conditions['_id']) == 'MongoId') {
                    $documentID = (array) $conditions['_id'];
                    $conditions['_id'] = new \MongoDB\BSON\ObjectId($documentID['$id']);
                } else if (!empty($conditions['_id']['$in'])) {
                    foreach ($conditions['_id']['$in'] as $k => $id) {
                        if (is_object($id)) {
                            $idArray = (array) $id;
                            $conditions['_id']['$in'][$k] = new \MongoDB\BSON\ObjectId($idArray['$id']);
                        } else {
                            $conditions['_id']['$in'][$k] = new \MongoDB\BSON\ObjectId($id);
                        }
                    }
                } else if (!empty($conditions['_id']) && is_string($conditions['_id'])) {
                    $conditions['_id'] = new \MongoDB\BSON\ObjectId($conditions['_id']);
                }
            }

            $bulk = new MongoDB\Driver\BulkWrite(['ordered' => self::INSERT_DOCUMENTS_ORDER]);
            $bulk->update($conditions, $update, $options);
            $result = self::$instance->executeBulkWrite($this->database . '.' . $this->collection, $bulk, self::$writeConcern);
            return array(
                'status' => $status,
                'records_updated' => $result->getModifiedCount(),
                'records_upserted' => $result->getUpsertedCount(),
                'acknowledged' => $result->isAcknowledged(),
                'updatedExisting' => $result->getMatchedCount(),
                'updatedIds' => $result->getUpsertedIds(),
                'err' => $result->getModifiedCount() > 0 ? NULL : json_encode($result->getWriteErrors()),
                'n' => ($result->getModifiedCount() + $result->getUpsertedCount()),
                'ok' => 1,
                'errmsg' => $result->getModifiedCount() > 0 ? NULL : json_encode($result->getWriteErrors())
            );
        } catch (Exception $ex) {
            $status = 0;
            $this->handleExceptions($ex);
        }
        return array(
            'status' => $status, 'records_updated' => 0, 'records_upserted' => 0,
            'acknowledged' => 0, 'updatedExisting' => 0, 'err' => '',
            'n' => $result->getModifiedCount(), 'ok' => $result->getModifiedCount(), 'errmsg' => ''
        );
    }

    public function updateByID($id, $update, $options) {
        $status = 1;
        $conditions = array();
        try {
            $this->collection->update($conditions, $update, $options);
            $conditions['_id'] = $this->sanitizeID($id);

            $bulk = new MongoDB\Driver\BulkWrite();
            $bulk->update($conditions, $update, $options);
            $result = self::$instance->executeBulkWrite($this->database . '.' . $this->collection, $bulk, self::$writeConcern);
            return array(
                'status' => $status,
                'records_updated' => $result->getModifiedCount(),
                'records_upserted' => $result->getUpsertedCount(),
                'acknowledged' => $result->isAcknowledged(),
                'updatedExisting' => $result->getMatchedCount(),
                'updatedIds' => $result->getUpsertedIds()
            );
        } catch (Exception $ex) {
            $status = 0;
            $this->handleExceptions($ex);
        }
        return array(
            'status' => $status, 'records_updated' => 0, 'records_upserted' => 0,
            'acknowledged' => 0, 'updatedExisting' => 0
        );
    }

    public function aggregate($options) {
        try {
            $aggregate = new \MongoDB\Driver\Command(
                    array(
                'aggregate' => $this->collection,
                'pipeline' => $options));

            $rows = self::$instance->executeCommand($this->database, $aggregate);

            $documents = array();
            foreach ($rows as $row) {
                if (isset($row->result)) {
                    $documents = $row->result;
                } else {
                    $documents[] = $row;
                }
            }

            $docs = array();
            foreach ($documents as $doc) {
                $docs[] = $this->documentFormatter($doc);
            }

            return array('result' => $docs);
        } catch (Exception $ex) {
            $this->handleExceptions($ex);
        }
    }

    public function findById($id) {
        try {
            $condition = ['_id' => $this->sanitizeID($id)];

            $options = [];
            $query = new \MongoDB\Driver\Query($condition, $options);
            $rows = self::$instance->executeQuery($this->database . '.' . $this->collection, $query);

            foreach ($rows as $document) {
                $document = $document;
            }
            return $this->documentFormatter($document);
        } catch (Exception $ex) {
            $this->handleExceptions($ex);
            $status = 0;
            return array(
                'status' => $status,
                'error' => $ex->getMessage()
            );
        }
    }

    public function findAndModify($conditions = array(), $update = array(), $fields = array(), $options = array()) {
        try {
            $options['limit'] = $conditions['$limit'] = 1;
            $document = $this->find($conditions, $fields);

            if (empty($document)) {
                if (!empty($options['upsert'])) {
                    return $this->insert($update);
                } else {
                    return $document;
                }
            }
            $document = (array) $document[0];
            $this->updateByID($document['_id'], $update, $options);

            if (!empty($options['new'])) {
                $document = $this->findById($document['_id']);
            }
            return $this->documentFormatter($document);
        } catch (Exception $ex) {
            $this->handleExceptions($ex);
        }
    }

    public function batchInsert(&$documents = array(), $options = array()) {
        $status = 1;
        try {
            $insertedDocument = array();
            $bulk = new MongoDB\Driver\BulkWrite(['ordered' => self::INSERT_DOCUMENTS_ORDER]);
            foreach ($documents as $key => $document) {
                $id = (array) $bulk->insert($document);
                $insertedDocument[] = $id['oid'];
                $documents[$key]['_id'] = new MongoId($id['oid']);
            }
            $result = self::$instance->executeBulkWrite($this->database . '.' . $this->collection, $bulk, self::$writeConcern);
            return array(
                'status' => $status,
                'records_inserted' => $result->getInsertedCount(),
                'inserted_ids' => $insertedDocument
            );
        } catch (Exception $ex) {
            $status = 0;
            $this->handleExceptions($ex);
        }
        return array(
            'status' => $status, 'records_inserted' => 0
        );
    }

    public function insert(&$document = array(), $options = array()) {
        try {
            $bulk = new MongoDB\Driver\BulkWrite(['ordered' => self::INSERT_DOCUMENTS_ORDER]);
            $insertedDocument = (array) $bulk->insert($document);
            $inserted = self::$instance->executeBulkWrite($this->database . '.' . $this->collection, $bulk, self::$writeConcern);
            $document['_id'] = new MongoId($insertedDocument['oid']);

            return array(
                'ok' => (bool) $inserted->getInsertedCount(),
                'err' => !$inserted->getInsertedCount(),
                'insertedDocument' => $insertedDocument['oid']
            );
        } catch (Exception $ex) {
            $this->handleExceptions($ex);
            return array();
        }
    }

    public function remove($conditions = array(), $options = array()) {
        try {
            if (!empty($conditions['_id'])) {
                $conditions = ['_id' => $this->sanitizeID($conditions['_id'])];
            }
            $bulk = new MongoDB\Driver\BulkWrite(['ordered' => self::INSERT_DOCUMENTS_ORDER]);
            $bulk->delete($conditions);
            return self::$instance->executeBulkWrite($this->database . '.' . $this->collection, $bulk, self::$writeConcern);
        } catch (Exception $ex) {
            $this->handleExceptions($ex);
        }
    }

    public function count($conditions = array(), $options = array()) {
        try {
            if ($this->useLegacyMongoDriver) {
                $this->collection->count($conditions, $options);
            } else {
                # todo
            }
        } catch (Exception $ex) {
            $this->handleExceptions($ex);
        }
    }

    public function getMongoIds($ids, $delimiter = ',') {

        $mongoIds = array();
        if (is_object($ids)) {
            return array($ids);
        }

        if (is_string($ids)) {
            $ids = explode($delimiter, $ids);
        }

        foreach ($ids as $id) {
            if (!is_object($id)) {
                $id = new MongoId($id);
            }
            $mongoIds[] = $id;
        }
        return $mongoIds;
    }

    public function documentFormatter($document) {
        $updatedDoc = array();
        foreach ($document as $key => $doc) {
            if (($key == '_id' && get_class($doc) == 'stdClass') || $key != '_id') {
                $updatedDoc[$key] = json_decode(json_encode($doc), true);
            } else {
                $updatedDoc[$key] = $doc;
            }
        }
        if (!empty($updatedDoc['_id'])) {
            $documentID = (array) $updatedDoc['_id'];
            if (!empty($documentID['oid'])) {
                $updatedDoc['_id']->{'$id'} = $documentID['oid'];
            }
        }

        return $updatedDoc;
    }

    public function sanitizeID($id) {
        if (is_array($id)) {
            if (!empty($id['$in'])) {
                foreach ($id['$in'] as $key => $val) {
                    $val = (array) $val;
                    $id['$in'][$key] = new \MongoDB\BSON\ObjectId($val['$id']);
                }
                return $id;
            }
            $id = (array) $id[0];
            $id = $id['$id'];
        } else if (is_object($id)) {
            $id = (string) $id;
        }
        return new \MongoDB\BSON\ObjectId($id);
    }

    public function dropDatabase() {
        self::$instance->executeCommand($this->database, new \MongoDB\Driver\Command(["dropDatabase" => 1]));
    }

    public function createCollection($collectionName = NULL) {
        if (!empty($collectionName)) {
            self::$instance->executeCommand($this->database, new \MongoDB\Driver\Command(["create" => $collectionName]));
        }
    }

    public function createIndex($index = array()) {
        if (!empty($index)) {
            $indexName = '';
            foreach ($index as $key => $val) {
                $indexName .= $key . '_';
            }
            $newIndex = array('name' => $this->collection . '_' . $indexName, 'key' => $index);
            $this->createIndexes($this->collection, array($newIndex));
        }
    }

    public function createIndexes($collectionName, $indexes) {

        foreach ($indexes as $key => $value) {
            if (is_array($value)) {
                $value['ns'] = $this->database . "." . $collectionName;
            }

            $command = new MongoDB\Driver\Command([
                "createIndexes" => $collectionName,
                "indexes" => [$value],
            ]);

            $result[] = self::$instance->executeCommand($this->database, $command);
        }
    }

    public function handleExceptions(Exception $ex) {
        echo 'Error occurred : ' . $ex->getMessage() . ', File : ' . $ex->getFile() . ', Line :' . $ex->getLine() . ', Trace :' . $ex->getTraceAsString();
    }

}
?>

