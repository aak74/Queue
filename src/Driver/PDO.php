<?php

namespace Queue\Driver;

use Queue\Job\Job as Job;
use Queue\Job\JobInterface;
use Psr\Log\LogLevel;

class PDO extends Driver
{
    private $db;
    protected $tablename = 'jobs';

    public function connect($dsn, $username, $password, array $options = [])
    {
        $this->db = new \PDO($dsn, $username, $password, $options);
    }

    public function setConnection(\PDO $db)
    {
        $this->db = $db;
    }

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function addJob($queueName, JobInterface $job)
    {
        // \Gb\Util::pre([$queueName, $job->getData()], 'PDO addJob');
        $this->log(LogLevel::DEBUG, 'addJob ' . $queueName, $job->getData());
        $hash = $job->getHash();
        return (
            count($this->getNewJobByHash($hash))
                ||
            $this->insertJob($queueName, $hash, $job->getSerialized())
        );
    }

    private function insertJob($queueName, $hash, $jobData)
    {
        // \Gb\Util::pre([$queueName, $hash, $jobData], 'insertJob');
        $sql = <<<SQL
            INSERT INTO $this->tablename
            (hash, job, queue, status, created_at)
            VALUES (:hash, :job, :queue, :status, NOW())
SQL;

        $this->execQuery(
            $sql,
            [
                'hash' => $hash,
                'job' => $jobData,
                'queue' => $queueName,
                'status' => Job::STATUS_NEW
            ]
        );
        return true;
    }

    /**
     * @param string $queueName
     *
     * @return JobInterface
    */

    public function resolveJob($queueName)
    {
        $this->log(LogLevel::DEBUG, 'PDO resolveJob ' . $queueName);
        // \Gb\Util::pre($queueName, 'PDO resolveJob');
        $sql = <<<SQL
            SELECT *
            FROM $this->tablename
            WHERE `queue` LIKE :queue
                AND `status` = :status
                AND `attempts` < :maxAttempts
            ORDER BY `attempts` asc, `id` asc LIMIT 1
SQL;

        return $this->getJobBySQL(
            $sql,
            ['queue' => $this->normalizeQueue($queueName), 'status' => Job::STATUS_NEW, 'maxAttempts' => 5]
        );
    }

    private function getJobBySQL($sql, $params)
    {
        $job = false;
        $stmt = $this->runQuery(
            $sql,
            $params
        );

        if ($data = $stmt->fetch()) {
            $this->log(LogLevel::DEBUG, 'PDO resolveJob ', $data);

            // \Gb\Util::pre([$data, $data['job'], $jobUnserialized], 'resolveJob__ rawData');
            $jobUnserialized = unserialize($data['job']);
            if (is_array($jobUnserialized) && isset($jobUnserialized['data'])) {
            // if (is_array($jobUnserialized)) {
                $job = new Job($jobUnserialized['name'], $jobUnserialized['data']);
                $job->setId($data['id']);
            }

            if (!$job) {
                $this->setJobStatus($data['id'], Job::STATUS_ERROR);
            }
        }

        $stmt->closeCursor();
        return $job;
    }

    public function removeJob(JobInterface $job, array $jobResult = [])
    {
        // \Gb\Util::pre([$queueName, $job], 'Driver\PDO removeJob');
        // \Gb\Util::pre($job, 'removeJob');
        $this->log(LogLevel::DEBUG, 'removeJob ' . $job->getId(), $job->getData());
        $this->execQuery(
            'UPDATE ' . $this->tablename
                . ' SET `status` = :status, `result` = :result WHERE `id` = :id',
            ['id' => $job->getId(), 'status' => Job::STATUS_DONE, 'result' => serialize($jobResult)]
        );
    }

    public function moveJobToEnd(JobInterface $job, $jobResult)
    {
        // \Gb\Util::pre([$job, $job->getPropertyByName('attempts')], 'Driver\PDO moveJobToEnd');
        // \Gb\Util::pre($job, 'removeJob');
        $this->log(LogLevel::DEBUG, 'moveJobToEnd ' . $job->getId(), $job->getData());
        $this->execQuery(
            'UPDATE ' . $this->tablename
                . ' SET `attempts` = `attempts` + 1, `result` = :result WHERE `id` = :id',
            ['id' => $job->getId(), 'result' => serialize($jobResult)]
        );
    }

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function buryJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "buryJob {$job->getId()}", $job->getData());
        $this->execQuery(
            'UPDATE ' . $this->tablename . ' SET `status` = :status WHERE `id` = :id',
            ['id' => $job->getId(), 'status' => Job::STATUS_BURIED]
        );
    }

    public function updateJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "updateJob {$job->getName()}", $job->getData());

        $sql = <<<SQL
            UPDATE $this->tablename
            SET `status` = :status, `queue` LIKE :queue, `job` = :job, `hash` = :hash
            WHERE `id` = :id
SQL;
        // \Gb\Util::pre([$job, $sql], 'updateJob job');
        $this->execQuery(
            $sql,
            [
                'id' => $job->getId(),
                'status' => $job->getStatus(),
                'queue' => $this->normalizeQueue($job->getName()),
                'job' => $job->getSerialized(),
                'hash' => $job->getHash(),
            ]
        );
    }


    public function getJobs($queueName)
    {
        $stmt = $this->runQuery(
            'SELECT * FROM ' . $this->tablename . ' WHERE `queue` LIKE :queue',
            // ['queue' => $queueName]
            ['queue' => str_replace('\\', '\\\\', $queueName)]
        );

        $result = $stmt->fetchAll();
        $stmt->closeCursor();

        // \Gb\Util::pre([$queueName, $result], 'getJobs');
        return $result;
    }

    public function getJobById($jobId)
    {
        return $this->getJobBySQL(
            'SELECT * FROM ' . $this->tablename . ' WHERE `id` = :id',
            ['id' => $jobId]
        );
    }

    public function getNewJobs($queueName)
    {
        $stmt = $this->runQuery(
            'SELECT * FROM ' . $this->tablename . ' WHERE queue LIKE :queue AND `status` = :status',
            ['queue' => $this->normalizeQueue($queueName), 'status' => Job::STATUS_NEW]
        );

        $result = $stmt->fetchAll();
        $stmt->closeCursor();

        return $result;
    }

    public function getNewJobsWithChildren($queueName)
    {
        $stmt = $this->runQuery(
            'SELECT * FROM ' . $this->tablename
                . ' WHERE `queue` LIKE :queue AND `status` = :status AND `attempts` < :maxAttempts',
            [
                'queue' => $this->normalizeQueue($queueName) . '%',
                'status' => Job::STATUS_NEW,
                'maxAttempts' => 5
            ]
        );

        $result = $stmt->fetchAll();
        $stmt->closeCursor();
        // \Gb\Util::pre([$queueName, $result], 'getNewJobsChildren');

        return $result;
    }


    public function removeNewJobsWithChildren($queueName)
    {
        return $this->execQuery(
            'DELETE FROM ' . $this->tablename
                . ' WHERE `queue` LIKE :queue AND `status` = :status',
            [
                'queue' => $this->normalizeQueue($queueName) . '%',
                'status' => Job::STATUS_NEW
            ]
        );
    }

/*
    private function getJobByHash($hash)
    {
        $stmt = $this->runQuery(
            'SELECT * FROM ' . $this->tablename . ' WHERE hash = :hash LIMIT 1',
            ['hash' => $hash]
        );
        $result = $stmt->fetch();
        $stmt->closeCursor();
        // \Gb\Util::pre([$hash, $result], 'getNewJobByHash $hash');

        return $result;
    }
*/
    private function getNewJobByHash($hash)
    {
        $stmt = $this->runQuery(
            'SELECT * FROM ' . $this->tablename
                . ' WHERE hash = :hash AND status = :status LIMIT 1',
            ['hash' => $hash, 'status' => Job::STATUS_NEW]
        );
        $result = $stmt->fetch();
        $stmt->closeCursor();
        // \Gb\Util::pre([$hash, $result], 'getNewJobByHash $hash');

        return $result;
    }

    public function removeJobs($queueName)
    {
        $this->execQuery(
            'DELETE FROM ' . $this->tablename
                . ' WHERE queue LIKE :queue',
            ['queue' => $queueName]
        );
    }

    public function removeAllJobs()
    {
        $this->execQuery('DELETE FROM ' . $this->tablename, []);
    }

/*
    private function updateJobAttempts($jobId, $attempts)
    {
        $this->execQuery(
            "UPDATE $this->tablename SET `attempts` = :attempts WHERE `id` = :id",
            [
                'id' => $jobId,
                'attempts' => $attempts,
            ]
        );
    }
*/

    private function setJobStatus($jobId, $status)
    {
        $this->runQuery(
            "UPDATE $this->tablename SET `status` = :status WHERE `id` = :id",
            [
                'id' => $jobId,
                'status' => $status,
            ]
        );
    }

    private function runQuery($sql, $params = [])
    {
        // \Gb\Util::pre([$sql, $params], 'runQuery');
        $stmt = $this->db->prepare($sql);
        $stmt->execute($params);
        return $stmt;
    }

    private function execQuery($sql, $params = [])
    {
        $stmt = $this->runQuery($sql, $params);
        $stmt->closeCursor();
    }

    private function normalizeQueue($queueName)
    {
        return str_replace('\\', '\\\\', $queueName);
    }
}
