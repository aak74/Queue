<?php

namespace Queue\Driver;

use Queue\Job\Job as Job;
use Queue\Job\JobInterface;
use Psr\Log\LogLevel;

class PDO extends Driver
{
    private $pdo;
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
        $jobData = $this->getNewJobByHash($hash);
        // \Gb\Util::pre([$jobData, $hash], 'PDO addJob jobData');
        if ($jobData) {
            $result = $jobData['id'];
        } else {
            $this->insertJob($queueName, $hash, $job->getSerialized());
            $result = true;
        }
        return $result;
    }

    public function insertJob($queueName, $hash, $jobData)
    {
        // \Gb\Util::pre([$queueName, $hash, $jobData], 'insertJob');
        $sql = <<<SQL
            INSERT INTO $this->tablename
            (hash, job, queue, status, created_at)
            VALUES (:hash, :job, :queue, :status, NOW())
SQL;

        $stmt = $this->runQuery(
            $sql,
            [
                'hash' => $hash,
                'job' => $jobData,
                'queue' => $queueName,
                'status' => Job::STATUS_NEW
            ],
            true
        );
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
            $this->log(LogLevel::DEBUG, 'PDO resolveJob ' . $queueName, $data);

            // \Gb\Util::pre([$data, $data['job'], $jobUnserialized], 'resolveJob__ rawData');
            $jobUnserialized = unserialize($data['job']);
            if (is_array($jobUnserialized) && isset($jobUnserialized['data'])) {
            // if (is_array($jobUnserialized)) {
                $job = new Job($jobUnserialized['name'], $jobUnserialized['data']);
                $job->setId($data['id']);
            } else {
                $this->updateJobAttempts($data['id'], $data['attempts']+1);
            }
        }

        $stmt->closeCursor();
        return $job;
    }

    public function removeJob($queueName, JobInterface $job, array $jobResult = [])
    {
        // \Gb\Util::pre([$queueName, $job], 'Driver\PDO removeJob');
        // \Gb\Util::pre($job, 'removeJob');
        $this->log(LogLevel::DEBUG, 'removeJob ' . $job->getId(), $job->getData());
        $stmt = $this->runQuery(
            'UPDATE ' . $this->tablename . ' SET `status` = :status, `result` = :result WHERE `id` = :id',
            ['id' => $job->getId(), 'status' => Job::STATUS_DONE, 'result' => serialize($jobResult)],
            true
        );
    }

    public function moveJobToEnd(JobInterface $job, $jobResult)
    {
        // \Gb\Util::pre([$job, $job->getPropertyByName('attempts')], 'Driver\PDO moveJobToEnd');
        // \Gb\Util::pre($job, 'removeJob');
        $this->log(LogLevel::DEBUG, 'moveJobToEnd ' . $job->getId(), $job->getData());
        $stmt = $this->runQuery(
            'UPDATE ' . $this->tablename . ' SET `attempts` = `attempts` + 1, `result` = :result WHERE `id` = :id',
            ['id' => $job->getId(), 'result' => serialize($jobResult)],
            // ['id' => $job->getId(), 'attempts' => $job->getPropertyByName('attempts') + 1],
            true
        );
    }

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function buryJob($queueName, JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "buryJob {$job->getId()}", $job->getData());
        $stmt = $this->runQuery(
            'UPDATE ' . $this->tablename . ' SET `status` = :status WHERE `id` = :id',
            ['id' => $job->getId(), 'status' => Job::STATUS_BURIED],
            true
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
        $stmt = $this->runQuery(
            $sql,
            [
                'id' => $job->getId(),
                'status' => $job->getStatus(),
                'queue' => $this->normalizeQueue($job->getName()),
                'job' => $job->getSerialized(),
                'hash' => $job->getHash(),
            ],
            true
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

    public function getJobById($id)
    {
        return $this->getJobBySQL(
            'SELECT * FROM ' . $this->tablename . ' WHERE `id` = :id',
            ['id' => $id]
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
            'SELECT * FROM ' . $this->tablename . ' WHERE `queue` LIKE :queue AND `status` = :status AND `attempts` < :maxAttempts',
            ['queue' => $this->normalizeQueue($queueName) . '%', 'status' => Job::STATUS_NEW, 'maxAttempts' => 5]
        );

        $result = $stmt->fetchAll();
        $stmt->closeCursor();
        // \Gb\Util::pre([$queueName, $result], 'getNewJobsChildren');

        return $result;
    }


    public function removeNewJobsWithChildren($queueName)
    {
        return $this->runQuery(
            'DELETE FROM ' . $this->tablename . ' WHERE `queue` LIKE :queue AND `status` = :status',
            ['queue' => $this->normalizeQueue($queueName) . '%', 'status' => Job::STATUS_NEW],
            true
        );
    }


    public function getJobByHash($hash)
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

    public function getNewJobByHash($hash)
    {

        $stmt = $this->runQuery(
            'SELECT * FROM ' . $this->tablename . ' WHERE hash = :hash AND status = :status LIMIT 1',
            ['hash' => $hash, 'status' => Job::STATUS_NEW]
        );
        $result = $stmt->fetch();
        $stmt->closeCursor();
        // \Gb\Util::pre([$hash, $result], 'getNewJobByHash $hash');

        return $result;
    }

    public function removeJobs($queueName)
    {
        $stmt = $this->runQuery(
            'DELETE FROM ' . $this->tablename . ' WHERE queue LIKE :queue',
            ['queue' => $queueName],
            true
        );
    }

    public function removeAllJobs()
    {
        $stmt = $this->runQuery('DELETE FROM ' . $this->tablename, [], true);
    }

    private function updateJobAttempts($id, $attempts)
    {
        $stmt = $this->runQuery(
            "UPDATE $this->tablename SET `attempts` = :attempts WHERE `id` = :id",
            [
                'id' => $id,
                'attempts' => $attempts,
            ],
            true
        );
    }

    private function runQuery($sql, $params = [], $autoclose = false)
    {
        // \Gb\Util::pre([$sql, $params], 'runQuery');
        $stmt = $this->db->prepare($sql);
        $stmt->execute($params);
        if ($autoclose) {
            $stmt->closeCursor();
        }
        return $stmt;
    }

    private function normalizeQueue($queueName)
    {
        return str_replace('\\', '\\\\', $queueName);
    }
}
