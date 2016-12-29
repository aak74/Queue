<?php

namespace Queue\Driver;

use Queue\Job\Job as Job;
use Queue\Job\JobInterface;
use Psr\Log\LogLevel;

class BitrixHl extends Driver
{
    private $elementObj;

    public function __construct($elementObj)
    {
        $this->elementObj = $elementObj;
    }

    protected function insertJob($queueName, $hash, $jobData)
    {
        return $this->elementObj->add([
            'hash' => $hash,
            'job' => $jobData,
            'name' => $queueName,
            'status' => Job::STATUS_NEW,
        ]);
    }

    /**
     * @param string $queueName
     *
     * @return JobInterface
    */

    public function resolveJob($queueName)
    {
        $this->log(LogLevel::DEBUG, 'Driver resolveJob ' . $queueName);
        // \Gb\Util::pre($queueName, 'PDO resolveJob');

        $data = $this->elementObj->getRow([
            'order' => ['tries', 'id'],
            'filter' => [
                'name' => $queueName,
                'status' => Job::STATUS_NEW,
                '<tries' => Job::MAX_TRIES,
            ]
        ]);
        return $this->getJobByData($data);
    }

    public function removeJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, 'removeJob ' . $job->getId(), $job->getData());
        $this->elementObj->update(
            $job->getId(),
            [
                'status' => Job::STATUS_DONE,
                'result' => serialize($job->getResult())
            ]
        );
    }

    public function moveJobToEnd(JobInterface $job)
    {
        // \Gb\Util::pre([$job, $job->getPropertyByName('attempts')], 'Driver\PDO moveJobToEnd');
        // \Gb\Util::pre($job, 'removeJob');
        $this->log(LogLevel::DEBUG, 'moveJobToEnd ' . $job->getId(), $job->getData());
        $this->elementObj->update(
            $job->getId(),
            [
                'tries' => 'tries + 1',
                'result' => serialize($job->getResult())
            ]
        );
    }

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function buryJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "buryJob {$job->getId()}", $job->getData());
        $this->elementObj->update(
            $job->getId(),
            [
                'status' => Job::STATUS_BURIED
            ]
        );
    }

    /**
     * @todo Перенести подобный функционал на уровень Driver
     */
    public function updateJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "updateJob {$job->getName()}", $job->getData());
        $this->elementObj->update(
            $job->getId(),
            [
                'status' => $job->getStatus(),
                'name' => $job->getName(),
                // 'queue' => $this->normalizeQueue($job->getName()),
                'job' => $job->getSerialized(),
                'result' => serialize($job->getResult()),
                'hash' => $job->getHash(),
            ]
        );
    }

    public function getJobs($filter)
    {
        return $this->elementObj->getList(['filter' => $filter]);
    }

    public function getJobById($jobId)
    {
        return $this->elementObj->getList([
                'filter' => [
                    'id' => $jobId,
                ]
            ]);
    }

    public function getNewJobs($queueName)
    {
        return $this->elementObj->getList([
                'filter' => [
                    'status' => Job::STATUS_NEW,
                    'name' => $queueName,
                ]
            ]);
    }

    protected function setJobStatus($jobId, $status)
    {
        $this->elementObj->update(
            $jobId,
            ['status' => $status]
        );
    }

    // private function normalizeQueue($queueName)
    // {
    //     return str_replace('\\', '\\\\', $queueName);
    // }
}
