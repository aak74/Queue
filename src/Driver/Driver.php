<?php

namespace Queue\Driver;

use Queue\Job\JobInterface;
use Queue\Job\Job;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

abstract class Driver implements DriverInterface
{
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function addJob(JobInterface $job)
    {
        // \Gb\Util::pre([$queueName, $job->getData()], 'PDO addJob');
        $this->log(LogLevel::DEBUG, 'addJob ' . $job->getName(), $job->getData());
        $hash = $job->getHash();
        return (
            count($this->getNewJobByHash($hash))
                ||
            $this->insertJob($job->getName(), $hash, $job->getSerialized())
        );
    }

    abstract protected function insertJob($queueName, $hash, $jobData);

    /**
     * @param string $queueName
     *
     * @return JobInterface
     */
    public function resolveJob($queueName)
    {
        $this->log(LogLevel::DEBUG, 'Job resolved '.$queueName);
    }

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function removeJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "Job {$job->getName()} removed", $job->getData());
    }

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function buryJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "Job {$job->getName()} buried", $job->getData());
    }

    public function updateJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "Job {$job->getName()} updated", $job->getData());
    }


    protected function getJobByData($data)
    {
        // \Gb\Util::pre([$data, $data['job'], $jobUnserialized], 'resolveJob__ rawData');
        if (!data) {
            return false;
        }
        $jobUnserialized = unserialize($data['job']);
        if (is_array($jobUnserialized) && isset($jobUnserialized['data'])) {
            // if (is_array($jobUnserialized)) {
            $job = new Job($jobUnserialized['name'], $jobUnserialized['data']);
            $job->setDataAll($data);
        }

        if (!$job) {
            $this->setJobStatus($data['id'], Job::STATUS_ERROR);
        }
        return $job;
    }
    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    protected function log($level, $message, $context = [])
    {
        if (!$this->logger) {
            return;
        }

        $this->logger->log($level, $message, $context);
    }

    private function getNewJobByHash($hash)
    {
        return $this->getJobs([
            'status' => Job::STATUS_NEW,
            'hash' => $hash,
        ]);
    }
}
