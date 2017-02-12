<?php

namespace Queue\Driver;

use Queue\Job\JobInterface;
use Queue\Job\Job;
use Psr\Log\LogLevel;

abstract class Driver implements DriverInterface
{
    use \Queue\Logger\LoggerTrait;

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function addJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, 'addJob ' . $job->getName(), $job->getData());
        $hash = $job->getHash();
        // \Akop\Util::pre($hash, 'addJob $hash');
        return (
            count($this->getNewJobByHash($hash))
                ||
            $this->insertJob($job)
        );
    }

    abstract protected function insertJob($job);
    abstract public function getNewJobTypes($type);

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

    public function moveJobToEnd(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, 'moveJobToEnd ' . $job->getId(), $job->getData());
    }

    protected function getJobByData($data)
    {
        \Akop\Util::pre([$data, $data['job'], $jobUnserialized], 'resolveJob__ rawData');
        if (!data) {
            return false;
        }
        $jobUnserialized = unserialize($data['payload']);
        if (is_array($jobUnserialized) && isset($jobUnserialized['data'])) {
            // if (is_array($jobUnserialized)) {
            $jobClass = '\\' . $data['worker'];
            $job = new $jobClass($data['name'], $jobUnserialized['data']);
            $job->setDataAll($data);
        }

        if (!$job) {
            $this->setJobStatus($data['id'], Job::STATUS_ERROR);
        }
        return $job;
    }

    private function getNewJobByHash($hash)
    {
        return $this->getJobs([
            'status' => Job::STATUS_NEW,
            'hash' => $hash,
        ]);
    }
}
