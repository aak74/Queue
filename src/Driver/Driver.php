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
        $this->log(LogLevel::DEBUG, 'addJob ' . $job->getName(), (array)$job->getData());
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
        \Akop\Util::pre($data, 'resolveJob__ rawData');
        if (!data) {
            return false;
        }
        $jobUnserialized = unserialize($data['payload']);
        // \Akop\Util::pre([$data, $data['payload'], $jobUnserialized], 'resolveJob__ rawData');
        // if (is_array($jobUnserialized)) {
        $jobClass = '\\' . $data['worker'];
        $jobData = (is_array($jobUnserialized) && isset($jobUnserialized['data']))
            ? $jobUnserialized['data']
            : [];
        $job = new $jobClass($data['name'], $jobData);
        $job->setDataAll($data);
        return $job;

        // if (!$job) {
        //     $this->setJobStatus($data['id'], Job::STATUS_ERROR);
        // }
        // \Akop\Util::pre($job, 'resolveJob__ job');
        // return $job;
    }

    private function getNewJobByHash($hash)
    {
        return $this->getJobs([
            'status' => Job::STATUS_NEW,
            'hash' => $hash,
        ]);
    }
}
