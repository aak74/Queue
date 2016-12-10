<?php

namespace Queue\Driver;

use Queue\Job\JobInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

class Driver implements DriverInterface
{
    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function addJob($queueName, JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "Job {$job->getName()} added to {$queueName}", $job->getData());
    }

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
    public function removeJob($queueName, JobInterface $job, $jobResult)
    {
        $this->log(LogLevel::DEBUG, "Job {$job->getName()} removed from {$queueName}", $job->getData());
    }

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function buryJob($queueName, JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "Job {$job->getName()} buried from {$queueName}", $job->getData());
    }

    public function updateJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, "Job {$job->getName()} updated", $job->getData());
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
}
