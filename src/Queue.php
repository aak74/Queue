<?php

namespace Queue;

use Queue\Driver\DriverInterface;
use Queue\Job\JobInterface;
use Queue\Job\Job as Job;
// use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

class Queue
{
    use Logger\LoggerTrait;
    /**
     * @var DriverInterface
     */
    private $driver;
    private $namespace;

    /**
     * @var
     */
    private $name;

    public function __construct(DriverInterface $driver)
    {
        $this->driver = $driver;
    }

    public function setName($name)
    {
        $this->name = $name;
    }

    public function runJob()
    {
        return $this->runJobByJob($this->resolveJob());
    }

    public function runJobById($jobId)
    {
        return $this->runJobByJob($this->getJobById($jobId));
    }

    private function runJobByJob(JobInterface $job)
    {
        // \Akop\Util::pre($job, 'runJobByJob job');
        // $this->log(LogLevel::DEBUG, 'runJobByJob ' . $this->name, (array)$job);
        $this->log(LogLevel::DEBUG, 'runJobByJob ' . $this->name);
        $job->run();
        $result = true;
        if (!empty($job->addToQueue) && is_array($job->addToQueue)) {
            foreach ($job->addToQueue as $item) {
                $result = $result && $this->addJobToQueue($item);
            }
        }
        // \Akop\Util::pre([$result, $job], 'runJobByJob');

        if ($result) {
            $this->updateJob($job);
        }
        return $result;
    }

    private function addJobToQueue(JobInterface $job, $queue)
    {
        return $this->driver->addJob($queue, $job);
    }

    /**
     * @param JobInterface $job
     */
    public function addJob(JobInterface $job)
    {
        $this->driver->addJob($job);
    }

    public function addJobByType($typeName, array $params = [])
    {
        $jobType = $this->jobTypes->getByName($typeName);
        $name = str_replace('#NAMESPACE#', $this->namespace, $jobType['executor']);
        $this->addJobToQueue(new Job($name, ['data' => $params]), $name);
    }

    /**
     * @return JobInterface
     */
    public function resolveJob()
    {
        $this->log(LogLevel::DEBUG, 'resolveJob');
        return $this->driver->resolveJob($this->name);
    }

    public function getJobById($jobId)
    {
        return $this->driver->getJobById($jobId);
    }

    /**
     * @param JobInterface $job
     */
    public function removeJob(JobInterface $job)
    {
        $this->driver->removeJob($job);
    }

    /**
     * @param JobInterface $job
     */
    public function buryJob(JobInterface $job)
    {
        $this->driver->buryJob($job);
    }

    public function updateJob(JobInterface $job)
    {
        // \Akop\Util::pre($job, 'Queue updateJob');
        $this->driver->updateJob($job);
    }

    public function getName()
    {
        return $this->name;
    }

    public function getJobs()
    {
        return $this->driver->getJobs($this->name);
    }

    public function getNewJobs()
    {
        return $this->driver->getNewJobs($this->name);
    }

    public function getNewJobTypes($type)
    {
        return $this->driver->getNewJobTypes($type);
    }

    public function setNamespace($namespace)
    {
        $this->namespace = $namespace;
    }
}
