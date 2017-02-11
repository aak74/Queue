<?php

namespace Queue;

use Queue\Driver\DriverInterface;
use Queue\Job\JobInterface;
use Queue\Job\Job as Job;
// use Psr\Log\LoggerInterface;
// use Psr\Log\LogLevel;

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
        $this->runJobByJob($this->resolveJob());
    }

    public function runJobById($jobId)
    {
        $this->runJobByJob($this->getJobById($jobId));
    }

    private function runJobByJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, 'runJobByJob ' . $this->name);
        $job->run();
        $result = true;
        // \Gb\Util::pre([$updaterName, $nextQueue, $jobResult], 'jobResult');
        foreach ($job->addToQueue as $item) {
            $result = $result && $this->addJobToQueue($item);
        }

        if ($result) {
            $this->updateJob($job);
        }
    }
/*
    public function addJobToQueue(JobInterface $job, $queue)
    {
        // \Gb\Util::pre([$queue, $job->getData()], 'addJobToQueue');
        return $this->driver->addJob($queue, $job);
    }
*/
    /**
     * @param JobInterface $job
     */
    public function addJob(JobInterface $job)
    {
        // \Gb\Util::pre([$this->getName(), $job], 'Queue addJob');
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
        // \Gb\Util::pre($job, 'Queue updateJob');
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

    public function getNewJobsWithChildren()
    {
        return $this->driver->getNewJobsWithChildren($this->name);
    }

    public function getNewJobTypes($renameTo = 'name')
    {
        $jobTypes = [];
        $jobs = $this->getNewJobsWithChildren($this->name);
        foreach ($jobs as $job) {
            ++$jobTypes[$job['queue']];
        }
        return $this->jobTypes->getRenamedAndSorted(
            $jobTypes,
            $this->name,
            $renameTo
        );

        return $jobTypes;
    }

    public function setNamespace($namespace)
    {
        $this->namespace = $namespace;
    }
}
