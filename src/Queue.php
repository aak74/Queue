<?php

/**
 * This file is part of the Queue package.
 *
 * Originally author (c) Dries De Peuter <dries@nousefreak.be>
 * coauthor: (c) Andrew Kopylov <aa74ko@gmail.com>
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace Queue;

use Queue\Driver\DriverInterface;
use Queue\Job\JobInterface;
use Queue\Job\Job as Job;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

class Queue
{
    /**
     * @var DriverInterface
     */
    private $driver;
    private $namespace;
    private $logger;

    /**
     * @var
     */
    private $name;

    public function __construct(
        DriverInterface $driver,
        $name = 'worker_queue',
        $jobTypes = null
    ) {
        $this->driver = $driver;
        $this->name = $name;
        $this->jobTypes = $jobTypes;
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
        $result = false;
        if ($jobResult = $job->run()) {
            $result = true;
            // \Gb\Util::pre([$updaterName, $nextQueue, $jobResult], 'jobResult');
            foreach ($jobResult as $item) {
                $result = $result
                    && $this->executeUpdater($item)
                    && $this->addToQueue($item);
            }
        } else {
            $try = $this->jobTypes->getPropByName(
                $this->getPropertyByName('try'),
                'executor',
                $this->namespace
            );
            if ($try && $jobResult = $job->tryRun($try)) {
                $result = $this->addJobToQueue(new Job($try, $job->getData()), $try);
            }
        }
        if ($result) {
            $this->removeJob($job, $jobResult);
        } else {
            $this->moveJobToEnd($job, $jobResult);
        }
    }

    private function executeUpdater($item)
    {
        $result = true;
        if ($updaterName = $this->getUpdater()) {
            $updater =  new $updaterName;
            $result =  $updater->execute(new Job($updaterName, $item));
        }
        return $result;
    }

    private function addToQueue($item)
    {
        $result = true;
        if ($nextQueue = $this->getNextQueue()) {
            $result = $this->addJobToQueue(new Job($nextQueue, $item), $nextQueue);
        }
        return $result;
    }

    private function getUpdater()
    {
        return $this->getPropertyByName('updater');
    }

    private function getNextQueue()
    {
        return $this->getPropertyByName('nextQueue');
    }
/*
    private function getExcecutor()
    {
        return $this->getPropertyByName('executor');
    }
*/
    private function getPropertyByName($name)
    {
        return $this->jobTypes->getPropByName($this->getQueueCleanName(), $name, $this->namespace);
    }


    private function getQueueCleanName()
    {
        $parts = explode('\\', $this->name);
        end($parts);
        return current($parts);
    }
/*
    private function update($item, $updater)
    {
        if ($updater) {
            $result = $updater->execute($item);
        }
        return $result;
    }
*/
    public function addJobToQueue(JobInterface $job, $queue)
    {
        // \Gb\Util::pre([$queue, $job->getData()], 'addJobToQueue');
        return $this->driver->addJob($queue, $job);
    }

    /**
     * @param JobInterface $job
     */
    public function addJob(JobInterface $job)
    {
        // \Gb\Util::pre([$this->getName(), $job], 'Queue addJob');
        $this->driver->addJob($this->getName(), $job);
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
    public function removeJob(JobInterface $job, array $jobResult = [])
    {
        // \Gb\Util::pre([$this->name, $job], 'Queue removeJob');
        $this->driver->removeJob($job, $jobResult);
    }

    private function moveJobToEnd(JobInterface $job)
    {
        // \Gb\Util::pre([$this->name, $job], 'Queue moveJobToEnd');
        $this->driver->moveJobToEnd($job);
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
