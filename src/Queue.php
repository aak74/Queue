<?php

/**
 * This file is part of the Queue package.
 *
 * (c) Dries De Peuter <dries@nousefreak.be>
 *
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
        $this->log(LogLevel::DEBUG, 'run ' . $this->name);
        $result = false;
        $job = $this->resolveJob();
        if ($job && ($jobResult = $job->run())) {
            $result = true;
            $updaterName = $this->getUpdater();
            $updater = (($updaterName)
                ? new $updaterName
                : null
            );

            $nextQueue = $this->getNextQueue();
            // \Gb\Util::pre([$updaterName, $nextQueue, $jobResult], 'jobResult');

            foreach ($jobResult as $item) {
                // \Gb\Util::pre($item, 'jobResult item');
                if ($updater) {
                    // \Gb\Util::pre($updater, 'updater');
                    $result = $result && $updater->execute(new Job($updaterName, $item));
                }
                if ($nextQueue) {
                    // \Gb\Util::pre($nextQueue, 'nextQueue');
                    $result = $result && $this->addJobToQueue(new Job($nextQueue, $item), $nextQueue);
                }
            }
        } else {
            $try = $this->jobTypes->getPropByName(
                $this->getPropertyByName('try'),
                'executor',
                $this->namespace
            );
            if ($try && $jobResult = $job->tryRun($try)) {
                // $this->moveJobToNext($job);

                $nextQueue = $this->getNextQueue();
                // \Gb\Util::pre([$try, $nextQueue, $jobResult], 'jobResult tryRun');
                $result = $this->addJobToQueue(new Job($try, $job->getData()), $try);
            }
        }
        if ($result) {
            $this->removeJob($job, $jobResult);
        } else {
            $this->moveJobToEnd($job, $jobResult);
        }
    }

    public function runJobById($id)
    {
        $this->log(LogLevel::DEBUG, 'run ' . $this->name);
        $result = false;
        $job = $this->getJobById($id);
        \Gb\Util::pre($job, 'Queue runJobById job');
        if ($job && ($jobResult = $job->run())) {
            $result = true;
            $updaterName = $this->getUpdater();
            $updater = (($updaterName)
                ? new $updaterName
                : null
            );

            $nextQueue = $this->getNextQueue();
            // \Gb\Util::pre([$updaterName, $nextQueue, $jobResult], 'jobResult');

            foreach ($jobResult as $item) {
                // \Gb\Util::pre($item, 'jobResult item');
                if ($updater) {
                    // \Gb\Util::pre($updater, 'updater');
                    $result = $result && $updater->execute(new Job($updaterName, $item));
                }
                if ($nextQueue) {
                    // \Gb\Util::pre($nextQueue, 'nextQueue');
                    $result = $result && $this->addJobToQueue(new Job($nextQueue, $item), $nextQueue);
                }
            }
        } else {
            $try = $this->jobTypes->getPropByName(
                $this->getPropertyByName('try'),
                'executor',
                $this->namespace
            );
            if ($try && $jobResult = $job->tryRun($try)) {
                // $this->moveJobToNext($job);

                $nextQueue = $this->getNextQueue();
                // \Gb\Util::pre([$try, $nextQueue, $jobResult], 'jobResult tryRun');
                $result = $this->addJobToQueue(new Job($try, $job->getData()), $try);
            }
        }
        if ($result) {
            $this->removeJob($job, $jobResult);
        } else {
            $this->moveJobToEnd($job, $jobResult);
        }
    }

    public function getUpdater()
    {
        return $this->getPropertyByName('updater');
    }

    public function getNextQueue()
    {
        return $this->getPropertyByName('nextQueue');
    }

    public function getExcecutor()
    {
        return $this->getPropertyByName('executor');
    }

    public function getPropertyByName($name)
    {
        return $this->jobTypes->getPropByName($this->getQueueCleanName(), $name, $this->namespace);
    }


    public function getQueueCleanName()
    {
        $parts = explode('\\', $this->name);
        end($parts);
        return current($parts);
    }

    public function update($item, $updater)
    {
        if ($updater) {
            $result = $updater->execute($item);
        }
        return $result;
    }

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
        $jobData = [
            // 'executor' => $name,
            // 'nextQueue' => str_replace('#NAMESPACE#', $this->namespace, $jobType['nextQueue']),
            // 'updater' => $jobType['updater'],
            'data' => $params
        ];
        $this->addJobToQueue(new Job($name, $jobData), $name);
    }

    /**
     * @return JobInterface
     */
    public function resolveJob()
    {
        return $this->driver->resolveJob($this->name);
    }

    public function getJobById($id)
    {
        return $this->driver->getJobById($id);
    }

    /**
     * @param JobInterface $job
     */
    public function removeJob(JobInterface $job, array $jobResult = [])
    {
        // \Gb\Util::pre([$this->name, $job], 'Queue removeJob');
        $this->driver->removeJob($this->name, $job, $jobResult);
    }

    public function moveJobToEnd(JobInterface $job, $jobResult)
    {
        // \Gb\Util::pre([$this->name, $job], 'Queue moveJobToEnd');
        $this->driver->moveJobToEnd($job);
    }

    /**
     * @param JobInterface $job
     */
    public function buryJob(JobInterface $job)
    {
        $this->driver->buryJob($this->name, $job);
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

    public function removeNewJobsWithChildren()
    {
        // return $this->driver->removeNewJobsWithChildren($this->name);
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
