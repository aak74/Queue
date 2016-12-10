<?php

/**
 * This file is part of the Queue package.
 *
 * (c) Dries De Peuter <dries@nousefreak.be>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace Queue\Worker;

use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Queue\Executor\JobExecutorInterface;
use Queue\Job\JobInterface;
use Queue\Job\Job;
use Queue\Queue;

class Worker
{
    protected $excecuted = 0;

    /**
     * @var int
     */
    private $workerId;

    /**
     * @var string
     */
    private $instanceHash;

    /**
     * @var bool
     */
    private $run;
    private $runStatus;

    /**
     * @var Queue
     */
    private $queue;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var JobExecutorInterface
     */
    private $executor;

    /**
     * Worker constructor.
     *
     * @param Queue                $queue
     * @param JobExecutorInterface $executor
     * @param int                  $workerId
     */
    public function __construct(
        Queue $queue,
        JobExecutorInterface $executor,
        $workerId
    ) {
        $this->queue = $queue;
        $this->executor = $executor;
        $this->workerId = $workerId;

        $this->instanceHash = md5(uniqid(rand(), true));
        $this->run = false;
    }

    /**
     * @param LoggerInterface $logger
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function run()
    {
        $this->run = true;

        while ($this->run) {
            $this->heartbeat('idle');
            $job = $this->resolveJob();

            if (!$job) {
                continue;
            }

            $this->heartbeat(sprintf('processing %s', $job->getName()));

            $success = false;
            try {
                $success = $this->runJob($job);
            } catch (\Exception $e) {
                $this->log(LogLevel::ALERT, 'error', ['exception' => $e]);
                $this->run = false;
            }

            if ($success) {
                $this->removeJob($job);
            } else {
                $this->buryJob($job);
            }
        }
    }


    public function runMax($maxToExcecute = 5)
    {
        $this->run = true;

        while ($this->run) {
            ++$this->excecuted;
            $this->run = ($this->excecuted < $maxToExcecute);
            $this->heartbeat('idle');
            $job = $this->resolveJob();

            if (!$job) {
                $this->run = false;
                continue;
            }

            $this->heartbeat(sprintf('processing %s', $job->getName()));

            $success = false;
            try {
                $success = $this->runJob($job);
                // \Gb\Util::pre($success, 'runMax $success');
            } catch (\Exception $e) {
                $this->log(LogLevel::ALERT, 'error', ['exception' => $e]);
                $this->run = false;
            }

            if ($success) {
                $this->removeJob($job);
            } else {
                $this->buryJob($job);
            }
        }
    }

    public function runOne()
    {
        $this->log(LogLevel::DEBUG, 'runOne in');
        $this->currentJob = $this->resolveJob();
        // \Gb\Util::pre($this->currentJob, '$this->currentJob runOne');

        if (!$this->currentJob) {
            $this->log(LogLevel::DEBUG, 'runOne empty job');
            return;
        }

        $this->heartbeat(sprintf('processing %s', $this->currentJob->getName(), $this->currentJob->getData()));

        $this->runStatus = false;
        try {
            $this->runStatus = $this->runJob($this->currentJob);
        } catch (\Exception $e) {
            $this->log(LogLevel::ALERT, 'error', ['exception' => $e]);
            return;
        }
        return $this->currentJob;
    }

    public function removeOrBuryJob()
    {
        if ($this->runStatus) {
            $this->removeJob($this->currentJob);
        } else {
            $this->buryJob($this->currentJob);
        }
    }

    public function moveJobToNextlevel($nextLevel)
    {
        // \Gb\Util::pre([$nextLevel, $this->currentJob], 'Worker moveJobToNextlevel');
        $this->log(LogLevel::DEBUG, 'moveJobToNextlevel ' . $nextLevel, $this->currentJob->getData());
        // $this->log(LogLevel::DEBUG, 'moveJobToNextlevel ' . $nextLevel, serialize($this->currentJob));
        $this->currentJob->updateName($nextLevel);
        $this->currentJob->setStatus(Job::STATUS_NEW);
        // \Gb\Util::pre([$this->queue, $this->currentJob, $nextLevel], 'Worker moveJobToNextlevel');
        $this->queue->updateJob($this->currentJob);
    }

    public function reset()
    {
        $this->excecuted = 0;
    }

    /**
     * @return JobInterface|null
     */
    protected function resolveJob()
    {
        $this->log(LogLevel::DEBUG, 'resolveJob ' . $this->queue->getName());
        return $this->queue->resolveJob();
    }

    /**
     * @param JobInterface $job
     *
     * @return bool
     */
    protected function runJob(JobInterface $job)
    {
        $this->log(LogLevel::DEBUG, 'Job starting ' . $job->getName(), $job->getData());
        $status = $this->executor->execute($job);
        $this->log(LogLevel::DEBUG, 'Job finished');

        return $status;
    }

    /**
     * @param JobInterface $job
     */
    protected function removeJob(JobInterface $job)
    {
        // \Gb\Util::pre([$job, $job->getId()], 'Job finished, Deleting');
        $this->log(LogLevel::DEBUG, 'Job deleting ' . $job->getId(), $job->getData());
        $this->queue->removeJob($job);
    }

    /**
     * @param JobInterface $job
     */
    protected function buryJob(JobInterface $job)
    {
        $this->log(LogLevel::WARNING, 'Job failed, Burying');
        $this->queue->buryJob($job);
    }

    protected function heartbeat($message)
    {
        $this->log(LogLevel::DEBUG, $message);
    }

    protected function log($level, $message, $context = [])
    {
        // \Gb\Util::pre([$level, $message, $context], 'worker log');
        if (!$this->logger) {
            return;
        }

        $message = sprintf('[%s] %s', $this->workerId, $message);

        $this->logger->log($level, $message, $context);
    }
}
