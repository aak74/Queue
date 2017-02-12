<?php

namespace Queue\Driver;

use Queue\Job\Job as Job;
use Queue\Job\JobInterface;

class BitrixHl extends Driver
{
    private $elementObj;

    public function __construct($elementObj)
    {
        $this->elementObj = $elementObj;
    }

    protected function insertJob($job)
    {
        return $this->elementObj->add($job->getDefaultParams());
    }

    /**
     * @param string $queueName
     *
     * @return JobInterface
    */

    public function resolveJob($queueName)
    {
        parent::resolveJob($queueName);
        $data = $this->elementObj->getRow([
            'order' => ['tries', 'id'],
            'filter' => [
                'name' => $queueName,
                'status' => Job::STATUS_NEW,
                // '<tries' => Job::MAX_TRIES,
            ]
        ]);
        // \Akop\Util::pre([$queueName, $data], 'resolveJob');
        return $this->getJobByData($data);
    }

    public function removeJob(JobInterface $job)
    {
        parent::removeJob($job);
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
        parent::moveJobToEnd($job);
        $this->elementObj->update(
            $job->getId(),
            array_merge(
                $job->getDefaultParams(),
                ['tries' => 'tries + 1']
            )
        );
    }

    /**
     * @param string       $queueName
     * @param JobInterface $job
     */
    public function buryJob(JobInterface $job)
    {
        parent::buryJob($job);
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
        parent::updateJob($job);
        $this->elementObj->update(
            $job->getId(),
            $job->getDefaultParams()
        );
    }

    public function getJobs($filter)
    {
        return $this->elementObj->getList(['filter' => $filter]);
    }

    public function getJobById($jobId)
    {
        return $this->elementObj->getList(['filter' => ['id' => $jobId]]);
    }

    public function getNewJobs($queueName)
    {
        return $this->elementObj->getList([
                'filter' => [
                    'status' => Job::STATUS_NEW,
                    'name' => str_replace('\\', '\\\\', $queueName),
                ]
            ]);
    }

    public function getNewJobTypes($type)
    {
        $list = $this->elementObj->getList([
                'filter' => [
                    'status' => Job::STATUS_NEW,
                    'type' => $type,
                ],
                'group' => ['name'],
                'order' => ['weight' => 'desc']
            ]);
        foreach ($list as $type) {
            $result[$type['name']] = $type['cnt'];
        }

        return $result;
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
