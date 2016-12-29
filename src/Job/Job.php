<?php

namespace Queue\Job;

use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

class Job implements JobInterface
{
    const MAX_TRIES = 5;

    const STATUS_NEW = 0;
    const STATUS_TRY_LATER = 2;
    const STATUS_MOVED_TO_NEXT = 4;
    const STATUS_DONE = 9;
    const STATUS_BURIED = -1;
    const STATUS_ERROR = -5;

    public $addToQueue;
    protected $nextJobClassName;
    /**
     * @var string
     */
    private $name;
    private $status;
    private $result;
    private $tries = 0;

    /**
     * @var array
     */
    private $data;

    private $jobId;

    /**
     * Job constructor.
     *
     * @param string $name
     * @param array  $data
     */
    public function __construct($name, $data = [])
    {
        // \Gb\Util::pre([$name, $data], 'Job  __construct');
        $this->name = $name;
        $this->data = $data;
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return array
     */
    public function getData()
    {
        return $this->data;
    }

    public function setDataAll($data)
    {
        $this->jobId = $data['jobId'];
        $this->name = $data['name'];
        $this->status = $data['status'];
        $this->result = $data['result'];
        $this->tries = $data['tries'];
        $this->data = $data['data'];
    }

    public function run()
    {
        $this->addToQueue = false;
        $this->log(LogLevel::DEBUG, 'run', $this->getData());

        if ($this->result = $this->execute()) {
            return ($this->status = Job::STATUS_DONE);
        }

        if ($this->result = $this->tryRun()) {
            return ($this->status = Job::STATUS_MOVED_TO_NEXT);
        }
        $this->tries++;
        return Job::STATUS_TRY_LATER;

    }

    protected function tryRun()
    {
        $this->addToQueue = false;
        $this->log(LogLevel::DEBUG, 'tryRun', $this->getData());
        $nextJob = new $this->nextJobClassName($this->nextJobClassName, $this->getData());
        if ($nextJob->execute()) {
            $this->addToQueue[] = $nextJob;
        }
        return !empty($this->addToQueue);
    }

    protected function execute()
    {

    }

    /**
     * @return integer
     */
    public function getId()
    {
        return $this->jobId;
    }

    public function setId($jobId)
    {
        $this->id = $jobId;
    }

    public function setStatus($status)
    {
        $this->status = $status;
    }

    public function getStatus()
    {
        return $this->status;
    }

    public function getResult()
    {
        return $this->result;
    }

    public function updateName($name)
    {
        $this->name = $name;
        $this->data['handler'] = $name;
    }

    public function getHash()
    {
        return md5($this->getSerialized());
    }

    public function getSerialized()
    {
        return serialize(['name' => $this->getName(), 'data' => $this->getData()]);
    }

    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
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
