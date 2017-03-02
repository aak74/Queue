<?php

namespace Queue\Job;

use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;

class Job implements JobInterface
{
    use \Queue\Logger\LoggerTrait;

    const MAX_TRIES = 5;

    const STATUS_NEW = 0;
    const STATUS_TRY_LATER = 2;
    const STATUS_MOVED_TO_NEXT = 4;
    const STATUS_DONE = 9;
    const STATUS_BURIED = -1;
    const STATUS_ERROR = -5;

    // public $addToQueue;
    protected $nextJobClassName;
    protected $name;
    protected $alias;
    protected $type;
    protected $weight = 500;
    private $status;
    private $result;
    private $tries = 0;
    private $payload = [];

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
        $this->payload = $data;
        $this->status = self::STATUS_NEW;
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
        return (array)$this->payload;
    }

    public function getParam($param)
    {
        return $this->payload[$param];
    }

    public function setDataAll($data)
    {
        $this->jobId = $data['id'];
        $this->name = $data['name'];
        $this->status = $data['status'];
        $this->result = $data['result'];
        $this->tries = $data['tries'];
        $this->type = $data['type'];
        // $this->payload = $data['payload'];
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
        if ($this->nextJobClassName == null) {
            return false;
        }
        $this->log(LogLevel::DEBUG, 'tryRun', $this->getData());
        $this->addToQueue = false;
        $nextJob = new $this->nextJobClassName($this->nextJobClassName, $this->getData());
        if ($nextJob->execute()) {
            $this->addToQueue[] = $nextJob;
        }
        return !empty($this->addToQueue);
    }

    protected function execute()
    {

    }

    public function getDefaultParams()
    {
        return [
            'name' => $this->getName(),
            'type' => $this->type,
            'tries' => $this->tries,
            'weight' => $this->weight,
            'status' => $this->getStatus(),
            'payload' => $this->getSerialized(),
            'worker' =>  get_class($this),
            'result' => serialize($this->getResult()),
            'hash' => $this->getHash(),
        ];
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
}
