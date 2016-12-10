<?php

namespace Queue\Job;

use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Queue\Executor\JobExecutorInterface;

class Job implements JobInterface
{
    const STATUS_NEW = 0;
    const STATUS_ERROR = 5;
    const STATUS_DONE = 9;
    const STATUS_BURIED = -1;

    /**
     * @var string
     */
    private $name;
    private $status;

    /**
     * @var array
     */
    private $data;

    private $id;

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

    public function run()
    {
        $result = false;
        $this->log(LogLevel::DEBUG, 'run', $this->getData());
        if ($res = $this->execute($this->getExcecutor())) {
            // if ($nextExecutor = $this->getNextQueue()) {
            //     if ($result = $this->execute($nextExecutor, $res)) {
            //         $result = true;
            //     }
            // } else {
            //     $result = true;
            // }
            $result = $res;
        }
        return $result;
    }

    public function tryRun($executorName)
    {
        $result = false;
        // \Gb\Util::pre($executorName, 'Job tryRun');
        $this->log(LogLevel::DEBUG, 'tryRun', $this->getData());
        $result = $this->execute($executorName);
        // \Gb\Util::pre($result, 'Job tryRun result');
        return $result;
    }

    private function execute($executorName, array $params = [])
    {
        $this->log(LogLevel::DEBUG, 'execute ' . $executorName, $params);
        // \Gb\Util::pre([$executorName, $params], 'Job execute');
        $executor = new $executorName;
        // \Gb\Util::pre($executor, 'Job execute');
        $result = $executor->execute($this, $params);
        // \Gb\Util::pre($result, 'execute $result');
        $this->log(LogLevel::DEBUG, 'execute result', $result);
        return $result;
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
        return $this->getName();
        // return $this->getPropertyByName('executor');
    }

    public function getPropertyByName($propertyName)
    {
        return $this->data[$propertyName];
    }

    /**
     * @return integer
     */
    public function getId()
    {
        return $this->id;
    }

    public function setId($id)
    {
        $this->id = $id;
    }

    public function setStatus($status)
    {
        $this->status = $status;
    }

    public function getStatus()
    {
        return $this->status;
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
