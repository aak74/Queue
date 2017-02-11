<?php

namespace Queue\Job;

interface JobInterface
{
    /**
     * @return string
     */
    public function getName();

    /**
     * @return array
     */
    public function getData();

    /**
     * @return integer
     */
    public function getId();
    public function setId($jobId);
    public function run();
}
