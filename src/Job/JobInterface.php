<?php

namespace Queue\Job;

interface JobInterface
{
    public function getName();
    public function getData();
    public function getId();
    public function setId($jobId);
    public function run();
}
