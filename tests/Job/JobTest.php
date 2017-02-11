<?php

namespace Queue\Job;

class JobTest extends \PHPUnit_Framework_TestCase
{
    public function testProperties()
    {
        $name = 'name';
        $data = ['data'];

        $job = new Job($name, $data);

        $this->assertEquals($name, $job->getName());
        $this->assertEquals($data, $job->getData());
    }
}
