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

    public function testGetDefaultParams()
    {
        $job = new Job('name', ['payload']);

        $this->assertEquals(
            [
                'name' => 'name',
                'type' => null,
                'weight' => 500,
                'status' => Job::STATUS_NEW,
                'payload' => 'a:2:{s:4:"name";s:4:"name";s:4:"data";a:1:{i:0;s:7:"payload";}}',
                'worker' => 'Queue\Job\Job',
                'result' => 'N;',
                'hash' => '91b91429a5a7982b2e16748b0be1fbca',
            ],
            $job->getDefaultParams()
        );
    }
}
