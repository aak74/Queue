# Queue

[![Build Status](https://api.travis-ci.org/aak74/queue.svg?branch=master)](https://travis-ci.org/aak74/queue)
[![Latest Stable Version](https://poser.pugx.org/aak74/queue/v/stable)](https://packagist.org/packages/aak74/queue)
[![Latest Unstable Version](https://poser.pugx.org/aak74/queue/v/unstable)](https://packagist.org/packages/aak74/queue)
[![License](https://poser.pugx.org/aak74/queue/license)](https://packagist.org/packages/aak74/queue)



This library is a Queueing System abstraction layer. It enables you to implement any queueing system in your application,
without having to depend on the actual implementation of the queueing system you choose.

**Attention**: You will need a Process Control System like [supervisord](http://supervisord.org/) to keep your workers going.

## Use Case

### Queueing system

Say you want to notify multiple users when a new comment is placed on a forum thread. Sending these email on the spot might
slow down your application significantly when many emails need to be send. When using a Queueing System you can delay this
action by adding the Jobs to a Queue. A worker will pick up these Jobs asynchronously from you web process. This way your
application is future proof and you will have a much easier time scaling in the future.

### Library

When you decide to use this library, you do not depend on the queueing system implementation. You can plug the driver of your
choice. You might choose a basic mysql version to get started and when you need more performance go for something like
[beanstalkd](http://kr.github.io/beanstalkd/) or [RabbitMQ](https://www.rabbitmq.com/).

## Code examples

### Add job to Queue

```php
<?php

require_once(__DIR__ . '/vendor/autoload.php');

use Akop\Queue;
use Akop\Job\Job;
use Akop\Driver\PDO as Driver; // Use your driver.
use Akop\Executor\JobExecutor;

$driver = new Driver();
$queue = new Queue($driver);

// Add a job to the queue
$queue->addJob(new Job('notify_forum_thead', ['threadId' => 12]));
```

### Run job from Queue

```php
<?php

use Akop\Queue;
use Akop\Job\Job;
use Akop\Driver\PDO as Driver; // Use your driver.
use Akop\Executor\JobExecutor;

$driver = new Driver();
$queue = new Queue($driver);

$worker = new Worker($queue, new JobExecutor(), 1);
$queue->run();

```


## Installation

Run this command to get the latest version from [packagist](packagist.org):

```bash
$ composer require aak74/queue
```

## Requirements

PHP 5.5 or above

## Author

Originally aiuthor Dries De Peuter - <dries@nousefreak.be> - <http://nousefreak.be>

[Original repository](https://github.com/NoUseFreak/Queue)

## License

Queue is licensed under the MIT license.
