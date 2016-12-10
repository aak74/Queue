<?php

namespace Queue\Job;

class JobTypes
{
    protected $types = [
      'Top' => [
            'name' => 'Top',
            'executor' => '\App\Queue\Executor\Parser\#NAMESPACE#\Top',
            'name_ru' => 'Парсинг разделов верхнего уровня',
            'level' => 0,
            'nextQueue' => '\App\Queue\Executor\Parser\#NAMESPACE#\Category',
            'updater' => '\App\Queue\Executor\Updater\Top'
        ],
        'Category' => [
            'name' => 'Category',
            'executor' => '\App\Queue\Executor\Parser\#NAMESPACE#\Category',
            'try' => 'ProductList',
            'name_ru' => 'Парсинг разделов',
            'level' => 1,
            'nextQueue' => '\App\Queue\Executor\Parser\#NAMESPACE#\Category',
            'updater' => '\App\Queue\Executor\Updater\Category'
        ],
    ];
    public function get()
    {
        return $this->types;
    }

    public function getByName($name)
    {
        return $this->types[$name];
    }

    public function getPropByName($name, $propertyName, $namespace = '')
    {
        $type = $this->getByName($name);
        return ( empty($namespace)
            ? $type[$propertyName]
            : str_replace('#NAMESPACE#', $namespace, $type[$propertyName])
        );
    }

    public function getRenamed(array $input, $namespace, $keyName = 'name')
    {
        // \Gb\Util::pre($input, 'getRenamed input');
        foreach ($input as $key => $value) {
            $type = $this->getByName((empty($namespace)
                ? $key
                : substr($key, strlen($namespace) + 1)
            ));
            $jobs[$type[$keyName]] = $value;
        }

        return $jobs;
    }

    public function getRenamedAndSorted(array $input, $namespace, $keyName = 'name')
    {
        // \Gb\Util::pre([$input, $namespace, $keyName], 'getRenamedAndSorted params');
        $types = $this->getRenamed($input, $namespace, $keyName);
        $levels = [];
        foreach ($types as $type) {
            $levels[$type['name']] = $this->getPropByName($type['name'], 'level');
        }
        // \Gb\Util::pre($types, 'types 1');
        array_multisort($levels, SORT_ASC, $types);
        // \Gb\Util::pre([$types, $levels], 'types 2');
        return $types;
    }

    public function getNextLevelTypeName($currentTypeName)
    {
        $types = $this->get();
        foreach ($types as $typeName => $type) {
            $levels[$type['level']] = $typeName;
        }
        $currentTypeLevel = $this->getPropByName($currentTypeName, 'level');
        // \Gb\Util::pre([$currentTypeLevel, $types, $levels], 'getNextLevelTypeName');
        return ( isset($levels[$currentTypeLevel + 1])
            ? $levels[$currentTypeLevel + 1]
            : ''
        );
    }
}
