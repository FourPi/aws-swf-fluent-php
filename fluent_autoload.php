<?php 
spl_autoload_register(
    function ($full_name) 
    {
        $prefix = 'Fluent\\';
        $len = strlen($prefix);
        if (strncmp($prefix, $full_name, $len) !== 0) {
            // no, move to the next registered autoloader
            return;
        }
        $file_path = __DIR__ . '/src/' . str_replace('\\', '/', $full_name) . '.php';
        if(file_exists($file_path))
        {
            require $file_path;
        }
    }
);