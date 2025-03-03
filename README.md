# Scaling File Processing Beyond 100 Files

## Current Limitations and Recommended Solutions

### 1. Process Management Optimization

**Current Implementation:**
```php
if ($processCount >= 5) {
    $this->log->info("Waiting, currently running: $processCount");
    pcntl_wait($status);
    $processCount--;
    continue;
}
```

**Recommended Changes:**
```php
// Make process limit configurable
protected $maxConcurrentProcesses = 5;

// In configure() method, add:
->addOption('process-limit', null, InputArgument::OPTIONAL, 'Maximum number of concurrent processes', 5);

// In execute() method:
$this->maxConcurrentProcesses = (int)$input->getOption('process-limit');

// Then update the process management logic:
if ($processCount >= $this->maxConcurrentProcesses) {
    $this->log->info("Waiting, currently running: $processCount");
    pcntl_wait($status);
    $processCount--;
    continue;
}
```

### 2. Memory Management

**Current Issue:**
Files are loaded entirely into memory without proper garbage collection.

**Recommended Changes:**
```php
// Add periodic garbage collection
private function applyMemoryManagement()
{
    // Force garbage collection
    if (gc_enabled()) {
        gc_collect_cycles();
    }
    
    // Log memory usage
    $memUsage = memory_get_usage(true) / 1024 / 1024;
    $this->log->info("Memory usage: {$memUsage} MB");
}

// Call this function periodically during processing
// e.g., after processing each file:
$this->applyMemoryManagement();
```

### 3. Database Connection Management

**Current Implementation:**
```php
private function createDBConnectionForEachProcess()
{
    $doctrine = $this->getContainer()->get('doctrine');
    $conn = $doctrine->getConnection();
    $conn->close();
    $conn->connect();
}
```

**Recommended Changes:**
```php
private function createDBConnectionForEachProcess()
{
    $doctrine = $this->getContainer()->get('doctrine');
    $conn = $doctrine->getConnection();
    
    // Close existing connection
    if ($conn->isConnected()) {
        $conn->close();
    }
    
    // Configure connection for higher concurrency
    $params = $conn->getParams();
    
    // Adjust connection timeout
    if (isset($params['driverOptions'])) {
        $params['driverOptions'][\PDO::ATTR_TIMEOUT] = 300; // 5 minutes
    } else {
        $params['driverOptions'] = [\PDO::ATTR_TIMEOUT => 300];
    }
    
    // Reconnect with optimized parameters
    $conn->__construct(
        $params,
        $conn->getDriver(),
        $conn->getConfiguration(),
        $conn->getEventManager()
    );
    
    $conn->connect();
    
    // Test connection
    try {
        $conn->executeQuery('SELECT 1');
        $this->log->info("Database connection successful for process " . getmypid());
    } catch (\Exception $e) {
        $this->log->error("Database connection failed: " . $e->getMessage());
        throw $e;
    }
}
```

### 4. File Processing in Batches

**Current Issue:**
Files are processed one by one without any batching.

**Recommended Implementation:**
```php
// Add a new configuration option for batch size
->addOption('batch-size', null, InputArgument::OPTIONAL, 'Number of files to process in a batch', 20);

// Modify the generator method to yield batches of relationships
private function generator($relationships): Generator
{
    $batch = [];
    $batchSize = (int)$this->batchSize;
    
    foreach ($relationships as $relationship) {
        $batch[] = $relationship;
        
        if (count($batch) >= $batchSize) {
            yield $batch;
            $batch = [];
        }
    }
    
    // Yield any remaining relationships
    if (!empty($batch)) {
        yield $batch;
    }
}

// Then modify the executeTask method to handle batches
private function executeTask($relationshipBatch, $pid)
{
    foreach ($relationshipBatch as $relationship) {
        // Process each relationship
        $this->processRelationship($relationship);
        
        // Apply memory management
        $this->applyMemoryManagement();
    }
}
```

### 5. Extended Timeouts for Network Operations

**Current Implementation:**
```php
ini_set('default_socket_timeout', '600');
```

**Recommended Changes:**
```php
// In execute() method
// Add a configuration option for timeout
->addOption('socket-timeout', null, InputArgument::OPTIONAL, 'Socket timeout in seconds', 1800);

// Then in fetch() method:
$timeout = (int)$input->getOption('socket-timeout');
ini_set('default_socket_timeout', (string)$timeout);

// Add connection timeout to HTTP client
$clientHttp = HttpClient::create([
    'auth_basic' => [$this->user, $this->pass],
    'timeout' => $timeout,
    'max_duration' => $timeout + 60
]);
```

### 6. Retry Mechanism for Failed Operations

**Recommended Implementation:**
```php
private function fetchWithRetry($connection, $action = "download", $is_ack = false, $maxRetries = 3)
{
    $retryCount = 0;
    $lastException = null;
    
    while ($retryCount < $maxRetries) {
        try {
            $result = $this->fetch($connection, $action, $is_ack);
            
            // If successful, return the result
            if ($result['status'] == 200) {
                if ($retryCount > 0) {
                    $this->log->info("Succeeded after {$retryCount} retries");
                }
                return $result;
            }
            
            // If we got an error response, wait and retry
            $this->log->warning("Error response (attempt {$retryCount+1}/{$maxRetries}): " . $result['message']);
        } catch (\Exception $e) {
            $lastException = $e;
            $this->log->warning("Exception in fetch (attempt {$retryCount+1}/{$maxRetries}): " . $e->getMessage());
        }
        
        // Exponential backoff with jitter
        $sleepTime = pow(2, $retryCount) + rand(1, 1000) / 1000;
        $this->log->info("Retrying in {$sleepTime} seconds...");
        sleep((int)$sleepTime);
        
        $retryCount++;
    }
    
    // If we've exhausted all retries, return an error
    if ($lastException) {
        return ['message' => "Failed after {$maxRetries} retries: " . $lastException->getMessage(), 'status' => 500];
    } else {
        return ['message' => "Failed after {$maxRetries} retries", 'status' => 500];
    }
}
```

### 7. Progress Tracking and Reporting

**Recommended Implementation:**
```php
// Add a progress tracking mechanism
private function initializeProgressTracking($totalFiles)
{
    $this->totalFilesCount = $totalFiles;
    $this->processedFilesCount = 0;
    $this->startTime = microtime(true);
    
    $this->output->writeln("<info>Starting to process {$totalFiles} files</info>");
}

private function updateProgress()
{
    $this->processedFilesCount++;
    $percentage = ($this->processedFilesCount / $this->totalFilesCount) * 100;
    
    // Calculate ETA
    $elapsedTime = microtime(true) - $this->startTime;
    $filesPerSecond = $this->processedFilesCount / $elapsedTime;
    $remainingFiles = $this->totalFilesCount - $this->processedFilesCount;
    $eta = $filesPerSecond > 0 ? $remainingFiles / $filesPerSecond : 0;
    
    $etaFormatted = gmdate("H:i:s", (int)$eta);
    
    $this->output->writeln(
        "<info>Progress: {$this->processedFilesCount}/{$this->totalFilesCount} " .
        "(" . number_format($percentage, 2) . "%) - ETA: {$etaFormatted}</info>"
    );
}
```

### 8. File System Management

**Recommended Implementation:**
```php
// Pre-create all necessary directories at the start
private function prepareFilesystem()
{
    $dirs = [
        $this->inbound,
        $this->outbound,
        $this->processed,
        $this->tmp_path_folder . "/" . $this->day_current_folder . "/" . $this->relationship->getId() . "/errors"
    ];
    
    foreach ($dirs as $dir) {
        if (!is_dir($dir)) {
            $this->log->info("Creating directory: {$dir}");
            if (!mkdir($dir, 0777, true)) {
                throw new \Exception("Failed to create directory: {$dir}");
            }
        }
    }
    
    // Check for adequate disk space
    $freeSpace = disk_free_space(dirname($this->tmp_path_folder));
    $requiredSpace = 1024 * 1024 * 1024; // 1GB minimum
    
    if ($freeSpace < $requiredSpace) {
        $this->log->warning("Low disk space warning: " . 
            number_format($freeSpace / 1024 / 1024, 2) . "MB available");
    }
}
```

## Implementation Plan

1. **Phase 1: Configuration and Monitoring**
   - Add configurable process limits and batch sizes
   - Implement memory management and monitoring
   - Add detailed logging of resource usage

2. **Phase 2: Connection and Resource Management**
   - Improve database connection handling
   - Implement proper resource cleanup
   - Add timeout management

3. **Phase 3: Scalability Improvements**
   - Implement batching mechanism
   - Add retry logic
   - Optimize filesystem operations

4. **Phase 4: Testing and Tuning**
   - Test with varying file counts (100, 500, 1000)
   - Identify optimal settings for different environments
   - Create performance benchmarks
