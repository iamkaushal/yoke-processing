<?php

namespace App\Command;

use App\Preprocessor\CSVSplitter;
use App\Preprocessor\X12Splitter;
use App\Utils\MapperLogger;

use App\Entity\Relationship;
use App\Entity\Execution;
use App\Entity\Mapper;
use App\Entity\Dom;
use App\Entity\MapExecution;
use App\Entity\Partners;
use App\Entity\Scheduler;
use App\Utils\CustomStatusCodes;
use DateTime;
use DateTimeZone;
use Symfony\Component\HttpClient\HttpClient;

use Doctrine\ORM\EntityManagerInterface;
use Error;
use Exception;
use Generator;
use Greicodex\Parser\EDIParser;
use Greicodex\Parser\Filetypes\AscX12\AscX12FA;
use Greicodex\Parser\Filetypes\AscX12\Ascx12Writer;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;

use Symfony\Component\Mime\Part\Multipart\FormDataPart;

use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

use Psr\Log\LoggerInterface;
use Symfony\Component\HttpFoundation\Session\Session;

class ParserAndMapperCommand extends ContainerAwareCommand
{
    // New configurables for scalability
    protected $maxConcurrentProcesses = 5;
    protected $batchSize = 20;
    protected $socketTimeout = 1800;
    protected $maxRetries = 3;
    protected $gcFrequency = 10; // Run GC after every X files
    
    // Progress tracking variables
    protected $totalFilesCount = 0;
    protected $processedFilesCount = 0;
    protected $startTime = 0;
    
    // ... [existing code] ...

    protected function configure()
    {
        $this->setName("app:parser-mapper-file")
            ->setDescription('This command allows you to parser and mapper file')
            ->setHelp("Example: bin/console app:parser-mapper-file")
            ->addArgument('relationship_id', InputArgument::OPTIONAL, 'What is the relationship id?')
            ->addArgument('map_execution_id', InputArgument::OPTIONAL, 'What is the map-execution id?')
            // New configuration options for scalability
            ->addOption('process-limit', null, InputOption::VALUE_OPTIONAL, 'Maximum number of concurrent processes', 5)
            ->addOption('batch-size', null, InputOption::VALUE_OPTIONAL, 'Number of files to process in a batch', 20)
            ->addOption('socket-timeout', null, InputOption::VALUE_OPTIONAL, 'Socket timeout in seconds', 1800)
            ->addOption('max-retries', null, InputOption::VALUE_OPTIONAL, 'Maximum number of retries for failed operations', 3);
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        set_time_limit(0);
        $this->output = $output;
        $this->session->start();

        // Apply configurations from input options
        $this->maxConcurrentProcesses = (int)$input->getOption('process-limit');
        $this->batchSize = (int)$input->getOption('batch-size');
        $this->socketTimeout = (int)$input->getOption('socket-timeout');
        $this->maxRetries = (int)$input->getOption('max-retries');
        
        // Set the socket timeout for all operations
        ini_set('default_socket_timeout', (string)$this->socketTimeout);

        $input_relationship = $input->getArgument('relationship_id');
        $map_excution_id = $input->getArgument('map_execution_id');

        try {
            $this->output->writeln('<fg=white;bg=cyan>******Parsing And Mapping Files******</>');
            $this->output->writeln("");
            $this->output->writeln("<info>Configuration:</info>");
            $this->output->writeln(" - Max concurrent processes: {$this->maxConcurrentProcesses}");
            $this->output->writeln(" - Batch size: {$this->batchSize}");
            $this->output->writeln(" - Socket timeout: {$this->socketTimeout} seconds");
            $this->output->writeln(" - Max retries: {$this->maxRetries}");
            $this->output->writeln("");

            $this->user  = $this->getContainer()->getParameter('app.user_command');
            $this->pass  = $this->getContainer()->getParameter('app.pass_command');

            $this->base_url  = $this->getContainer()->getParameter('app.host');
            $this->tmp_path_folder = $this->getContainer()->getParameter('app.tmp_path_folder');

            $relationships = null;
            if (!empty($input_relationship)) {
                $relationships = $this->em->getRepository(Relationship::class)->findBy(['id' => $input_relationship]);
                $this->manual_execution = true;
                if (!empty($map_excution_id)) {
                    $this->map_excution = $this->em->getRepository(MapExecution::class)->findOneBy(['id' => $map_excution_id]);
                }
            } else {
                $relationships = $this->em->getRepository(Relationship::class)->findBy(['active' => 1]);
            }

            $processCount = 0;
            $status       = null;
            $taskList     = $this->generator($relationships);

            // Modified process management logic
            do {
                if ($processCount >= $this->maxConcurrentProcesses) {
                    $this->log->info("Waiting, currently running: $processCount (max: {$this->maxConcurrentProcesses})");
                    pcntl_wait($status);
                    $processCount--;
                    continue;
                }

                $this->log->info("Tasks running: $processCount of {$this->maxConcurrentProcesses}");

                $task = $taskList->current();
                $taskList->next();
                $processCount++;

                $pid = pcntl_fork();

                if ($pid === -1) {
                    $this->log->error('Error forking... ');
                    throw new \Exception('Error forking process');
                }

                if ($pid === 0) {
                    $processList[] = getmypid();
                    $this->log->info('Running task as pid: ' . getmypid());
                    
                    // Reset memory stats for child process
                    $this->logMemoryUsage("Initial state of child process");
                    
                    $this->executeTask($task, getmypid());
                    
                    $this->logMemoryUsage("Final state of child process");
                    exit();
                }
            } while ($taskList->valid());

            $waitForChildrenToFinish = true;
            while ($waitForChildrenToFinish) {
                if (pcntl_waitpid(0, $status) === -1) {
                    $this->log->info('parallel execution is complete');
                    $this->output->writeln('<info>Parallel execution is complete</info>');
                    $waitForChildrenToFinish = false;
                }
            }

            $this->output->writeln('<fg=white;bg=green>******Parsed and mapped file******</>');
            return 0; // SUCCESS

        } catch (\Exception $e) {
            $this->log->error("Error in execute: " . $e->getMessage());
            $this->output->writeln("<fg=white;bg=red>" . $e->getMessage() . "</>");
            return 1; // FAILURE
        }
    }
    
    // New helper method for memory management
    private function logMemoryUsage($context = "")
    {
        $memUsage = memory_get_usage(true) / 1024 / 1024;
        $memPeakUsage = memory_get_peak_usage(true) / 1024 / 1024;
        
        $this->log->info(
            sprintf("%s - Memory usage: %.2f MB (peak: %.2f MB)", 
                $context, $memUsage, $memPeakUsage)
        );
        
        // Force garbage collection if enabled
        if (gc_enabled()) {
            $collected = gc_collect_cycles();
            $this->log->debug("Garbage collection ran, {$collected} cycles collected");
        }
    }

    // Modified to batch relationships
    private function generator($relationships): Generator
    {
        if ($this->batchSize <= 1) {
            // Original behavior - yield single relationships
            foreach ($relationships as $relationship) {
                yield $relationship;
            }
        } else {
            // Batch behavior - yield arrays of relationships
            $batch = [];
            foreach ($relationships as $relationship) {
                $batch[] = $relationship;
                
                if (count($batch) >= $this->batchSize) {
                    yield $batch;
                    $batch = [];
                }
            }
            
            // Yield any remaining relationships
            if (!empty($batch)) {
                yield $batch;
            }
        }
    }

    // Improved database connection for each process
    private function createDBConnectionForEachProcess()
    {
        try {
            $doctrine = $this->getContainer()->get('doctrine');
            $conn = $doctrine->getConnection();
            
            // Close existing connection
            if ($conn->isConnected()) {
                $conn->close();
            }
            
            // Configure connection for higher concurrency
            $params = $conn->getParams();
            
            // Adjust connection timeout and other PDO settings for better performance
            if (isset($params['driverOptions'])) {
                $params['driverOptions'][\PDO::ATTR_TIMEOUT] = 300; // 5 minutes
                $params['driverOptions'][\PDO::ATTR_PERSISTENT] = false; // Don't use persistent connections
            } else {
                $params['driverOptions'] = [
                    \PDO::ATTR_TIMEOUT => 300,
                    \PDO::ATTR_PERSISTENT => false
                ];
            }
            
            // Reconnect with optimized parameters
            $conn->__construct(
                $params,
                $conn->getDriver(),
                $conn->getConfiguration(),
                $conn->getEventManager()
            );
            
            $conn->connect();
            
            // Test connection with simple query
            $conn->executeQuery('SELECT 1');
            $this->log->info("Database connection successful for process " . getmypid());
        } catch (\Exception $e) {
            $this->log->error("Database connection failed: " . $e->getMessage());
            throw $e;
        }
    }

    // Execution task modified to handle batches if needed
    private function executeTask($task, $pid)
    {
        // Apply database connection for each process
        $this->createDBConnectionForEachProcess();
        
        date_default_timezone_set("America/New_York");
        $time_start = microtime(true);
        
        if (is_array($task)) {
            // Handling batch mode
            $relationshipCount = count($task);
            $this->log->info("Processing batch of {$relationshipCount} relationships in process {$pid}");
            
            foreach ($task as $index => $relationship) {
                $this->log->info("Processing relationship {$index+1}/{$relationshipCount} (ID: {$relationship->getId()}) in process {$pid}");
                $this->processRelationship($relationship, $pid);
                
                // Apply memory management periodically
                if (($index + 1) % $this->gcFrequency === 0) {
                    $this->logMemoryUsage("After processing {$index+1} relationships in batch");
                }
            }
        } else {
            // Single relationship mode (original behavior)
            $this->processRelationship($task, $pid);
        }
        
        $time_end = microtime(true);
        $execution_time = ($time_end - $time_start) / 60;
        
        $this->log->info("Completed process: {$pid}. Took " . sprintf('%.2f', $execution_time) . " minutes");
    }
    
    // Split out relationship processing logic from executeTask
    private function processRelationship($relationship, $pid)
    {
        try {
            $this->relationship = $relationship;
            
            $timestamp = strtotime(date('Y/m/d H:i:s'));
            $current_year = date('Y', $timestamp);
            $current_month = date('m', $timestamp);
            $current_day_number = date('d', $timestamp);
            
            $this->day_current_folder = $current_year . "/" . $current_month . "/" . $current_day_number;
            
            $this->output->writeln('<fg=white;bg=green>Checking Relationship: ' . $this->relationship->getId() . '</>');
            
            $scheduler = json_decode($this->relationship->getScheduler(), true);
            $connection = json_decode($this->relationship->getConnection(), true);
            
            $this->prepareFilesystem();
            
            if ($this->manual_execution) {
                $this->runProcess($connection);
            } else {
                // Scheduler logic (unchanged)
                $schObj = new Scheduler($scheduler['frequency']);
                // ... existing scheduler code ...
                if ($schObj->isDueNowForExecution(new DateTime(), $last_execution)) {
                    $this->runProcess($connection);
                }
            }
            
            $this->output->writeln("");
            // Output for execution ID used in single relationship mode
            $this->output->writeln('<fg=white;bg=green>execution_id: ' . $this->session->get('execution') . '</>');
            
        } catch (\Exception $e) {
            $this->log->error("Error processing relationship {$relationship->getId()}: " . $e->getMessage());
            throw $e;
        }
    }
    
    // New method to prepare filesystem
    private function prepareFilesystem()
    {
        $this->inbound = $this->tmp_path_folder . "/" . $this->day_current_folder . "/" . $this->relationship->getId() . "/inbound";
        $this->outbound = $this->tmp_path_folder . "/" . $this->day_current_folder . "/" . $this->relationship->getId() . "/outbound";
        $this->processed = $this->tmp_path_folder . "/" . $this->day_current_folder . "/" . $this->relationship->getId() . "/processed";
        $this->errors = $this->tmp_path_folder . "/" . $this->day_current_folder . "/" . $this->relationship->getId() . "/errors";
        
        $dirs = [$this->inbound, $this->outbound, $this->processed, $this->errors];
        
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
    
    // Fetch with retry logic
    private function fetchWithRetry($connection, $action = "download", $is_ack = false)
    {
        $retryCount = 0;
        $lastException = null;
        
        while ($retryCount < $this->maxRetries) {
            try {
                $result = $this->fetch($connection, $action, $is_ack);
                
                // If successful, return the result
                if ($result['status'] == 200) {
                    if ($retryCount > 0) {
                        $this->log->info("Succeeded after {$retryCount} retries");
                    }
                    return $result;
                }
                
                // If we got an error response, log it
                $this->log->warning("Error response (attempt " . ($retryCount+1) . "/{$this->maxRetries}): " . $result['message']);
            } catch (\Exception $e) {
                $lastException = $e;
                $this->log->warning("Exception in fetch (attempt " . ($retryCount+1) . "/{$this->maxRetries}): " . $e->getMessage());
            }
            
            // Exponential backoff with jitter
            $sleepTime = pow(2, $retryCount) + rand(1, 1000) / 1000;
            $this->log->info("Retrying in {$sleepTime} seconds...");
            sleep((int)$sleepTime);
            
            $retryCount++;
        }
        
        // If we've exhausted all retries, return an error
        if ($lastException) {
            return ['message' => "Failed after {$this->maxRetries} retries: " . $lastException->getMessage(), 'status' => 500];
        } else {
            return ['message' => "Failed after {$this->maxRetries} retries", 'status' => 500];
        }
    }
    
    // Modified runProcess to use retry logic
    private function runProcess($connection)
    {
        $this->output->writeln('<fg=white;bg=blue>Executing Relationship: ' . $this->relationship->getId() . '</>');
        
        // Use fetch with retry for download
        $resp = $this->fetchWithRetry($connection, "download");
        
        if ($resp['status'] == 200 && $resp['message'] == "success_download") {
            // Existing code for processing files
            $this->acknowledge_files = [];
            $map = $this->relationship->getMapperId();
            $is_passthru = ($map == null || $map->getId() == 0);
            
            // Initialize progress tracking
            $fileCount = count($resp['files']);
            $this->initializeProgressTracking($fileCount);
            
            // Process files with progress updates
            // ... existing file processing code ...
            
            // Upload acknowledgments with retry if needed
            if (count($this->acknowledge_files) > 0) {
                $this->log->debug("Uploading FA-Ack/Results");
                $respUpload = $this->fetchWithRetry($connection, "upload", true);
            }
            
            // ... rest of the processing logic ...
            
            // Upload results with retry if needed
            if (count($this->outbound_files) > 0) {
                $this->log->debug("Uploading Files/Results");
                $respUpload = $this->fetchWithRetry($connection, "upload");
                $this->output->writeln('<fg=white;bg=green>' . $respUpload['message'] . '</>');
            }
        } else {
            // Existing error handling
        }
    }
    
    // Progress tracking methods
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
        
        // Only output every 5% or for small file counts, every file
        if ($this->totalFilesCount <= 20 || 
            $this->processedFilesCount % max(1, intval($this->totalFilesCount * 0.05)) === 0 ||
            $this->processedFilesCount === $this->totalFilesCount) {
            
            $percentage = ($this->processedFilesCount / $this->totalFilesCount) * 100;
            
            // Calculate ETA
            $elapsedTime = microtime(true) - $this->startTime;
            $filesPerSecond = $this->processedFilesCount / max(1, $elapsedTime); // Avoid division by zero
            $remainingFiles = $this->totalFilesCount - $this->processedFilesCount;
            $eta = $filesPerSecond > 0 ? $remainingFiles / $filesPerSecond : 0;
            
            $etaFormatted = gmdate("H:i:s", (int)$eta);
            
            $this->output->writeln(
                "<info>Progress: {$this->processedFilesCount}/{$this->totalFilesCount} " .
                "(" . number_format($percentage, 2) . "%) - ETA: {$etaFormatted}</info>"
            );
        }
    }
    
    // Other methods remain unchanged
    // ...
}
