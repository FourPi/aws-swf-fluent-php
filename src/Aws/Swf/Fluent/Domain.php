<?php

namespace Aws\Swf\Fluent;

use Aws\Swf\Fluent\Enum;

/**
 * Class Domain
 * @package Aws\Swf\Fluent
 */
class Domain {
    /* @var $swfClient \Aws\Swf\SwfClient */
    protected $domainName = null;
    /**
     * @var null
     */
    protected $swfClient = null;
    /**
     * @var array
     */
    protected $workflows = array();
    /**
     * @var int
     */
    protected $workflowExecutionRetentionPeriodInDays = 10;
    /**
     * @var string
     */
    protected $taskList = 'main';

    /**
     * @var bool
     */
    protected $isConfigured = false;
    /**
     * @var bool
     */
    protected $isRegistered = false;

    /**
     * @var null
     */
    protected $cachedActivities = null;

    /**
     * @var string
     */
    protected $deciderIdentity = 'decider';
    /**
     * @var string
     */
    protected $workerIdentity = 'worker';

    /**
     * @var string
     */
    protected $executionStartToCloseTimeout = "1800";
    
    /**
     * @var string
     */
    protected $taskStartToCloseTimeout = "600";
    
    /**
     * @var string
     */
    protected $scheduleToCloseTimeout = "900";
   
    /**
     * @var string
     */
    protected $scheduleToStartTimeout = "300";
    
    /**
     * @var string
     */
    protected $startToCloseTimeout = "600";
   
    /**
     * @var string
     */
    protected $heartbeatTimeout = "120";


    /**
     *
     */
    protected function configure() {

    }

    /**
     *
     */
    public function lazyInitialization($skipRegistration = false) {
        if (!$this->isConfigured) {
            $this->configure();
            $this->isConfigured = true;
        }
        if (!$skipRegistration) {
            $this->register();
        }
    }

    /**
     *
     */
    public function register() {
        if (!$this->isRegistered) {
            try {
                $this->registerDomain();
                $this->registerWorkflowTypes();
                $this->registerActivityTypes();

            }
            catch (\Exception $e) {
                // ignore registration in progress concurrency
            }

            $this->isRegistered = true;
        }
    }

    /**
     *
     */
    protected function registerDomain() {
        $isDomainRegistered = true;
        try {
            $this->getSwfClient()->describeDomain(array('name' => $this->getDomainName()));
        }
        catch (\Exception $e) {
            $isDomainRegistered = false;
        }

        if (!$isDomainRegistered) {
            $this->getSwfClient()->registerDomain(array(
                'name' => $this->getDomainName(),
                'workflowExecutionRetentionPeriodInDays' => $this->getWorkflowExecutionRetentionPeriodInDays()
            ));
        }
    }

    /**
     *
     */
    protected function registerWorkflowTypes() {
        $registeredWorkflowTypesResponse = $this->getSwfClient()->listWorkflowTypes(array(
            'domain' => $this->getDomainName(),
            'registrationStatus' => 'REGISTERED'
        ));

        $registeredWorkflowTypes = array();
        foreach ($registeredWorkflowTypesResponse['typeInfos'] as $workflowType) {
            $workflowName = $workflowType['workflowType']['name'];
            $workflowVersion = $workflowType['workflowType']['version'];
            $registeredWorkflowTypes[$workflowName . $workflowVersion] = 1;
        }

        foreach ($this->getWorkflows() as $workflow) {
            if (!array_key_exists($workflow->getName() . $workflow->getVersion(), $registeredWorkflowTypes)) {
                $this->getSwfClient()->registerWorkflowType(array(
                    'name' => $workflow->getName(),
                    'version' => $workflow->getVersion(),
                    'domain' => $this->getDomainName()));
            }
        }
    }

    /**
     *
     */
    protected function registerActivityTypes() {
        $registeredActivityTypesResponse = $this->getSwfClient()->listActivityTypes(array(
            'domain' => $this->getDomainName(),
            'registrationStatus' => 'REGISTERED'));

        $registeredActivityTypes = array();
        foreach ($registeredActivityTypesResponse['typeInfos'] as $activityType) {
            $activityName = $activityType['activityType']['name'];
            $activityVersion = $activityType['activityType']['version'];
            $registeredActivityTypes[$activityName . $activityVersion] = 1;
        }

        foreach ($this->getCachedAllActivities() as $activity) {
            if (!array_key_exists($activity->getName() . $activity->getVersion(), $registeredActivityTypes)) {
                $this->getSwfClient()->registerActivityType(array(
                    'name' => $activity->getName(),
                    'version' => $activity->getVersion(),
                    'domain' => $this->getDomainName()));
            }
        }
    }

    /**
     * @param int $workflowExecutionRetentionPeriodInDays
     */
    public function setWorkflowExecutionRetentionPeriodInDays($workflowExecutionRetentionPeriodInDays) {
        $this->workflowExecutionRetentionPeriodInDays = $workflowExecutionRetentionPeriodInDays;
    }

    /**
     * @return int
     */
    public function getWorkflowExecutionRetentionPeriodInDays() {
        return $this->workflowExecutionRetentionPeriodInDays;
    }

    /**
     * @return \Aws\Swf\SwfClient|null
     * @throws Exception
     */
    public function getSwfClient() {
        if (is_null($this->swfClient)) {
            throw new \Exception('swf client not set');
        }
        return $this->swfClient;
    }

    /**
     * @param $swfClient
     * @return $this
     */
    public function setSwfClient($swfClient) {
        $this->swfClient = $swfClient;
        return $this;
    }

    /**
     * @param $domainName
     */
    public function setDomainName($domainName) {
        $this->domainName = $domainName;
    }

    /**
     * @return null
     */
    public function getDomainName() {
        return $this->domainName;
    }

    /**
     * @return string
     */
    public function getTaskList() {
        return $this->taskList;
    }

    /**
     * @param $taskList
     */
    public function setTaskList($taskList) {
        $this->taskList = $taskList;
    }

    /**
     * @param $workflowName
     * @param array $options
     * @return Workflow
     */
    public function addWorkflow($workflowName, $version, $options = array()) {
        $workflow = new Workflow($workflowName, $options);
        $this->workflows[$workflowName] = $workflow;
        $workflow->setVersion($version);
        return $workflow;
    }

    /**
     * @param $workflowName
     * @return null
     */
    public function getWorkflow($workflowName) {
        $result = null;
        if (array_key_exists($workflowName, $this->workflows)) {
            $result = $this->workflows[$workflowName];
        }
        return $result;
    }

    /**
     * @return string
     */
    public function getDeciderIdentity() {
        return $this->deciderIdentity;
    }

    /**
     * @return string
     */
    public function setDeciderIdentity($deciderIdentity) {
        $this->deciderIdentity = $deciderIdentity;
    }

    /**
     * @param $executionStartToCloseTimeout
     */
    public function setExecutionStartToCloseTimeout($executionStartToCloseTimeout) {
        $this->executionStartToCloseTimeout = $executionStartToCloseTimeout;
    }

    /**
     * @return string
     */
    public function getExecutionStartToCloseTimeout() {
        return $this->executionStartToCloseTimeout;
    }

    /**
     * @param $taskStartToCloseTimeout
     */
    public function setTaskStartToCloseTimeout($taskStartToCloseTimeout) {
        $this->taskStartToCloseTimeout = $taskStartToCloseTimeout;
    }

    /**
     * @return string
     */
    public function getTaskStartToCloseTimeout() {
        return $this->taskStartToCloseTimeout;
    }

     /**
     * @param $scheduleToCloseTimeout
     */
    public function setScheduleToCloseTimeout($scheduleToCloseTimeout) {
        $this->scheduleToCloseTimeout = $scheduleToCloseTimeout;
    }

    /**
     * @return string
     */
    public function getScheduleToCloseTimeout() {
        return $this->scheduleToCloseTimeout;
    }

    /**
     * @param $scheduleToStartTimeout
     */
    public function setScheduleToStartTimeout($scheduleToStartTimeout) {
        $this->scheduleToStartTimeout = $scheduleToStartTimeout;
    }

    /**
     * @return string
     */
    public function getScheduleToStartTimeout() {
        return $this->scheduleToStartTimeout;
    }

     /**
     * @param $startToCloseTimeout
     */
    public function setStartToCloseTimeout($startToCloseTimeout) {
        $this->startToCloseTimeout = $startToCloseTimeout;
    }

    /**
     * @return string
     */
    public function getStartToCloseTimeout() {
        return $this->startToCloseTimeout;
    }

    /**
     * @param $heartbeatTimeout
     */
    public function setHeartbeatTimeout($heartbeatTimeout) {
        $this->heartbeatTimeout = $heartbeatTimeout;
    }

    /**
     * @return string
     */
    public function getHeartbeatTimeout() {
        return $this->heartbeatTimeout;
    }

    /**
     * @param $workflowName
     * @param null $input
     * @return Model
     */
    public function startWorkflowExecution($workflowName, $input = null, $skipRegistration = false) {

        $this->lazyInitialization($skipRegistration);
        $workflow = $this->getWorkflow($workflowName);
        $result = $this->getSwfClient()->startWorkflowExecution(array(
            "domain" => $this->getDomainName(),
            "workflowId" => microtime(),
            "workflowType" => array(
                "name" => $workflow->getName(),
                "version" => $workflow->getVersion()),
            "taskList" => array("name" => $this->getTaskList()),
            "input" => (string)$input,
            "executionStartToCloseTimeout" => $this->getExecutionStartToCloseTimeout(),
            "taskStartToCloseTimeout" => $this->getTaskStartToCloseTimeout(),
            "childPolicy" => "TERMINATE"));
        return $result;
    }

    public function listEvents($workflowId, $runId)
    {
        try
        {
            $result = $this->getSwfClient()->getWorkflowExecutionHistory([
                'domain' => $this->getDomainName(), // REQUIRED
                'execution' => [ // REQUIRED
                    'runId' => $runId, // REQUIRED
                    'workflowId' => $workflowId, // REQUIRED
                ],
                //'maximumPageSize' => <integer>,
                //'nextPageToken' => '<string>',
                'reverseOrder' => false,
            ]);
            return $result['events'];
        }
        catch (\Exception $err)
        {
            echo $err->getMessage();
        }

        throw new \Exception('Could not list events.');
        
        //return array();

    }

    /**
     *
     */
    public function pollForDecisionTask() {
        $this->lazyInitialization();
        while (true) {

            //echo "#######################################\n";
            //echo "pollForDecisionTask\n";
            //echo "#######################################\n";

            $decisionTaskData = $this->getSwfClient()->pollForDecisionTask(array(
                'domain' => $this->getDomainName(),
                'taskList' => array('name' => $this->getTaskList()),
                'identity' => $this->getDeciderIdentity(),
                'reverseOrder' => true
            ));

            if ($decisionTaskData['taskToken']) {
                $decisions = $this->processDecisionTask($decisionTaskData);
                $this->getSwfClient()->respondDecisionTaskCompleted(array(
                    'taskToken' => $decisionTaskData['taskToken'],
                    'decisions' => $decisions
                ));
            }
        }
    }

    /**
     *
     */
    public function pollForActivityTask() {
        $this->lazyInitialization();
        while (true) {

            //echo "#######################################\n";
            //echo "pollForActivityTask\n";
            //echo "#######################################\n";


            $activityTaskData = $this->getSwfClient()->pollForActivityTask(array(
                'domain' => $this->getDomainName(),
                'taskList' => array('name' => $this->getTaskList()),
                'identity' => $this->getWorkerIdentity()
            ));

            if ($activityTaskData['taskToken']) {
                try {
                    $result = $this->processActivityTask($activityTaskData);

                    $this->getSwfClient()->respondActivityTaskCompleted(array(
                        'taskToken' => $activityTaskData['taskToken'],
                        'result' => (string)$result
                    ));

                   
                }
                catch (\Exception $e) {
                    $this->getSwfClient()->respondActivityTaskFailed(array(
                        'taskToken' => $activityTaskData['taskToken'],
                        'details' => $e->getTraceAsString(),
                        'reason' => $e->getMessage()
                    ));
                }
            }
        }
    }

    /**
     * @param $decisionTaskData
     * @return array
     */
    protected function processActivityTask($activityTaskData) {
        $activityType = $activityTaskData['activityType'];
        $activity = $this->getActivity($activityType['name']);

        $activityContext = new ActivityContext();
        $activityContext->setDomain($this);
        $activityContext->setActivityTaskData($activityTaskData);
        $activityContext->setInput($activityTaskData['input']);

        $methodName = $activity->getName();
        $object = $activity->getOption('object');
        if (is_null($object)) {
            $object = $this;
        }
       
        $result = call_user_func_array(array($object, $methodName), array($activityContext));
                
        return $result;
    }

    /**
     * @param $decisionTaskData
     * @return array
     */
    protected function processDecisionTask($decisionTaskData) {
        $workflowType = $decisionTaskData['workflowType'];
        $workflow = $this->getWorkflow($workflowType['name']);
        $decisionHint = new DecisionHint();

        try {
            $decisionContext = new DecisionContext();
            $decisionContext->setDomain($this);
            $decisionContext->setWorkflow($workflow);
            $decisionContext->loadReversedEventHistory($decisionTaskData['events']);
            $decisionHint = $decisionContext->getDecisionHint();
        }
        catch (\Exception $e) {
            $decisionHint->setDecisionType(Enum\DecisionType::FAIL_WORKFLOW_EXECUTION);
            $decisionHint->setLastException($e);
        }

        return $this->getDecisions($decisionHint);
    }

    /**
     * @param $decisionHint DecisionHint
     * @return array
     */
    protected function getDecisions($decisionHint) {
        $decisionType = $decisionHint->getDecisionType();
        $item = $decisionHint->getItem();
        $lastEvent = $decisionHint->getLastEvent();
        $lastEventResult = $decisionHint->getLastEventResult();
        $decisions = array();

        echo "decisionType: $decisionType\n";
        //var_dump($decisionHint);


        switch ($decisionType) {
            case Workflow::NOOP:
                // no operation.
                break;

            
            case Enum\DecisionType::START_TIMER:

                $decisions[] = array(
                    'decisionType' => Enum\DecisionType::START_TIMER,
                    'startTimerDecisionAttributes' => array(
                        'timerId' => $item->getId(),
                        'control' => $item->getName(), //OPTIONAL DATA,
                        'startToFireTimeout' => $decisionHint->getTimerDuration() //DURATION TO WAIT IN SECONDS
                    )
                );

                break; 

            case Enum\DecisionType::SCHEDULE_ACTIVITY_TASK:
                $decisions[] = array(
                    'decisionType' => Enum\DecisionType::SCHEDULE_ACTIVITY_TASK,
                    'scheduleActivityTaskDecisionAttributes' => array(
                        'control' => $item->getId(),
                        'activityType' => array(
                            'name' => $item->getName(),
                            'version' => $item->getVersion()
                        ),
                        'activityId' => $item->getName() . time(),
                        'input' => (string)$lastEventResult,
                        'scheduleToCloseTimeout' => $this->getScheduleToCloseTimeout(),
                        'taskList' => array('name' => $this->getTaskList()),
                        'scheduleToStartTimeout' => $this->getScheduleToStartTimeout(),
                        'startToCloseTimeout' => $this->getStartToCloseTimeout(),
                        'heartbeatTimeout' => $this->getHeartbeatTimeout())
                );
                break;

            case Enum\DecisionType::START_CHILD_WORKFLOW_EXECUTION:
                $decisions[] = array(
                    'decisionType' => Enum\DecisionType::START_CHILD_WORKFLOW_EXECUTION,
                    'startChildWorkflowExecutionDecisionAttributes' => array(
                        'childPolicy' => 'TERMINATE',
                        'control' => $item->getId(),
                        'executionStartToCloseTimeout' => $this->getExecutionStartToCloseTimeout(),
                        'input' => (string)$lastEventResult,
                        'taskList' => array('name' => $this->getTaskList()),
                        "taskStartToCloseTimeout" => $this->getTaskStartToCloseTimeout(),
                        "workflowId" => microtime(),
                        "workflowType" => array(
                            "name" => $item->getName(),
                            "version" => $item->getVersion())
                    ));
                break;

            case Enum\DecisionType::COMPLETE_WORKFLOW_EXECUTION:
                $decisions[] = array(
                    'decisionType' => Enum\DecisionType::COMPLETE_WORKFLOW_EXECUTION,
                    'completeWorkflowExecutionDecisionAttributes' => array(
                        'result' => $lastEventResult));
                break;

            case Enum\DecisionType::FAIL_WORKFLOW_EXECUTION:
            default:
                $details = 'error';
                $reason = 'error';
                $lastException = $decisionHint->getLastException();
                if ($lastException) {
                    $details = $lastException->getTraceAsString();
                    $reason = $lastException->getMessage();
                }

                $decisions = array(array(
                    'decisionType' => Enum\DecisionType::FAIL_WORKFLOW_EXECUTION,
                    'failWorkflowExecutionDecisionAttributes' => array(
                        'details' => $details,
                        'reason' => $reason)));
                break;
        }

        return $decisions;
    }

    /**
     *
     */
    public function getWorkerIdentity() {
        return $this->workerIdentity;
    }

    /**
     *
     */
    public function setWorkerIdentity($workerIdentity) {
        $this->workerIdentity = $workerIdentity;
    }

    /**
     * @return array
     */
    public function getAllActivities() {
        $activities = array();
        foreach ($this->getWorkflows() as $workflow) {
            foreach ($workflow->getTasksByType(WorkflowTask::ACTIVITY_TYPE) as $activity) {
                $activities[$activity->getName()] = $activity;
            }
        }

        return $activities;
    }

    /**
     * @return array|null
     */
    public function getCachedAllActivities() {
        if (is_null($this->cachedActivities)) {
            $this->cachedActivities = $this->getAllActivities();
        }
        return $this->cachedActivities;
    }

    /**
     * @param $activityName
     * @return null
     */
    public function getActivity($activityName) {
        $result = null;
        $activities = $this->getCachedAllActivities();
        if (array_key_exists($activityName, $activities)) {
            $result = $activities[$activityName];
        }
        return $result;
    }

    /**
     * @return array
     */
    public function getWorkflows() {
        return $this->workflows;
    }
}