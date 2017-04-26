<?php

namespace Aws\Swf\Fluent;


use Aws\Swf\Fluent\Enum;

/**
 * Class DecisionHint
 * @package Aws\Swf\Fluent
 */
class DecisionHint {
    /**
     * @var null
     */
    protected $item = null;
    /**
     * @var null
     */
    protected $decisionType = null;
    /**
     * @var null
     */
    protected $lastEvent = null;

    /**
     * @var null
     */
    protected $lastException = null;


    /**
     * @var null
     */
    protected $timerDuration = '5';

    /**
     * @param null $decisionType
     */
    public function setDecisionType($decisionType) {
        $this->decisionType = $decisionType;
    }

    /**
     * @return null
     */
    public function getDecisionType() {
        return $this->decisionType;
    }

    /**
     * @param null $item
     */
    public function setItem($item) {
        $this->item = $item;
    }

    /**
     * @return null
     */
    public function getItem() {
        return $this->item;
    }

    /**
     * @param null $lastEvent
     */
    public function setLastEvent($lastEvent) {
        $this->lastEvent = $lastEvent;
    }

    /**
     * @return null
     */
    public function getLastEvent() {
        return $this->lastEvent;
    }

    /**
     * @param null $lastException
     */
    public function setLastException($lastException) {
        $this->lastException = $lastException;
    }

    /**
     * @return null
     */
    public function getLastException() {
        return $this->lastException;
    }

    

    /**
     * @return null
     */
    public function getLastEventResult() {
        $result = null;
        $event = $this->getLastEvent();
        if ($event) {
            switch ($event['eventType']) {
                case Enum\EventType::CHILD_WORKFLOW_EXECUTION_COMPLETED:
                    $result = $event['childWorkflowExecutionCompletedEventAttributes']['result'];
                    break;
                case Enum\EventType::ACTIVITY_TASK_COMPLETED:
                    $result = $event['activityTaskCompletedEventAttributes']['result'];
                    break;
                case Enum\EventType::WORKFLOW_EXECUTION_STARTED:
                    $result = $event['workflowExecutionStartedEventAttributes']['input'];
                    break;
            }
        }
        return $result;
    }

    public function setTimerDuration($timerDuration)
    {
        $this->timerDuration = (string)$timerDuration;
    }

    public function getTimerDuration()
    {
        return $this->timerDuration;
    }
}
