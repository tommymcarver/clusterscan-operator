/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Package controller implements the ClusterScan controller for managing scheduled
and one-off Kubernetes cluster scanning jobs.

# Overview

The ClusterScan controller watches ClusterScan custom resources and manages the
lifecycle of scanning jobs. It supports:

  - One-off scans: Run immediately when created (no schedule)
  - Scheduled scans: Run on a cron schedule
  - Manual triggers: Via annotation or triggerNow field
  - History management: Retains configurable number of past scan results
  - Log capture: Optionally stores scan output in status

# Reconciliation Flow

 1. Fetch ClusterScan resource
 2. Handle finalizer (add on create, cleanup on delete)
 3. List and categorize child Jobs (active, successful, failed)
 4. Update status (phase, job counts, history)
 5. Cleanup old jobs exceeding history limit
 6. Determine if new job should be created (schedule, trigger, one-off)
 7. Handle concurrency policy (Forbid, Replace, Allow)
 8. Create job if needed
 9. Update status and requeue for next schedule

# Concurrency Policies

  - Forbid (default): Skip if a job is already running
  - Replace: Delete running job and create new one
  - Allow: Run multiple jobs concurrently

# Triggering Scans

Scans can be triggered via:

  - Schedule: Cron expression in spec.schedule
  - TriggerNow: Set spec.triggerNow to true
  - Annotation: kubectl annotate clusterscan <name> scan.spectrocloud.com/trigger=$(date +%s)
*/
package controller

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	scanv1alpha1 "github.com/tommymcarver/clusterscan-operator/api/v1alpha1"
)

const (
	// jobOwnerKey is the field index for listing jobs by owner
	jobOwnerKey = ".metadata.controller"
	// scanJobLabel is the label used to identify scan jobs
	scanJobLabel = "scan.spectrocloud.com/clusterscan"
	// scheduledTimeAnnotation stores when a job was scheduled
	scheduledTimeAnnotation = "scan.spectrocloud.com/scheduled-at"
	// clusterScanFinalizer is the finalizer added to ClusterScan resources
	clusterScanFinalizer = "scan.spectrocloud.com/finalizer"
	// triggerAnnotation is used to trigger a scan via annotation
	// Usage: kubectl annotate clusterscan <name> scan.spectrocloud.com/trigger=$(date +%s)
	triggerAnnotation = "scan.spectrocloud.com/trigger"
	// lastTriggerAnnotation tracks the last processed trigger value
	lastTriggerAnnotation = "scan.spectrocloud.com/last-trigger"
)

// ClusterScanReconciler reconciles a ClusterScan object.
//
// The reconciler is responsible for:
//   - Creating Kubernetes Jobs based on ClusterScan specifications
//   - Managing job lifecycle (creation, cleanup, status tracking)
//   - Handling scheduled execution via cron expressions
//   - Capturing and storing scan results and logs
//   - Cleaning up resources when ClusterScan is deleted
//
// # Dependencies
//
// The reconciler requires:
//   - Client: For interacting with the Kubernetes API
//   - Scheme: For setting owner references on created Jobs
//   - Clientset: Optional, for reading pod logs (log capture feature)
//   - Clock: Optional, for time operations (defaults to system clock)
type ClusterScanReconciler struct {
	client.Client

	// Scheme is used to set owner references on created Jobs
	Scheme *runtime.Scheme

	// Clock provides time operations. Defaults to realClock if nil.
	// Can be mocked in tests for deterministic time-based behavior.
	Clock

	// Clientset is the Kubernetes clientset for reading pod logs.
	// If nil, log capture is disabled even when spec.retainLogs is true.
	Clientset kubernetes.Interface
}

// Clock interface for time operations.
// This abstraction allows mocking time in unit tests for deterministic behavior.
type Clock interface {
	Now() time.Time
}

// realClock implements Clock using the actual system time.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// +kubebuilder:rbac:groups=scan.spectrocloud.com,resources=clusterscans,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=scan.spectrocloud.com,resources=clusterscans/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=scan.spectrocloud.com,resources=clusterscans/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get

// Reconcile is the main entry point for the controller's reconciliation loop.
//
// It is triggered when:
//   - A ClusterScan resource is created, updated, or deleted
//   - A child Job owned by the ClusterScan changes state
//   - The requeue timer fires (for scheduled scans)
//
// # Return Values
//
//   - ctrl.Result{}: Reconciliation complete, no requeue needed
//   - ctrl.Result{RequeueAfter: duration}: Requeue after duration (for scheduled scans)
//   - error: Reconciliation failed, will be retried with backoff
//
// # Error Handling
//
// The function returns errors for transient failures that should be retried:
//   - API server errors
//   - Resource conflicts (handled with optimistic locking)
//
// Non-fatal errors (like cleanup failures) are logged but don't stop reconciliation.
//
//nolint:gocyclo
func (r *ClusterScanReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Ensure we have a clock
	if r.Clock == nil {
		r.Clock = realClock{}
	}

	// Fetch the ClusterScan instance
	var clusterScan scanv1alpha1.ClusterScan
	if err := r.Get(ctx, req.NamespacedName, &clusterScan); err != nil {
		if apierrors.IsNotFound(err) {
			// Resource was deleted before reconcile - this is normal
			log.V(1).Info("ClusterScan not found, likely deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Unable to fetch ClusterScan")
		return ctrl.Result{}, err
	}

	// Handle finalizer for cleanup on deletion
	if isContinueReconcile, err := r.handleFinalizer(ctx, &clusterScan, req.NamespacedName); err != nil {
		return ctrl.Result{}, err
	} else if !isContinueReconcile {
		// Resource is being deleted, stop reconciliation
		return ctrl.Result{}, nil
	}

	// List all jobs owned by this ClusterScan
	var childJobs batchv1.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "Unable to list child Jobs")
		return ctrl.Result{}, err
	}

	// Categorize jobs by status
	// Pre-allocate with reasonable capacity to reduce allocations
	totalJobs := len(childJobs.Items)
	activeJobs := make([]*batchv1.Job, 0, min(totalJobs, 2))     // Usually 0-1 active
	successfulJobs := make([]*batchv1.Job, 0, totalJobs)         // Most common case
	failedJobs := make([]*batchv1.Job, 0, min(totalJobs/4+1, 5)) // Failures are less common

	for i := range childJobs.Items {
		job := &childJobs.Items[i]
		_, finishedType := isJobFinished(job)
		switch finishedType {
		case "":
			activeJobs = append(activeJobs, job)
		case batchv1.JobComplete:
			successfulJobs = append(successfulJobs, job)
		case batchv1.JobFailed:
			failedJobs = append(failedJobs, job)
		}
	}

	// Sort jobs by start time for history management
	sortJobsByStartTime(successfulJobs)
	sortJobsByStartTime(failedJobs)

	// Update status with active jobs
	clusterScan.Status.Active = nil
	for _, job := range activeJobs {
		clusterScan.Status.Active = append(clusterScan.Status.Active, corev1.ObjectReference{
			APIVersion: batchv1.SchemeGroupVersion.String(),
			Kind:       "Job",
			Name:       job.Name,
			Namespace:  job.Namespace,
			UID:        job.UID,
		})
	}

	// Update job counts
	clusterScan.Status.CompletedJobs = int32(len(successfulJobs))
	clusterScan.Status.FailedJobs = int32(len(failedJobs))

	// Update phase based on current state
	clusterScan.Status.Phase = r.determinePhase(&clusterScan, activeJobs, successfulJobs, failedJobs)

	// Update last successful time
	if len(successfulJobs) > 0 {
		lastSuccess := successfulJobs[len(successfulJobs)-1]
		if lastSuccess.Status.CompletionTime != nil {
			clusterScan.Status.LastSuccessfulTime = lastSuccess.Status.CompletionTime
		}
	}

	// Update scan history and LastScanResult from finished jobs
	r.updateScanHistory(ctx, &clusterScan, successfulJobs, failedJobs)

	// Clean up old jobs based on history limits
	if err := r.cleanupOldJobs(ctx, &clusterScan, successfulJobs, failedJobs); err != nil {
		// Non-fatal: log warning but continue reconciliation
		log.V(1).Info("Unable to clean up old jobs", "error", err)
	}

	// Determine if we should create a new job and why
	trigger := r.shouldCreateJob(ctx, &clusterScan, activeJobs, successfulJobs)
	shouldCreate := trigger.reason != TriggerReasonNone

	// Handle side effects based on trigger reason
	if err := r.handleTriggerSideEffects(ctx, &clusterScan, trigger.reason, req.NamespacedName); err != nil {
		return ctrl.Result{}, err
	}

	// Handle concurrency policy
	if shouldCreate && len(activeJobs) > 0 {
		switch clusterScan.Spec.ConcurrencyPolicy {
		case scanv1alpha1.ForbidConcurrent, "":
			log.V(1).Info("Skipping: job already active (policy=Forbid)", "activeJobs", len(activeJobs))
			shouldCreate = false
		case scanv1alpha1.ReplaceConcurrent:
			log.Info("Replacing active jobs (policy=Replace)", "count", len(activeJobs))
			for _, job := range activeJobs {
				if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
					log.Error(err, "Unable to delete active job", "job", job.Name)
					return ctrl.Result{}, err
				}
			}
		case scanv1alpha1.AllowConcurrent:
			log.V(1).Info("Creating concurrent job (policy=Allow)", "activeJobs", len(activeJobs))
		}
	}

	// Create the job if needed
	if shouldCreate {
		job, err := r.constructJobForClusterScan(&clusterScan)
		if err != nil {
			log.Error(err, "Unable to construct job from template")
			return ctrl.Result{}, err
		}

		if err := r.Create(ctx, job); err != nil {
			log.Error(err, "Unable to create Job", "job", job.Name)
			return ctrl.Result{}, err
		}

		log.Info("Scan job created", "job", job.Name, "trigger", trigger.reason)
		now := metav1.NewTime(r.Now())
		clusterScan.Status.LastScheduleTime = &now
	}

	// Set next schedule time for scheduled scans (reusing info from shouldCreateJob)
	var requeueAfter time.Duration
	if clusterScan.Spec.Schedule != "" && !trigger.schedule.nextScheduleTime.IsZero() {
		clusterScan.Status.NextScheduleTime = &metav1.Time{Time: trigger.schedule.nextScheduleTime}
		requeueAfter = trigger.schedule.waitDuration
	}

	// Update status using patch (more resilient to conflicts than update)
	if err := r.updateStatus(ctx, req.NamespacedName, clusterScan.Status); err != nil {
		log.Error(err, "Unable to update status")
		return ctrl.Result{}, err
	}

	// Requeue if we have a scheduled time
	if requeueAfter > 0 {
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	return ctrl.Result{}, nil
}

// handleFinalizer manages the finalizer for ClusterScan resources.
// Returns:
//   - continueReconcile: false if the resource is being deleted (stop reconciliation)
//   - err: any error that occurred
func (r *ClusterScanReconciler) handleFinalizer(
	ctx context.Context,
	scan *scanv1alpha1.ClusterScan,
	namespacedName types.NamespacedName,
) (continueReconcile bool, err error) {
	log := logf.FromContext(ctx)

	// Check if the ClusterScan is being deleted
	if !scan.DeletionTimestamp.IsZero() {
		// Resource is being deleted - handle cleanup
		if controllerutil.ContainsFinalizer(scan, clusterScanFinalizer) {
			log.V(1).Info("Processing deletion, cleaning up child resources")

			// Perform cleanup of child resources
			if err := r.cleanupOnDelete(ctx, scan); err != nil {
				log.Error(err, "Unable to cleanup child resources")
				return false, err
			}

			// Remove finalizer to allow deletion to proceed
			log.V(1).Info("Removing finalizer")
			controllerutil.RemoveFinalizer(scan, clusterScanFinalizer)
			if err := r.Update(ctx, scan); err != nil {
				// If resource is already gone, that's fine - deletion succeeded
				if apierrors.IsNotFound(err) {
					log.V(1).Info("Resource already deleted")
					return false, nil
				}
				log.Error(err, "Unable to remove finalizer")
				return false, err
			}
		}

		// Stop reconciliation - resource is being deleted
		return false, nil
	}

	// Resource is not being deleted - ensure finalizer is present
	if !controllerutil.ContainsFinalizer(scan, clusterScanFinalizer) {
		log.V(1).Info("Adding finalizer")
		patch := client.MergeFrom(scan.DeepCopy())
		controllerutil.AddFinalizer(scan, clusterScanFinalizer)
		if err := r.Patch(ctx, scan, patch); err != nil {
			log.Error(err, "Unable to add finalizer")
			return false, err
		}
		// Re-fetch after patch to get the latest resourceVersion
		if err := r.Get(ctx, namespacedName, scan); err != nil {
			return false, err
		}
	}

	// Continue with normal reconciliation
	return true, nil
}

// handleTriggerSideEffects handles the side effects of trigger reasons.
//
// Different triggers require different cleanup actions:
//   - TriggerNow: Reset the spec.triggerNow field to false
//   - Annotation: Update the last-trigger annotation to prevent re-triggering
//   - OneOff/Schedule: No side effects needed
//
// This separation keeps shouldCreateJob as a pure decision function while
// centralizing all trigger-related mutations here.
func (r *ClusterScanReconciler) handleTriggerSideEffects(
	ctx context.Context,
	scan *scanv1alpha1.ClusterScan,
	reason TriggerReason,
	namespacedName types.NamespacedName,
) error {
	log := logf.FromContext(ctx)

	switch reason {
	case TriggerReasonTriggerNow:
		// Reset triggerNow flag and update LastTriggeredTime
		now := metav1.NewTime(r.Now())
		scan.Status.LastTriggeredTime = &now

		patch := client.MergeFrom(scan.DeepCopy())
		scan.Spec.TriggerNow = false
		if err := r.Patch(ctx, scan, patch); err != nil {
			log.Error(err, "Unable to reset triggerNow flag")
			return err
		}

	case TriggerReasonAnnotation:
		// Update last-trigger annotation to prevent re-triggering
		now := metav1.NewTime(r.Now())
		scan.Status.LastTriggeredTime = &now

		triggerValue := scan.Annotations[triggerAnnotation]
		patch := client.MergeFrom(scan.DeepCopy())
		if scan.Annotations == nil {
			scan.Annotations = make(map[string]string)
		}
		scan.Annotations[lastTriggerAnnotation] = triggerValue
		if err := r.Patch(ctx, scan, patch); err != nil {
			log.Error(err, "Unable to update trigger annotation")
			return err
		}
		// Re-fetch after patch to get updated resourceVersion
		if err := r.Get(ctx, namespacedName, scan); err != nil {
			return err
		}

	case TriggerReasonOneOff, TriggerReasonSchedule, TriggerReasonNone:
		// No side effects needed for these triggers
	}

	return nil
}

// determinePhase determines the current phase based on job states.
//
// Phase transitions:
//
//	Pending → Running (job started)
//	Running → Completed (job succeeded)
//	Running → Failed (job failed)
//
// For scheduled scans, phase reflects the most recent job's state.
// For one-off scans, phase is final once a job completes.
//
// Parameters:
//   - scan: The ClusterScan resource
//   - activeJobs: Currently running jobs
//   - successfulJobs: Completed jobs (sorted by start time)
//   - failedJobs: Failed jobs (sorted by start time)
func (r *ClusterScanReconciler) determinePhase(
	scan *scanv1alpha1.ClusterScan,
	activeJobs, successfulJobs, failedJobs []*batchv1.Job,
) scanv1alpha1.ScanPhase {
	// If there are active jobs, we're running
	if len(activeJobs) > 0 {
		return scanv1alpha1.ScanPhaseRunning
	}

	// For one-off scans (no schedule)
	if scan.Spec.Schedule == "" {
		if len(successfulJobs) > 0 {
			return scanv1alpha1.ScanPhaseCompleted
		}
		if len(failedJobs) > 0 {
			return scanv1alpha1.ScanPhaseFailed
		}
		return scanv1alpha1.ScanPhasePending
	}

	// For scheduled scans, show the result of the last job
	// Since successfulJobs and failedJobs are already sorted, just compare the last element of each
	lastJob, wasSuccessful := getMostRecentJobWithStatus(successfulJobs, failedJobs)
	if lastJob == nil {
		return scanv1alpha1.ScanPhasePending
	}

	// We already know the job is finished (it's in one of the finished arrays)
	if wasSuccessful {
		return scanv1alpha1.ScanPhaseCompleted
	}
	return scanv1alpha1.ScanPhaseFailed
}

// TriggerReason indicates why a job should be created.
type TriggerReason string

const (
	// TriggerReasonNone means no job should be created.
	TriggerReasonNone TriggerReason = ""
	// TriggerReasonOneOff means this is a one-off scan that hasn't run yet.
	TriggerReasonOneOff TriggerReason = "OneOff"
	// TriggerReasonSchedule means the cron schedule time was reached.
	TriggerReasonSchedule TriggerReason = "Schedule"
	// TriggerReasonTriggerNow means spec.triggerNow was set to true.
	TriggerReasonTriggerNow TriggerReason = "TriggerNow"
	// TriggerReasonAnnotation means a trigger annotation was detected.
	TriggerReasonAnnotation TriggerReason = "Annotation"
)

// triggerResult contains the result of shouldCreateJob evaluation.
type triggerResult struct {
	// reason indicates why a job should be created (empty if it shouldn't)
	reason TriggerReason
	// scheduleInfo contains schedule timing info (only for scheduled scans)
	schedule scheduleInfo
}

// shouldCreateJob determines if a new job should be created and why.
//
// This function consolidates all trigger logic into one place:
//   - Suspension check
//   - One-off scan (first run)
//   - Scheduled scan (cron time reached)
//   - TriggerNow field
//   - Trigger annotation
//
// Returns triggerResult containing:
//   - reason: Why the job should be created (empty if it shouldn't)
//   - schedule: Schedule timing info (for scheduled scans)
//
// Note: This function does NOT perform side effects (resetting triggerNow, etc.).
// The caller is responsible for handling side effects based on the returned reason.
func (r *ClusterScanReconciler) shouldCreateJob(
	ctx context.Context,
	scan *scanv1alpha1.ClusterScan,
	activeJobs, successfulJobs []*batchv1.Job,
) triggerResult {
	log := logf.FromContext(ctx)

	// Check if suspended - never create jobs when suspended
	if scan.Spec.Suspend != nil && *scan.Spec.Suspend {
		log.V(1).Info("Scan suspended, skipping")
		return triggerResult{reason: TriggerReasonNone}
	}

	// Check triggerNow field (highest priority manual trigger)
	if scan.Spec.TriggerNow {
		log.V(1).Info("Trigger: triggerNow=true")
		return triggerResult{reason: TriggerReasonTriggerNow}
	}

	// Check trigger annotation
	if triggerValue, ok := scan.Annotations[triggerAnnotation]; ok && triggerValue != "" {
		lastTrigger := scan.Annotations[lastTriggerAnnotation]
		if triggerValue != lastTrigger {
			log.V(1).Info("Trigger: annotation", "value", triggerValue)
			return triggerResult{reason: TriggerReasonAnnotation}
		}
	}

	// One-off scan (no schedule) - only create if no jobs exist
	if scan.Spec.Schedule == "" {
		if scan.Status.LastScheduleTime == nil && len(activeJobs) == 0 && len(successfulJobs) == 0 {
			return triggerResult{reason: TriggerReasonOneOff}
		}
		return triggerResult{reason: TriggerReasonNone}
	}

	// Scheduled scan - check if it's time to run
	schedInfo := r.isScheduledTimeReached(ctx, scan)
	if schedInfo.reached {
		return triggerResult{reason: TriggerReasonSchedule, schedule: schedInfo}
	}
	return triggerResult{reason: TriggerReasonNone, schedule: schedInfo}
}

// scheduleInfo contains the result of schedule evaluation.
type scheduleInfo struct {
	// reached is true if it's time to run a job
	reached bool
	// nextScheduleTime is the next time a job should run
	nextScheduleTime time.Time
	// waitDuration is the time until nextScheduleTime
	waitDuration time.Duration
}

// isScheduledTimeReached checks if the scheduled time has been reached.
//
// The function:
//  1. Parses the cron schedule from spec.schedule
//  2. Calculates next run time based on lastScheduleTime (or creation time)
//  3. Checks if current time >= next scheduled time
//  4. Handles startingDeadlineSeconds (missed schedule detection)
//
// Returns scheduleInfo containing:
//   - reached: true if it's time to run
//   - nextScheduleTime: the calculated next schedule time
//   - waitDuration: time until next schedule if not reached
func (r *ClusterScanReconciler) isScheduledTimeReached(
	ctx context.Context,
	scan *scanv1alpha1.ClusterScan,
) scheduleInfo {
	log := logf.FromContext(ctx)

	sched, err := cron.ParseStandard(scan.Spec.Schedule)
	if err != nil {
		log.Error(err, "Invalid cron schedule", "schedule", scan.Spec.Schedule)
		return scheduleInfo{}
	}

	now := r.Now()
	var lastScheduleTime time.Time

	if scan.Status.LastScheduleTime != nil {
		lastScheduleTime = scan.Status.LastScheduleTime.Time
	} else {
		lastScheduleTime = scan.CreationTimestamp.Time
	}

	// Get the next scheduled time after the last schedule
	nextSchedule := sched.Next(lastScheduleTime)

	// Check starting deadline
	if scan.Spec.StartingDeadlineSeconds != nil {
		deadline := nextSchedule.Add(time.Duration(*scan.Spec.StartingDeadlineSeconds) * time.Second)
		if now.After(deadline) {
			log.V(1).Info("Missed scheduled time, skipping to next", "missed", nextSchedule, "deadline", deadline)
			// We missed this schedule, find the next one
			nextSchedule = sched.Next(now)
		}
	}

	if now.After(nextSchedule) || now.Equal(nextSchedule) {
		// Time to run - calculate the NEXT schedule after this one for status
		futureSchedule := sched.Next(now)
		return scheduleInfo{
			reached:          true,
			nextScheduleTime: futureSchedule,
			waitDuration:     futureSchedule.Sub(now), // Use mocked clock instead of time.Until
		}
	}

	return scheduleInfo{
		reached:          false,
		nextScheduleTime: nextSchedule,
		waitDuration:     nextSchedule.Sub(now), // Use mocked clock instead of time.Until
	}
}

// constructJobForClusterScan creates a Kubernetes Job from the ClusterScan spec.
//
// The created Job:
//   - Has a unique name: {clusterscan-name}-{unix-timestamp}
//   - Is labeled with scan.spectrocloud.com/clusterscan={clusterscan-name}
//   - Has an owner reference to the ClusterScan (for garbage collection)
//   - Uses RestartPolicy=Never (jobs handle retries via backoffLimit)
//   - Contains a single container named "scan" with the specified image
//
// The function copies these fields from ScanTemplate:
//   - Image, Command, Args, Env, EnvFrom
//   - Resources, VolumeMounts, Volumes
//   - ServiceAccountName, NodeSelector, Tolerations, Affinity
//   - ImagePullSecrets, SecurityContext
//   - BackoffLimit, ActiveDeadlineSeconds
func (r *ClusterScanReconciler) constructJobForClusterScan(scan *scanv1alpha1.ClusterScan) (*batchv1.Job, error) {
	// Generate a unique name using Unix timestamp to ensure ordering
	name := fmt.Sprintf("%s-%d", scan.Name, time.Now().Unix())

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: scan.Namespace,
			Labels: map[string]string{
				scanJobLabel: scan.Name,
			},
			Annotations: map[string]string{
				scheduledTimeAnnotation: r.Now().Format(time.RFC3339),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          scan.Spec.ScanTemplate.BackoffLimit,
			ActiveDeadlineSeconds: scan.Spec.ScanTemplate.ActiveDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						scanJobLabel: scan.Name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: scan.Spec.ScanTemplate.ServiceAccountName,
					NodeSelector:       scan.Spec.ScanTemplate.NodeSelector,
					Tolerations:        scan.Spec.ScanTemplate.Tolerations,
					Affinity:           scan.Spec.ScanTemplate.Affinity,
					ImagePullSecrets:   scan.Spec.ScanTemplate.ImagePullSecrets,
					SecurityContext:    scan.Spec.ScanTemplate.SecurityContext,
					Volumes:            scan.Spec.ScanTemplate.Volumes,
					Containers: []corev1.Container{
						{
							Name:            "scan",
							Image:           scan.Spec.ScanTemplate.Image,
							ImagePullPolicy: scan.Spec.ScanTemplate.ImagePullPolicy,
							Command:         scan.Spec.ScanTemplate.Command,
							Args:            scan.Spec.ScanTemplate.Args,
							Env:             scan.Spec.ScanTemplate.Env,
							EnvFrom:         scan.Spec.ScanTemplate.EnvFrom,
							Resources:       scan.Spec.ScanTemplate.Resources,
							VolumeMounts:    scan.Spec.ScanTemplate.VolumeMounts,
						},
					},
				},
			},
		},
	}

	// Set default backoff limit if not specified
	if job.Spec.BackoffLimit == nil {
		job.Spec.BackoffLimit = ptr.To(int32(3))
	}

	// Set ClusterScan as the owner
	if err := controllerutil.SetControllerReference(scan, job, r.Scheme); err != nil {
		return nil, err
	}

	return job, nil
}

// cleanupOldJobs removes old jobs that exceed the history limit.
//
// Jobs are deleted oldest-first to maintain the most recent history.
// The history limit comes from spec.historyLimit (default: 5).
//
// This function:
//  1. Merges successful and failed jobs (already sorted)
//  2. Calculates how many jobs exceed the limit
//  3. Deletes excess jobs using Background propagation
//
// Errors are returned if deletion fails (except NotFound, which is ignored).
func (r *ClusterScanReconciler) cleanupOldJobs(
	ctx context.Context,
	scan *scanv1alpha1.ClusterScan,
	successfulJobs, failedJobs []*batchv1.Job,
) error {
	log := logf.FromContext(ctx)

	// Get history limit from spec (default 5)
	historyLimit := int32(5)
	if scan.Spec.HistoryLimit != nil {
		historyLimit = *scan.Spec.HistoryLimit
	}

	// Merge already-sorted slices in O(n) instead of concat + sort O(n log n)
	allFinishedJobs := mergeSortedJobs(successfulJobs, failedJobs)

	// Delete jobs that exceed the history limit
	if int32(len(allFinishedJobs)) > historyLimit {
		jobsToDelete := allFinishedJobs[:int32(len(allFinishedJobs))-historyLimit]
		for _, job := range jobsToDelete {
			log.V(1).Info("Deleting old job", "job", job.Name)
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	return nil
}

// updateScanHistory builds and maintains the scan history from finished jobs.
//
// This function:
//  1. Merges successful and failed jobs (pre-sorted, O(n) merge)
//  2. Identifies jobs not yet in history (using a map for O(1) lookups)
//  3. Converts new jobs to ScanResult entries (including log capture)
//  4. Sorts history by completion time (only if new entries added)
//  5. Trims history to respect historyLimit
//  6. Sets LastScanResult to the most recent entry
//
// Optimization: Sorting is skipped if no new entries were added, since
// existing history is already sorted.
func (r *ClusterScanReconciler) updateScanHistory(
	ctx context.Context,
	scan *scanv1alpha1.ClusterScan,
	successfulJobs, failedJobs []*batchv1.Job,
) {
	// Get history limit from spec (default 5)
	historyLimit := int32(5)
	if scan.Spec.HistoryLimit != nil {
		historyLimit = *scan.Spec.HistoryLimit
	}

	// Merge already-sorted slices in O(n) instead of concat + sort O(n log n)
	allFinishedJobs := mergeSortedJobs(successfulJobs, failedJobs)
	if len(allFinishedJobs) == 0 {
		return
	}

	// Build a set of job names already in history to avoid duplicates
	// Pre-allocate with capacity for efficiency
	existingJobs := make(map[string]struct{}, len(scan.Status.History))
	for _, result := range scan.Status.History {
		existingJobs[result.JobName] = struct{}{}
	}

	// Track if any new entries were added (to skip sorting if not)
	addedNew := false

	// Add new results to history
	for _, job := range allFinishedJobs {
		if _, exists := existingJobs[job.Name]; exists {
			continue // Already in history
		}

		result := r.jobToScanResult(ctx, scan, job)
		if result != nil {
			scan.Status.History = append(scan.Status.History, *result)
			existingJobs[job.Name] = struct{}{}
			addedNew = true
		}
	}

	// Only sort if new entries were added (optimization)
	if addedNew {
		sortHistoryByCompletionTime(scan.Status.History)
	}

	// Trim history to limit
	if int32(len(scan.Status.History)) > historyLimit {
		excess := int32(len(scan.Status.History)) - historyLimit
		scan.Status.History = scan.Status.History[excess:]
	}

	// Set LastScanResult to the most recent entry
	if len(scan.Status.History) > 0 {
		lastResult := scan.Status.History[len(scan.Status.History)-1]
		scan.Status.LastScanResult = &lastResult
	}
}

// jobToScanResult converts a completed Job to a ScanResult for history storage.
//
// The result includes:
//   - JobName: Name of the job
//   - StartTime/CompletionTime: Timing information
//   - Succeeded: Whether the job completed successfully
//   - Message: Human-readable status message
//   - Findings: Count from job annotation (if set by scan container)
//   - Logs: Captured output (if spec.retainLogs is true)
//
// Returns nil if the job is not finished (still running or pending).
//
// # Findings Annotation
//
// Scan containers can set the findings count by annotating their own job:
//
//	kubectl annotate job $JOB_NAME scan.spectrocloud.com/findings=<count>
func (r *ClusterScanReconciler) jobToScanResult(
	ctx context.Context,
	scan *scanv1alpha1.ClusterScan,
	job *batchv1.Job,
) *scanv1alpha1.ScanResult {
	log := logf.FromContext(ctx)

	finished, finishedType := isJobFinished(job)
	if !finished {
		return nil
	}

	result := &scanv1alpha1.ScanResult{
		JobName:   job.Name,
		Succeeded: finishedType == batchv1.JobComplete,
	}

	// Set start time
	if job.Status.StartTime != nil {
		result.StartTime = job.Status.StartTime
	}

	// Set completion time
	if job.Status.CompletionTime != nil {
		result.CompletionTime = job.Status.CompletionTime
	}

	// Set message based on job status
	if result.Succeeded {
		result.Message = "Scan completed successfully"
	} else {
		// Try to get failure reason from job conditions
		for _, cond := range job.Status.Conditions {
			if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
				result.Message = fmt.Sprintf("Scan failed: %s", cond.Reason)
				break
			}
		}
		if result.Message == "" {
			result.Message = "Scan failed"
		}
	}

	// Check for results annotation (if the scan container wrote one)
	if job.Annotations != nil {
		if findings, ok := job.Annotations["scan.spectrocloud.com/findings"]; ok {
			if count, err := parseInt32(findings); err == nil {
				result.Findings = &count
			}
		}
	}

	// Capture logs if enabled
	if scan.Spec.RetainLogs && r.Clientset != nil {
		logs, truncated, err := r.getJobLogs(ctx, job, scan)
		if err != nil {
			log.V(1).Info("Failed to capture logs", "job", job.Name, "error", err)
		} else {
			result.Logs = logs
			result.LogsTruncated = truncated
		}
	}

	return result
}

// getJobLogs retrieves logs from the pods of a completed job.
//
// This function:
//  1. Lists pods for the job (limited to 1 for efficiency)
//  2. Requests logs from the "scan" container
//  3. Applies byte limit from spec.logsMaxBytes (default 10KB)
//  4. Returns truncated flag if logs exceeded the limit
//
// Log retrieval uses the Kubernetes clientset (not controller-runtime client)
// because controller-runtime doesn't support pod log streaming.
//
// Returns:
//   - logs: The captured log content (may be truncated)
//   - truncated: true if logs were truncated to fit maxBytes
//   - error: Any error during log retrieval
func (r *ClusterScanReconciler) getJobLogs(
	ctx context.Context,
	job *batchv1.Job,
	scan *scanv1alpha1.ClusterScan,
) (string, bool, error) {
	// Get max bytes from spec (default 10KB)
	maxBytes := int64(10000)
	if scan.Spec.LogsMaxBytes != nil {
		maxBytes = int64(*scan.Spec.LogsMaxBytes)
	}

	// List pods for this job (limit to 1 since we only need the first pod)
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{"job-name": job.Name},
		client.Limit(1), // Optimization: only fetch 1 pod since jobs typically have one
	); err != nil {
		return "", false, fmt.Errorf("failed to list pods: %w", err)
	}

	if len(pods.Items) == 0 {
		return "", false, fmt.Errorf("no pods found for job")
	}

	pod := pods.Items[0]

	// Request logs with byte limit
	limitBytes := maxBytes + 1 // Request 1 extra byte to detect truncation
	logReq := r.Clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
		Container:  "scan",
		LimitBytes: &limitBytes,
	})

	logStream, err := logReq.Stream(ctx)
	if err != nil {
		return "", false, fmt.Errorf("failed to get log stream: %w", err)
	}
	defer logStream.Close() //nolint:errcheck

	// Read logs
	var buf bytes.Buffer
	_, err = io.Copy(&buf, logStream)
	if err != nil {
		return "", false, fmt.Errorf("failed to read logs: %w", err)
	}

	logs := buf.String()
	truncated := int64(len(logs)) > maxBytes

	// Truncate if needed
	if truncated {
		logs = logs[:maxBytes]
	}

	return logs, truncated, nil
}

// sortHistoryByCompletionTime sorts scan results by completion time (oldest first).
// Results with nil completion times are sorted to the front.
// This ensures the most recent result is always at the end of the slice.
func sortHistoryByCompletionTime(history []scanv1alpha1.ScanResult) {
	sort.Slice(history, func(i, j int) bool {
		if history[i].CompletionTime == nil {
			return history[j].CompletionTime != nil
		}
		if history[j].CompletionTime == nil {
			return false
		}
		return history[i].CompletionTime.Before(history[j].CompletionTime)
	})
}

// parseInt32 parses a string to int32
func parseInt32(s string) (int32, error) {
	var i int32
	_, err := fmt.Sscanf(s, "%d", &i)
	return i, err
}

// cleanupOnDelete performs cleanup when a ClusterScan is being deleted.
//
// This is called by the finalizer to ensure all child resources are cleaned up
// before the ClusterScan is deleted. While Kubernetes garbage collection would
// eventually clean up Jobs with owner references, this explicit cleanup:
//
//   - Immediately stops running scan jobs (rather than waiting for GC)
//   - Uses Foreground propagation to ensure pods are deleted first
//   - Provides predictable cleanup timing
//
// The cleanup is idempotent - it's safe to call multiple times.
func (r *ClusterScanReconciler) cleanupOnDelete(ctx context.Context, scan *scanv1alpha1.ClusterScan) error {
	log := logf.FromContext(ctx)

	// List all jobs owned by this ClusterScan
	var childJobs batchv1.JobList
	if err := r.List(ctx, &childJobs,
		client.InNamespace(scan.Namespace),
		client.MatchingFields{jobOwnerKey: scan.Name},
	); err != nil {
		return fmt.Errorf("failed to list child jobs: %w", err)
	}

	// Delete all child jobs with Foreground propagation.
	// Foreground ensures pods are deleted before the job, giving running
	// containers a chance to receive SIGTERM and clean up gracefully.
	for i := range childJobs.Items {
		job := &childJobs.Items[i]
		log.V(1).Info("Deleting child job", "job", job.Name)

		if err := r.Delete(ctx, job,
			client.PropagationPolicy(metav1.DeletePropagationForeground),
		); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete job %s: %w", job.Name, err)
		}
	}

	log.V(1).Info("Cleanup completed", "deletedJobs", len(childJobs.Items))
	return nil
}

// updateStatus fetches the latest ClusterScan and applies the computed status.
//
// This function uses a patch-based update strategy to minimize conflicts:
//
//  1. Fetch the latest version of the resource (to get current resourceVersion)
//  2. Create a patch from the current state
//  3. Apply the computed status
//  4. Patch only the status subresource
//
// Using Patch instead of Update is more resilient to concurrent modifications
// because it merges changes rather than replacing the entire status.
//
// Note: This requires RBAC permission for "patch" on clusterscans/status.
func (r *ClusterScanReconciler) updateStatus(
	ctx context.Context,
	namespacedName types.NamespacedName,
	status scanv1alpha1.ClusterScanStatus,
) error {
	// Fetch the latest version to get current resourceVersion
	var latest scanv1alpha1.ClusterScan
	if err := r.Get(ctx, namespacedName, &latest); err != nil {
		return err
	}

	// Use MergeFrom patch for better conflict handling than Update
	patch := client.MergeFrom(latest.DeepCopy())
	latest.Status = status

	return r.Status().Patch(ctx, &latest, patch)
}

// isJobFinished checks if a job has completed (either successfully or with failure).
//
// A job is considered finished if it has a condition of type Complete or Failed
// with status True. Returns the condition type to distinguish success from failure.
//
// Returns:
//   - finished: true if job has completed
//   - conditionType: JobComplete or JobFailed (empty string if not finished)
func isJobFinished(job *batchv1.Job) (bool, batchv1.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

// sortJobsByStartTime sorts jobs by their start time (oldest first).
// Jobs with nil start times are sorted to the front.
// This is used to maintain chronological order for history management.
func sortJobsByStartTime(jobs []*batchv1.Job) {
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].Status.StartTime == nil {
			return jobs[j].Status.StartTime != nil
		}
		if jobs[j].Status.StartTime == nil {
			return false
		}
		return jobs[i].Status.StartTime.Before(jobs[j].Status.StartTime)
	})
}

// getMostRecentJobWithStatus returns the job with the latest start time from two pre-sorted slices,
// along with a boolean indicating if the job came from the first slice (sortedA).
// This avoids redundant isJobFinished calls when we already know which array contains successful/failed jobs.
func getMostRecentJobWithStatus(sortedA, sortedB []*batchv1.Job) (*batchv1.Job, bool) {
	var lastA, lastB *batchv1.Job

	if len(sortedA) > 0 {
		lastA = sortedA[len(sortedA)-1]
	}
	if len(sortedB) > 0 {
		lastB = sortedB[len(sortedB)-1]
	}

	if lastA == nil {
		return lastB, false // from B (or nil)
	}
	if lastB == nil {
		return lastA, true // from A
	}

	// Compare start times
	if lastA.Status.StartTime == nil {
		return lastB, false
	}
	if lastB.Status.StartTime == nil {
		return lastA, true
	}

	if lastA.Status.StartTime.After(lastB.Status.StartTime.Time) {
		return lastA, true
	}
	return lastB, false
}

// mergeSortedJobs merges two pre-sorted job slices into one sorted slice.
// This is O(n+m) instead of O((n+m) log(n+m)) for concat + sort.
func mergeSortedJobs(sortedA, sortedB []*batchv1.Job) []*batchv1.Job {
	result := make([]*batchv1.Job, 0, len(sortedA)+len(sortedB))
	i, j := 0, 0

	for i < len(sortedA) && j < len(sortedB) {
		timeA := sortedA[i].Status.StartTime
		timeB := sortedB[j].Status.StartTime

		// Handle nil times (push to front)
		if timeA == nil {
			result = append(result, sortedA[i])
			i++
		} else if timeB == nil {
			result = append(result, sortedB[j])
			j++
		} else if timeA.Before(timeB) {
			result = append(result, sortedA[i])
			i++
		} else {
			result = append(result, sortedB[j])
			j++
		}
	}

	// Append remaining elements
	result = append(result, sortedA[i:]...)
	result = append(result, sortedB[j:]...)

	return result
}

// SetupWithManager sets up the controller with the Manager.
//
// This function:
//
//  1. Creates a field index on Jobs for efficient owner lookups.
//     This allows O(1) lookups of all Jobs owned by a ClusterScan,
//     instead of filtering in memory after listing all Jobs.
//
//  2. Configures the controller to:
//     - Watch ClusterScan resources (primary resource)
//     - Watch Jobs owned by ClusterScans (triggers reconcile on job state changes)
//
// The controller is named "clusterscan" and will appear in logs and metrics
// with this name.
func (r *ClusterScanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Create a field index on Jobs by their controller owner.
	// This enables efficient listing of all Jobs owned by a specific ClusterScan
	// using client.MatchingFields{jobOwnerKey: scanName}.
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &batchv1.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		job := rawObj.(*batchv1.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		// Only index Jobs owned by ClusterScan resources
		if owner.APIVersion != scanv1alpha1.GroupVersion.String() || owner.Kind != "ClusterScan" {
			return nil
		}
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	// Build the controller:
	// - For(): Primary watched resource (ClusterScan)
	// - Owns(): Secondary watched resource (Jobs) - triggers reconcile when owned Jobs change
	return ctrl.NewControllerManagedBy(mgr).
		For(&scanv1alpha1.ClusterScan{}).
		Owns(&batchv1.Job{}).
		Named("clusterscan").
		Complete(r)
}

// Ensure ClusterScan implements the necessary interface for type checking
var _ types.NamespacedName
