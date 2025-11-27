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

package controller

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/robfig/cron/v3"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
)

// ClusterScanReconciler reconciles a ClusterScan object
type ClusterScanReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock  // For testing, allows mocking time
}

// Clock interface for time operations (allows mocking in tests)
type Clock interface {
	Now() time.Time
}

// realClock implements Clock using the actual time
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// +kubebuilder:rbac:groups=scan.spectrocloud.com,resources=clusterscans,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=scan.spectrocloud.com,resources=clusterscans/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=scan.spectrocloud.com,resources=clusterscans/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles the reconciliation loop for ClusterScan resources
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
			log.Info("ClusterScan resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get ClusterScan")
		return ctrl.Result{}, err
	}

	// List all jobs owned by this ClusterScan
	var childJobs batchv1.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "Failed to list child Jobs")
		return ctrl.Result{}, err
	}

	// Categorize jobs by status
	var activeJobs, successfulJobs, failedJobs []*batchv1.Job
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

	// Clean up old jobs based on history limits
	if err := r.cleanupOldJobs(ctx, &clusterScan, successfulJobs, failedJobs); err != nil {
		log.Error(err, "Failed to clean up old jobs")
		// Continue reconciliation even if cleanup fails
	}

	// Determine if we should create a new job
	shouldCreateJob, requeueAfter := r.shouldCreateJob(ctx, &clusterScan, activeJobs, successfulJobs)

	// Handle triggerNow - reset the flag after processing
	if clusterScan.Spec.TriggerNow {
		log.Info("TriggerNow is set, will create job immediately")
		shouldCreateJob = true
		now := metav1.NewTime(r.Clock.Now())
		clusterScan.Status.LastTriggeredTime = &now

		// Reset triggerNow flag
		clusterScan.Spec.TriggerNow = false
		if err := r.Update(ctx, &clusterScan); err != nil {
			log.Error(err, "Failed to reset triggerNow flag")
			return ctrl.Result{}, err
		}
	}

	// Handle concurrency policy
	if shouldCreateJob && len(activeJobs) > 0 {
		switch clusterScan.Spec.ConcurrencyPolicy {
		case scanv1alpha1.ForbidConcurrent, "":
			log.Info("Concurrency policy is Forbid and job is active, skipping")
			shouldCreateJob = false
		case scanv1alpha1.ReplaceConcurrent:
			log.Info("Concurrency policy is Replace, deleting active jobs")
			for _, job := range activeJobs {
				if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
					log.Error(err, "Failed to delete active job", "job", job.Name)
					return ctrl.Result{}, err
				}
			}
		case scanv1alpha1.AllowConcurrent:
			log.Info("Concurrency policy is Allow, creating additional job")
		}
	}

	// Create the job if needed
	if shouldCreateJob {
		job, err := r.constructJobForClusterScan(&clusterScan)
		if err != nil {
			log.Error(err, "Failed to construct job")
			return ctrl.Result{}, err
		}

		if err := r.Create(ctx, job); err != nil {
			log.Error(err, "Failed to create Job")
			return ctrl.Result{}, err
		}

		log.Info("Created scan job", "job", job.Name)
		now := metav1.NewTime(r.Clock.Now())
		clusterScan.Status.LastScheduleTime = &now
	}

	// Calculate next schedule time for scheduled scans
	if clusterScan.Spec.Schedule != "" {
		nextTime, err := r.getNextScheduleTime(&clusterScan)
		if err != nil {
			log.Error(err, "Failed to parse schedule")
		} else {
			clusterScan.Status.NextScheduleTime = &metav1.Time{Time: nextTime}
			// Requeue at next schedule time
			if requeueAfter == 0 || time.Until(nextTime) < requeueAfter {
				requeueAfter = time.Until(nextTime)
			}
		}
	}

	// Update status
	if err := r.Status().Update(ctx, &clusterScan); err != nil {
		log.Error(err, "Failed to update ClusterScan status")
		return ctrl.Result{}, err
	}

	// Requeue if we have a scheduled time
	if requeueAfter > 0 {
		return ctrl.Result{RequeueAfter: requeueAfter}, nil
	}

	return ctrl.Result{}, nil
}

// determinePhase determines the current phase based on job states
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
	allJobs := append(successfulJobs, failedJobs...)
	if len(allJobs) == 0 {
		return scanv1alpha1.ScanPhasePending
	}

	sortJobsByStartTime(allJobs)
	lastJob := allJobs[len(allJobs)-1]
	finished, finishedType := isJobFinished(lastJob)
	if finished {
		if finishedType == batchv1.JobComplete {
			return scanv1alpha1.ScanPhaseCompleted
		}
		return scanv1alpha1.ScanPhaseFailed
	}

	return scanv1alpha1.ScanPhaseRunning
}

// shouldCreateJob determines if a new job should be created
func (r *ClusterScanReconciler) shouldCreateJob(
	ctx context.Context,
	scan *scanv1alpha1.ClusterScan,
	activeJobs, successfulJobs []*batchv1.Job,
) (bool, time.Duration) {
	log := logf.FromContext(ctx)

	// Check if suspended
	if scan.Spec.Suspend != nil && *scan.Spec.Suspend {
		log.V(1).Info("ClusterScan is suspended, skipping job creation")
		return false, 0
	}

	// One-off scan (no schedule)
	if scan.Spec.Schedule == "" {
		// Only create if no jobs have been created yet
		if scan.Status.LastScheduleTime == nil && len(activeJobs) == 0 && len(successfulJobs) == 0 {
			return true, 0
		}
		return false, 0
	}

	// Scheduled scan - check if it's time to run
	return r.isScheduledTimeReached(ctx, scan)
}

// isScheduledTimeReached checks if the scheduled time has been reached
func (r *ClusterScanReconciler) isScheduledTimeReached(
	ctx context.Context,
	scan *scanv1alpha1.ClusterScan,
) (bool, time.Duration) {
	log := logf.FromContext(ctx)

	sched, err := cron.ParseStandard(scan.Spec.Schedule)
	if err != nil {
		log.Error(err, "Failed to parse schedule")
		return false, 0
	}

	now := r.Clock.Now()
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
			log.Info("Missed starting deadline", "deadline", deadline)
			// We missed this schedule, find the next one
			nextSchedule = sched.Next(now)
		}
	}

	if now.After(nextSchedule) || now.Equal(nextSchedule) {
		return true, 0
	}

	return false, time.Until(nextSchedule)
}

// getNextScheduleTime returns the next scheduled time
func (r *ClusterScanReconciler) getNextScheduleTime(scan *scanv1alpha1.ClusterScan) (time.Time, error) {
	sched, err := cron.ParseStandard(scan.Spec.Schedule)
	if err != nil {
		return time.Time{}, err
	}

	now := r.Clock.Now()
	return sched.Next(now), nil
}

// constructJobForClusterScan creates a Job from the ClusterScan spec
func (r *ClusterScanReconciler) constructJobForClusterScan(scan *scanv1alpha1.ClusterScan) (*batchv1.Job, error) {
	// Generate a unique name for the job
	name := fmt.Sprintf("%s-%d", scan.Name, time.Now().Unix())

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: scan.Namespace,
			Labels: map[string]string{
				scanJobLabel: scan.Name,
			},
			Annotations: map[string]string{
				scheduledTimeAnnotation: r.Clock.Now().Format(time.RFC3339),
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

// cleanupOldJobs removes old jobs based on history limits
func (r *ClusterScanReconciler) cleanupOldJobs(
	ctx context.Context,
	scan *scanv1alpha1.ClusterScan,
	successfulJobs, failedJobs []*batchv1.Job,
) error {
	log := logf.FromContext(ctx)

	// Default limits
	successLimit := int32(3)
	failLimit := int32(1)

	if scan.Spec.SuccessfulScansHistoryLimit != nil {
		successLimit = *scan.Spec.SuccessfulScansHistoryLimit
	}
	if scan.Spec.FailedScansHistoryLimit != nil {
		failLimit = *scan.Spec.FailedScansHistoryLimit
	}

	// Clean up successful jobs
	for i := 0; i < len(successfulJobs)-int(successLimit); i++ {
		job := successfulJobs[i]
		log.V(1).Info("Deleting old successful job", "job", job.Name)
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	// Clean up failed jobs
	for i := 0; i < len(failedJobs)-int(failLimit); i++ {
		job := failedJobs[i]
		log.V(1).Info("Deleting old failed job", "job", job.Name)
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// isJobFinished checks if a job is finished and returns the condition type
func isJobFinished(job *batchv1.Job) (bool, batchv1.JobConditionType) {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true, c.Type
		}
	}
	return false, ""
}

// sortJobsByStartTime sorts jobs by their start time (oldest first)
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

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterScanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Index jobs by owner for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &batchv1.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		job := rawObj.(*batchv1.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		if owner.APIVersion != scanv1alpha1.GroupVersion.String() || owner.Kind != "ClusterScan" {
			return nil
		}
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&scanv1alpha1.ClusterScan{}).
		Owns(&batchv1.Job{}).
		Named("clusterscan").
		Complete(r)
}

// Helper to get a pointer to a value
func getPointer[T any](v T) *T {
	return &v
}

// Ensure ClusterScan implements the necessary interface for type checking
var _ types.NamespacedName
