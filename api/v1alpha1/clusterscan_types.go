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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConcurrencyPolicy describes how the controller will handle overlapping scan jobs.
// +kubebuilder:validation:Enum=Allow;Forbid;Replace
type ConcurrencyPolicy string

const (
	// AllowConcurrent allows CronJobs to run concurrently.
	AllowConcurrent ConcurrencyPolicy = "Allow"
	// ForbidConcurrent forbids concurrent runs, skipping next run if previous hasn't finished yet.
	ForbidConcurrent ConcurrencyPolicy = "Forbid"
	// ReplaceConcurrent cancels currently running job and replaces it with a new one.
	ReplaceConcurrent ConcurrencyPolicy = "Replace"
)

// ClusterScanSpec defines the desired state of ClusterScan
type ClusterScanSpec struct {
	// schedule is a cron expression defining when to run the scan.
	// See https://en.wikipedia.org/wiki/Cron for cron syntax.
	// If not specified, the scan will run once immediately (one-off job).
	// +optional
	Schedule string `json:"schedule,omitempty"`

	// triggerNow, when set to true, triggers an immediate scan execution.
	// For scheduled scans, this runs an additional scan outside the schedule.
	// For one-off scans (no schedule), this can be used to re-trigger the scan.
	// The controller will reset this field to false after triggering.
	// +optional
	TriggerNow bool `json:"triggerNow,omitempty"`

	// suspend allows pausing the scheduled scans without deleting the ClusterScan.
	// Has no effect on one-off scans. Defaults to false.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// concurrencyPolicy specifies how to treat concurrent executions of a scan.
	// Valid values are:
	// - "Allow": allows concurrent scan jobs
	// - "Forbid" (default): skips new scan if previous is still running
	// - "Replace": cancels currently running scan and starts a new one
	// +optional
	// +kubebuilder:default=Forbid
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// historyLimit is the number of finished scan Jobs and status history entries to retain.
	// This controls both the Job resources in the cluster and the results in status.history.
	// Defaults to 5.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=5
	HistoryLimit *int32 `json:"historyLimit,omitempty"`

	// retainLogs determines whether to capture and store scan logs in status.
	// Logs are truncated to logsMaxBytes. Defaults to false.
	// +optional
	RetainLogs bool `json:"retainLogs,omitempty"`

	// logsMaxBytes is the maximum number of bytes of logs to retain per scan.
	// Only used when retainLogs is true. Defaults to 10000 (10KB).
	// +optional
	// +kubebuilder:validation:Minimum=100
	// +kubebuilder:validation:Maximum=100000
	// +kubebuilder:default=10000
	LogsMaxBytes *int32 `json:"logsMaxBytes,omitempty"`

	// startingDeadlineSeconds is the optional deadline in seconds for starting the scan
	// if it misses its scheduled time for any reason. Missed scans will be counted as failed.
	// Only applies to scheduled scans.
	// +optional
	// +kubebuilder:validation:Minimum=0
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// scanTemplate defines the scan job configuration.
	// +kubebuilder:validation:Required
	ScanTemplate ScanTemplate `json:"scanTemplate"`
}

// ScanTemplate defines the template for scan job execution.
type ScanTemplate struct {
	// image is the container image to use for the scan job.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Image string `json:"image"`

	// imagePullPolicy defines the pull policy for the scan image.
	// Defaults to IfNotPresent.
	// +optional
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	// +kubebuilder:default=IfNotPresent
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// imagePullSecrets is a list of references to secrets for pulling the scan image.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// command is the entrypoint array for the scan container.
	// Not executed within a shell. If not provided, the image's ENTRYPOINT is used.
	// +optional
	Command []string `json:"command,omitempty"`

	// args are the arguments to the entrypoint.
	// If not provided, the image's CMD is used.
	// +optional
	Args []string `json:"args,omitempty"`

	// env is a list of environment variables to set in the scan container.
	// +optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// envFrom is a list of sources to populate environment variables in the scan container.
	// +optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom,omitempty"`

	// resources defines the compute resource requirements for the scan container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// serviceAccountName is the name of the ServiceAccount to use for the scan job.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// nodeSelector is a selector which must be true for the scan pod to fit on a node.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// tolerations are the tolerations for the scan pod.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// affinity is the scheduling constraints for the scan pod.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// activeDeadlineSeconds is the duration in seconds the scan job may be active
	// before the system tries to terminate it.
	// +optional
	// +kubebuilder:validation:Minimum=1
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// backoffLimit is the number of retries before marking the scan as failed.
	// Defaults to 3.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// volumes is a list of volumes that can be mounted by containers in the scan pod.
	// +optional
	Volumes []corev1.Volume `json:"volumes,omitempty"`

	// volumeMounts are the volume mounts for the scan container.
	// +optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`

	// securityContext holds pod-level security attributes for the scan pod.
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty"`
}

// ScanPhase represents the current phase of a ClusterScan.
// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
type ScanPhase string

const (
	// ScanPhasePending indicates the scan has not yet started.
	ScanPhasePending ScanPhase = "Pending"
	// ScanPhaseRunning indicates the scan is currently executing.
	ScanPhaseRunning ScanPhase = "Running"
	// ScanPhaseCompleted indicates the scan finished successfully.
	ScanPhaseCompleted ScanPhase = "Completed"
	// ScanPhaseFailed indicates the scan failed.
	ScanPhaseFailed ScanPhase = "Failed"
)

// ClusterScanStatus defines the observed state of ClusterScan.
type ClusterScanStatus struct {
	// conditions represent the current state of the ClusterScan resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// phase represents the current phase of the ClusterScan.
	// For one-off scans: Pending -> Running -> Completed/Failed
	// For scheduled scans: typically Running (active schedule) or the phase of the last scan.
	// +optional
	Phase ScanPhase `json:"phase,omitempty"`

	// active is a list of pointers to currently running scan jobs.
	// +optional
	Active []corev1.ObjectReference `json:"active,omitempty"`

	// completedJobs is the total number of successfully completed scan jobs.
	// +optional
	CompletedJobs int32 `json:"completedJobs,omitempty"`

	// failedJobs is the total number of failed scan jobs.
	// +optional
	FailedJobs int32 `json:"failedJobs,omitempty"`

	// lastScheduleTime is the last time a scan was scheduled.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// lastSuccessfulTime is the last time a scan completed successfully.
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`

	// lastScanResult contains a summary of the most recent scan result.
	// +optional
	LastScanResult *ScanResult `json:"lastScanResult,omitempty"`

	// history contains the results of previous scan executions.
	// The number of entries is limited by spec.resultsHistoryLimit.
	// Ordered from oldest to newest.
	// +optional
	History []ScanResult `json:"history,omitempty"`

	// nextScheduleTime is the next time a scan is scheduled to run.
	// Only set for scheduled scans.
	// +optional
	NextScheduleTime *metav1.Time `json:"nextScheduleTime,omitempty"`

	// lastTriggeredTime is the last time a scan was manually triggered via triggerNow.
	// +optional
	LastTriggeredTime *metav1.Time `json:"lastTriggeredTime,omitempty"`
}

// ScanResult contains the summary of a scan execution.
type ScanResult struct {
	// jobName is the name of the Job that produced this result.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// startTime is when the scan started.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// completionTime is when the scan finished.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// succeeded indicates whether the scan completed successfully.
	// +optional
	Succeeded bool `json:"succeeded,omitempty"`

	// message is a human-readable message about the scan result.
	// +optional
	Message string `json:"message,omitempty"`

	// findings is the number of issues found during the scan.
	// +optional
	Findings *int32 `json:"findings,omitempty"`

	// logs contains the captured output from the scan container.
	// Only populated when spec.retainLogs is true.
	// May be truncated to spec.logsMaxBytes.
	// +optional
	Logs string `json:"logs,omitempty"`

	// logsTruncated indicates whether the logs were truncated due to size limits.
	// +optional
	LogsTruncated bool `json:"logsTruncated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Schedule",type="string",JSONPath=".spec.schedule",description="Cron schedule (empty for one-off)"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Current phase"
// +kubebuilder:printcolumn:name="Last Success",type="date",JSONPath=".status.lastSuccessfulTime",description="Last successful scan time"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// ClusterScan is the Schema for the clusterscans API
type ClusterScan struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of ClusterScan
	// +required
	Spec ClusterScanSpec `json:"spec"`

	// status defines the observed state of ClusterScan
	// +optional
	Status ClusterScanStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ClusterScanList contains a list of ClusterScan
type ClusterScanList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ClusterScan `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterScan{}, &ClusterScanList{})
}
