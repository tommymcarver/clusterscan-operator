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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	scanv1alpha1 "github.com/tommymcarver/clusterscan-operator/api/v1alpha1"
)

const fiveMinuteCronSchedule = "*/5 * * * *"

// parseQuantity is a helper to parse resource quantities in tests
func parseQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

// mockClock implements Clock interface for deterministic testing
type mockClock struct {
	currentTime time.Time
}

func (m *mockClock) Now() time.Time {
	return m.currentTime
}

func (m *mockClock) Advance(d time.Duration) {
	m.currentTime = m.currentTime.Add(d)
}

// Helper to create a test ClusterScan
func newTestClusterScan(name, namespace string) *scanv1alpha1.ClusterScan {
	return &scanv1alpha1.ClusterScan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: scanv1alpha1.ClusterScanSpec{
			ScanTemplate: scanv1alpha1.ScanTemplate{
				Image:   "busybox:latest",
				Command: []string{"/bin/sh", "-c"},
				Args:    []string{"echo 'test scan'"},
			},
		},
	}
}

var _ = Describe("ClusterScan Controller", func() {

	// ============================================================
	// Unit Tests for Helper Functions (Pure functions, no envtest needed)
	// ============================================================

	Describe("isJobFinished", func() {
		It("should return false for a running job", func() {
			job := &batchv1.Job{
				Status: batchv1.JobStatus{
					Active: 1,
				},
			}
			finished, condType := isJobFinished(job)
			Expect(finished).To(BeFalse())
			Expect(condType).To(BeEmpty())
		})

		It("should return true with JobComplete for a successful job", func() {
			job := &batchv1.Job{
				Status: batchv1.JobStatus{
					Succeeded: 1,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
					},
				},
			}
			finished, condType := isJobFinished(job)
			Expect(finished).To(BeTrue())
			Expect(condType).To(Equal(batchv1.JobComplete))
		})

		It("should return true with JobFailed for a failed job", func() {
			job := &batchv1.Job{
				Status: batchv1.JobStatus{
					Failed: 1,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
					},
				},
			}
			finished, condType := isJobFinished(job)
			Expect(finished).To(BeTrue())
			Expect(condType).To(Equal(batchv1.JobFailed))
		})

		It("should return false when condition status is False", func() {
			job := &batchv1.Job{
				Status: batchv1.JobStatus{
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobComplete, Status: corev1.ConditionFalse},
					},
				},
			}
			finished, condType := isJobFinished(job)
			Expect(finished).To(BeFalse())
			Expect(condType).To(BeEmpty())
		})
	})

	Describe("sortJobsByStartTime", func() {
		It("should sort jobs by start time (oldest first)", func() {
			now := time.Now()
			jobs := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "job3"}, Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: now.Add(2 * time.Hour)}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "job1"}, Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: now}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "job2"}, Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: now.Add(1 * time.Hour)}}},
			}

			sortJobsByStartTime(jobs)

			Expect(jobs[0].Name).To(Equal("job1"))
			Expect(jobs[1].Name).To(Equal("job2"))
			Expect(jobs[2].Name).To(Equal("job3"))
		})

		It("should put nil start times at the front", func() {
			now := time.Now()
			jobs := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "job1"}, Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: now}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "job2"}, Status: batchv1.JobStatus{StartTime: nil}},
			}

			sortJobsByStartTime(jobs)

			Expect(jobs[0].Name).To(Equal("job2")) // nil first
			Expect(jobs[1].Name).To(Equal("job1"))
		})

		It("should handle empty slice", func() {
			jobs := []*batchv1.Job{}
			Expect(func() { sortJobsByStartTime(jobs) }).NotTo(Panic())
		})

		It("should handle single element", func() {
			jobs := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "job1"}},
			}
			sortJobsByStartTime(jobs)
			Expect(jobs[0].Name).To(Equal("job1"))
		})
	})

	Describe("mergeSortedJobs", func() {
		It("should return empty slice for empty inputs", func() {
			result := mergeSortedJobs(nil, nil)
			Expect(result).To(BeEmpty())
		})

		It("should return the non-empty slice when one is empty", func() {
			now := time.Now()
			jobs := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "job1"}, Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: now}}},
			}

			result := mergeSortedJobs(jobs, nil)
			Expect(result).To(HaveLen(1))
			Expect(result[0].Name).To(Equal("job1"))

			result = mergeSortedJobs(nil, jobs)
			Expect(result).To(HaveLen(1))
			Expect(result[0].Name).To(Equal("job1"))
		})

		It("should merge sorted slices correctly", func() {
			now := time.Now()
			sliceA := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "a1"}, Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: now}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "a2"}, Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: now.Add(2 * time.Hour)}}},
			}
			sliceB := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "b1"}, Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: now.Add(1 * time.Hour)}}},
				{ObjectMeta: metav1.ObjectMeta{Name: "b2"}, Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: now.Add(3 * time.Hour)}}},
			}

			result := mergeSortedJobs(sliceA, sliceB)

			Expect(result).To(HaveLen(4))
			Expect(result[0].Name).To(Equal("a1")) // now
			Expect(result[1].Name).To(Equal("b1")) // now + 1h
			Expect(result[2].Name).To(Equal("a2")) // now + 2h
			Expect(result[3].Name).To(Equal("b2")) // now + 3h
		})

		It("should handle nil start times in merge", func() {
			now := time.Now()
			sliceA := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "a1"}, Status: batchv1.JobStatus{StartTime: nil}},
			}
			sliceB := []*batchv1.Job{
				{ObjectMeta: metav1.ObjectMeta{Name: "b1"}, Status: batchv1.JobStatus{StartTime: &metav1.Time{Time: now}}},
			}

			result := mergeSortedJobs(sliceA, sliceB)

			Expect(result).To(HaveLen(2))
			Expect(result[0].Name).To(Equal("a1")) // nil comes first
			Expect(result[1].Name).To(Equal("b1"))
		})
	})

	Describe("sortHistoryByCompletionTime", func() {
		It("should sort history by completion time (oldest first)", func() {
			now := time.Now()
			history := []scanv1alpha1.ScanResult{
				{JobName: "job3", CompletionTime: &metav1.Time{Time: now.Add(2 * time.Hour)}},
				{JobName: "job1", CompletionTime: &metav1.Time{Time: now}},
				{JobName: "job2", CompletionTime: &metav1.Time{Time: now.Add(1 * time.Hour)}},
			}

			sortHistoryByCompletionTime(history)

			Expect(history[0].JobName).To(Equal("job1"))
			Expect(history[1].JobName).To(Equal("job2"))
			Expect(history[2].JobName).To(Equal("job3"))
		})

		It("should put nil completion times at the front", func() {
			now := time.Now()
			history := []scanv1alpha1.ScanResult{
				{JobName: "job1", CompletionTime: &metav1.Time{Time: now}},
				{JobName: "job2", CompletionTime: nil},
			}

			sortHistoryByCompletionTime(history)

			Expect(history[0].JobName).To(Equal("job2"))
			Expect(history[1].JobName).To(Equal("job1"))
		})

		It("should handle empty slice", func() {
			history := []scanv1alpha1.ScanResult{}
			Expect(func() { sortHistoryByCompletionTime(history) }).NotTo(Panic())
		})
	})

	Describe("parseInt32", func() {
		It("should parse valid positive integers", func() {
			val, err := parseInt32("42")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int32(42)))
		})

		It("should parse zero", func() {
			val, err := parseInt32("0")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int32(0)))
		})

		It("should parse negative integers", func() {
			val, err := parseInt32("-5")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int32(-5)))
		})

		It("should return error for invalid input", func() {
			_, err := parseInt32("not-a-number")
			Expect(err).To(HaveOccurred())
		})

		It("should return error for empty string", func() {
			_, err := parseInt32("")
			Expect(err).To(HaveOccurred())
		})

		It("should parse integer part of floating point", func() {
			// Sscanf with %d stops at non-digit characters
			val, err := parseInt32("3.14")
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(int32(3)))
		})
	})

	Describe("TriggerReason Constants", func() {
		It("should have distinct values", func() {
			reasons := []TriggerReason{
				TriggerReasonNone,
				TriggerReasonOneOff,
				TriggerReasonSchedule,
				TriggerReasonTriggerNow,
				TriggerReasonAnnotation,
			}

			// Check all are distinct
			seen := make(map[TriggerReason]bool)
			for _, r := range reasons {
				Expect(seen[r]).To(BeFalse(), "Duplicate trigger reason: %s", r)
				seen[r] = true
			}
		})

		It("should have TriggerReasonNone as empty string", func() {
			Expect(string(TriggerReasonNone)).To(BeEmpty())
		})
	})

	// ============================================================
	// determinePhase Tests (Unit tests - no envtest needed)
	// ============================================================

	Describe("determinePhase", func() {
		var reconciler *ClusterScanReconciler

		BeforeEach(func() {
			reconciler = &ClusterScanReconciler{}
		})

		Context("One-off scans (no schedule)", func() {
			It("should return Pending for new scan with no jobs", func() {
				scan := newTestClusterScan("test", "default")
				phase := reconciler.determinePhase(scan, nil, nil, nil)
				Expect(phase).To(Equal(scanv1alpha1.ScanPhasePending))
			})

			It("should return Running when active jobs exist", func() {
				scan := newTestClusterScan("test", "default")
				activeJob := &batchv1.Job{Status: batchv1.JobStatus{Active: 1}}
				phase := reconciler.determinePhase(scan, []*batchv1.Job{activeJob}, nil, nil)
				Expect(phase).To(Equal(scanv1alpha1.ScanPhaseRunning))
			})

			It("should return Completed for one-off scan with successful job", func() {
				scan := newTestClusterScan("test", "default")
				successfulJob := &batchv1.Job{
					Status: batchv1.JobStatus{
						Succeeded:  1,
						Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
					},
				}
				phase := reconciler.determinePhase(scan, nil, []*batchv1.Job{successfulJob}, nil)
				Expect(phase).To(Equal(scanv1alpha1.ScanPhaseCompleted))
			})

			It("should return Failed for one-off scan with failed job", func() {
				scan := newTestClusterScan("test", "default")
				failedJob := &batchv1.Job{
					Status: batchv1.JobStatus{
						Failed:     1,
						Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}},
					},
				}
				phase := reconciler.determinePhase(scan, nil, nil, []*batchv1.Job{failedJob})
				Expect(phase).To(Equal(scanv1alpha1.ScanPhaseFailed))
			})
		})

		Context("Scheduled scans", func() {
			It("should return Pending for scheduled scan with no jobs", func() {
				scan := newTestClusterScan("test", "default")
				scan.Spec.Schedule = fiveMinuteCronSchedule
				phase := reconciler.determinePhase(scan, nil, nil, nil)
				Expect(phase).To(Equal(scanv1alpha1.ScanPhasePending))
			})

			It("should return Running when active jobs exist", func() {
				scan := newTestClusterScan("test", "default")
				scan.Spec.Schedule = fiveMinuteCronSchedule
				activeJob := &batchv1.Job{Status: batchv1.JobStatus{Active: 1}}
				phase := reconciler.determinePhase(scan, []*batchv1.Job{activeJob}, nil, nil)
				Expect(phase).To(Equal(scanv1alpha1.ScanPhaseRunning))
			})

			It("should return Completed when last job succeeded", func() {
				scan := newTestClusterScan("test", "default")
				scan.Spec.Schedule = fiveMinuteCronSchedule
				now := time.Now()
				successfulJob := &batchv1.Job{
					Status: batchv1.JobStatus{
						StartTime:  &metav1.Time{Time: now},
						Succeeded:  1,
						Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
					},
				}
				phase := reconciler.determinePhase(scan, nil, []*batchv1.Job{successfulJob}, nil)
				Expect(phase).To(Equal(scanv1alpha1.ScanPhaseCompleted))
			})

			It("should return Failed when last job failed", func() {
				scan := newTestClusterScan("test", "default")
				scan.Spec.Schedule = fiveMinuteCronSchedule
				now := time.Now()
				failedJob := &batchv1.Job{
					Status: batchv1.JobStatus{
						StartTime:  &metav1.Time{Time: now},
						Failed:     1,
						Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}},
					},
				}
				phase := reconciler.determinePhase(scan, nil, nil, []*batchv1.Job{failedJob})
				Expect(phase).To(Equal(scanv1alpha1.ScanPhaseFailed))
			})
		})
	})

	// ============================================================
	// shouldCreateJob Tests (Unit tests with mocked clock)
	// ============================================================

	Describe("shouldCreateJob", func() {
		var (
			reconciler *ClusterScanReconciler
			clock      *mockClock
		)

		BeforeEach(func() {
			clock = &mockClock{currentTime: time.Date(2025, 1, 1, 0, 5, 0, 0, time.UTC)}
			reconciler = &ClusterScanReconciler{
				Clock: clock,
			}
		})

		It("should return TriggerReasonNone when suspended", func() {
			scan := newTestClusterScan("test", "default")
			scan.Spec.Suspend = ptr.To(true)
			scan.Spec.TriggerNow = true // Should be ignored

			result := reconciler.shouldCreateJob(context.Background(), scan, nil, nil)
			Expect(result.reason).To(Equal(TriggerReasonNone))
		})

		It("should return TriggerReasonTriggerNow when triggerNow is set", func() {
			scan := newTestClusterScan("test", "default")
			scan.Spec.TriggerNow = true

			result := reconciler.shouldCreateJob(context.Background(), scan, nil, nil)
			Expect(result.reason).To(Equal(TriggerReasonTriggerNow))
		})

		It("should return TriggerReasonAnnotation when trigger annotation is set", func() {
			scan := newTestClusterScan("test", "default")
			scan.Annotations = map[string]string{
				triggerAnnotation: "12345",
			}

			result := reconciler.shouldCreateJob(context.Background(), scan, nil, nil)
			Expect(result.reason).To(Equal(TriggerReasonAnnotation))
		})

		It("should return TriggerReasonOneOff when annotation already processed for new one-off scan", func() {
			scan := newTestClusterScan("test", "default")
			scan.Annotations = map[string]string{
				triggerAnnotation:     "12345",
				lastTriggerAnnotation: "12345", // Same value = already processed
			}
			// No schedule, no LastScheduleTime = one-off scan that hasn't run yet

			result := reconciler.shouldCreateJob(context.Background(), scan, nil, nil)
			// Since trigger annotation is same, it falls through to one-off check
			// and since the scan hasn't run yet (no LastScheduleTime), it returns OneOff
			Expect(result.reason).To(Equal(TriggerReasonOneOff))
		})

		It("should return TriggerReasonNone when annotation already processed and scan already ran", func() {
			scan := newTestClusterScan("test", "default")
			scan.Annotations = map[string]string{
				triggerAnnotation:     "12345",
				lastTriggerAnnotation: "12345", // Same value = already processed
			}
			// Mark as already run
			now := metav1.NewTime(time.Now())
			scan.Status.LastScheduleTime = &now

			result := reconciler.shouldCreateJob(context.Background(), scan, nil, nil)
			Expect(result.reason).To(Equal(TriggerReasonNone))
		})

		It("should return TriggerReasonOneOff for new one-off scan", func() {
			scan := newTestClusterScan("test", "default")
			// No schedule, no LastScheduleTime, no jobs

			result := reconciler.shouldCreateJob(context.Background(), scan, nil, nil)
			Expect(result.reason).To(Equal(TriggerReasonOneOff))
		})

		It("should return TriggerReasonNone for already-run one-off scan", func() {
			scan := newTestClusterScan("test", "default")
			now := metav1.NewTime(time.Now())
			scan.Status.LastScheduleTime = &now

			result := reconciler.shouldCreateJob(context.Background(), scan, nil, nil)
			Expect(result.reason).To(Equal(TriggerReasonNone))
		})

		It("should return TriggerReasonSchedule when schedule time reached", func() {
			scan := newTestClusterScan("test", "default")
			scan.Spec.Schedule = fiveMinuteCronSchedule // Every 5 minutes
			// Clock is set to 00:05:00, which is a 5-minute boundary
			scan.CreationTimestamp = metav1.Time{Time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}

			result := reconciler.shouldCreateJob(context.Background(), scan, nil, nil)
			Expect(result.reason).To(Equal(TriggerReasonSchedule))
		})

		It("should return TriggerReasonNone for scheduled scan not yet due", func() {
			// Set deterministic test time
			clock.currentTime = time.Date(2025, 1, 1, 0, 3, 0, 0, time.UTC) // 00:03

			scan := newTestClusterScan("test", "default")
			scan.Spec.Schedule = fiveMinuteCronSchedule // Every 5 minutes
			// Created at 00:00, last schedule at 00:00, clock at 00:03 - next is 00:05
			scan.CreationTimestamp = metav1.Time{Time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
			scan.Status.LastScheduleTime = &metav1.Time{Time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}

			result := reconciler.shouldCreateJob(context.Background(), scan, nil, nil)
			Expect(result.reason).To(Equal(TriggerReasonNone))
		})

		It("should prioritize triggerNow over schedule", func() {
			scan := newTestClusterScan("test", "default")
			scan.Spec.Schedule = "0 0 * * *" // Daily at midnight (not due)
			scan.Spec.TriggerNow = true
			scan.CreationTimestamp = metav1.Time{Time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
			clock.currentTime = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC) // Noon

			result := reconciler.shouldCreateJob(context.Background(), scan, nil, nil)
			Expect(result.reason).To(Equal(TriggerReasonTriggerNow))
		})
	})

	// ============================================================
	// constructJobForClusterScan Tests
	// ============================================================

	Describe("constructJobForClusterScan", func() {
		var reconciler *ClusterScanReconciler

		BeforeEach(func() {
			reconciler = &ClusterScanReconciler{
				Scheme: k8sClient.Scheme(),
				Clock:  &mockClock{currentTime: time.Now()},
			}
		})

		It("should create a job with correct name prefix", func() {
			scan := newTestClusterScan("my-scan", "default")
			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(job.Name).To(HavePrefix("my-scan-"))
		})

		It("should set correct labels", func() {
			scan := newTestClusterScan("label-test", "default")
			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(job.Labels[scanJobLabel]).To(Equal("label-test"))
			Expect(job.Spec.Template.Labels[scanJobLabel]).To(Equal("label-test"))
		})

		It("should set scheduled-at annotation", func() {
			scan := newTestClusterScan("annotation-test", "default")
			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(job.Annotations).To(HaveKey(scheduledTimeAnnotation))
		})

		It("should copy image and command from scan template", func() {
			scan := newTestClusterScan("template-test", "default")
			scan.Spec.ScanTemplate.Image = "custom-image:v1"
			scan.Spec.ScanTemplate.Command = []string{"/bin/custom"}
			scan.Spec.ScanTemplate.Args = []string{"--arg1", "--arg2"}

			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.Template.Spec.Containers).To(HaveLen(1))
			container := job.Spec.Template.Spec.Containers[0]
			Expect(container.Name).To(Equal("scan"))
			Expect(container.Image).To(Equal("custom-image:v1"))
			Expect(container.Command).To(Equal([]string{"/bin/custom"}))
			Expect(container.Args).To(Equal([]string{"--arg1", "--arg2"}))
		})

		It("should copy resources from scan template", func() {
			scan := newTestClusterScan("resources-test", "default")
			scan.Spec.ScanTemplate.Resources = corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    *parseQuantity("100m"),
					corev1.ResourceMemory: *parseQuantity("128Mi"),
				},
			}

			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			container := job.Spec.Template.Spec.Containers[0]
			Expect(container.Resources.Requests.Cpu().String()).To(Equal("100m"))
			Expect(container.Resources.Requests.Memory().String()).To(Equal("128Mi"))
		})

		It("should set default backoff limit if not specified", func() {
			scan := newTestClusterScan("backoff-default", "default")
			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.BackoffLimit).NotTo(BeNil())
			Expect(*job.Spec.BackoffLimit).To(Equal(int32(3)))
		})

		It("should use custom backoff limit if specified", func() {
			scan := newTestClusterScan("backoff-custom", "default")
			scan.Spec.ScanTemplate.BackoffLimit = ptr.To(int32(5))

			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(*job.Spec.BackoffLimit).To(Equal(int32(5)))
		})

		It("should copy service account name", func() {
			scan := newTestClusterScan("sa-test", "default")
			scan.Spec.ScanTemplate.ServiceAccountName = "custom-sa"

			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.Template.Spec.ServiceAccountName).To(Equal("custom-sa"))
		})

		It("should set RestartPolicy to Never", func() {
			scan := newTestClusterScan("restart-test", "default")
			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
		})

		It("should set active deadline seconds", func() {
			scan := newTestClusterScan("deadline-test", "default")
			scan.Spec.ScanTemplate.ActiveDeadlineSeconds = ptr.To(int64(300))

			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(*job.Spec.ActiveDeadlineSeconds).To(Equal(int64(300)))
		})

		It("should copy node selector", func() {
			scan := newTestClusterScan("nodeselector-test", "default")
			scan.Spec.ScanTemplate.NodeSelector = map[string]string{
				"disktype": "ssd",
			}

			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.Template.Spec.NodeSelector["disktype"]).To(Equal("ssd"))
		})

		It("should copy tolerations", func() {
			scan := newTestClusterScan("tolerations-test", "default")
			scan.Spec.ScanTemplate.Tolerations = []corev1.Toleration{
				{Key: "key1", Operator: corev1.TolerationOpEqual, Value: "value1"},
			}

			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.Template.Spec.Tolerations).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.Tolerations[0].Key).To(Equal("key1"))
		})

		It("should copy volumes and volume mounts", func() {
			scan := newTestClusterScan("volumes-test", "default")
			scan.Spec.ScanTemplate.Volumes = []corev1.Volume{
				{Name: "config", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			}
			scan.Spec.ScanTemplate.VolumeMounts = []corev1.VolumeMount{
				{Name: "config", MountPath: "/config"},
			}

			job, err := reconciler.constructJobForClusterScan(scan)

			Expect(err).NotTo(HaveOccurred())
			Expect(job.Spec.Template.Spec.Volumes).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.Containers[0].VolumeMounts).To(HaveLen(1))
		})
	})

	// ============================================================
	// jobToScanResult Tests
	// ============================================================

	Describe("jobToScanResult", func() {
		var reconciler *ClusterScanReconciler

		BeforeEach(func() {
			reconciler = &ClusterScanReconciler{
				Clock: &mockClock{currentTime: time.Now()},
			}
		})

		It("should return nil for unfinished job", func() {
			scan := newTestClusterScan("test", "default")
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "test-job"},
				Status:     batchv1.JobStatus{Active: 1},
			}

			result := reconciler.jobToScanResult(context.Background(), scan, job)
			Expect(result).To(BeNil())
		})

		It("should create result for successful job", func() {
			scan := newTestClusterScan("test", "default")
			now := time.Now()
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "success-job"},
				Status: batchv1.JobStatus{
					StartTime:      &metav1.Time{Time: now.Add(-1 * time.Minute)},
					CompletionTime: &metav1.Time{Time: now},
					Succeeded:      1,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
					},
				},
			}

			result := reconciler.jobToScanResult(context.Background(), scan, job)

			Expect(result).NotTo(BeNil())
			Expect(result.JobName).To(Equal("success-job"))
			Expect(result.Succeeded).To(BeTrue())
			Expect(result.Message).To(Equal("Scan completed successfully"))
			Expect(result.StartTime).NotTo(BeNil())
			Expect(result.CompletionTime).NotTo(BeNil())
		})

		It("should create result for failed job", func() {
			scan := newTestClusterScan("test", "default")
			now := time.Now()
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "failed-job"},
				Status: batchv1.JobStatus{
					StartTime:      &metav1.Time{Time: now.Add(-1 * time.Minute)},
					CompletionTime: &metav1.Time{Time: now},
					Failed:         1,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
					},
				},
			}

			result := reconciler.jobToScanResult(context.Background(), scan, job)

			Expect(result).NotTo(BeNil())
			Expect(result.JobName).To(Equal("failed-job"))
			Expect(result.Succeeded).To(BeFalse())
			Expect(result.Message).To(ContainSubstring("failed"))
		})

		It("should parse findings annotation", func() {
			scan := newTestClusterScan("test", "default")
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: "findings-job",
					Annotations: map[string]string{
						"scan.spectrocloud.com/findings": "42",
					},
				},
				Status: batchv1.JobStatus{
					Succeeded: 1,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
					},
				},
			}

			result := reconciler.jobToScanResult(context.Background(), scan, job)

			Expect(result).NotTo(BeNil())
			Expect(result.Findings).NotTo(BeNil())
			Expect(*result.Findings).To(Equal(int32(42)))
		})

		It("should handle missing findings annotation", func() {
			scan := newTestClusterScan("test", "default")
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "no-findings-job"},
				Status: batchv1.JobStatus{
					Succeeded: 1,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
					},
				},
			}

			result := reconciler.jobToScanResult(context.Background(), scan, job)

			Expect(result).NotTo(BeNil())
			Expect(result.Findings).To(BeNil())
		})
	})

	// ============================================================
	// isScheduledTimeReached Tests
	// ============================================================

	Describe("isScheduledTimeReached", func() {
		var (
			reconciler *ClusterScanReconciler
			clock      *mockClock
		)

		BeforeEach(func() {
			clock = &mockClock{currentTime: time.Date(2025, 1, 1, 0, 5, 0, 0, time.UTC)}
			reconciler = &ClusterScanReconciler{
				Clock: clock,
			}
		})

		It("should return reached=true and next schedule time when schedule time is reached", func() {
			scan := newTestClusterScan("test", "default")
			scan.Spec.Schedule = fiveMinuteCronSchedule
			// Set creation time to 00:00, clock is at 00:05
			scan.CreationTimestamp = metav1.Time{Time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}

			info := reconciler.isScheduledTimeReached(context.Background(), scan)
			Expect(info.reached).To(BeTrue())
			// Next schedule after 00:05 should be 00:10
			Expect(info.nextScheduleTime).To(Equal(time.Date(2025, 1, 1, 0, 10, 0, 0, time.UTC)))
			Expect(info.waitDuration).To(Equal(5 * time.Minute))
		})

		It("should return reached=false with correct next time when not yet due", func() {
			clock.currentTime = time.Date(2025, 1, 1, 0, 3, 0, 0, time.UTC) // 00:03
			scan := newTestClusterScan("test", "default")
			scan.Spec.Schedule = fiveMinuteCronSchedule
			// Created at 00:00, clock at 00:03, next should be 00:05
			scan.CreationTimestamp = metav1.Time{Time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}

			info := reconciler.isScheduledTimeReached(context.Background(), scan)
			Expect(info.reached).To(BeFalse())
			Expect(info.nextScheduleTime).To(Equal(time.Date(2025, 1, 1, 0, 5, 0, 0, time.UTC)))
			Expect(info.waitDuration).To(Equal(2 * time.Minute))
		})

		It("should handle hourly schedule", func() {
			clock.currentTime = time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC) // 00:30
			scan := newTestClusterScan("test", "default")
			scan.Spec.Schedule = "0 * * * *" // Every hour at minute 0
			scan.CreationTimestamp = metav1.Time{Time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}

			info := reconciler.isScheduledTimeReached(context.Background(), scan)
			Expect(info.reached).To(BeFalse())
			Expect(info.nextScheduleTime).To(Equal(time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)))
			Expect(info.waitDuration).To(Equal(30 * time.Minute))
		})

		It("should handle daily schedule", func() {
			clock.currentTime = time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC) // Noon
			scan := newTestClusterScan("test", "default")
			scan.Spec.Schedule = "0 0 * * *" // Daily at midnight
			scan.CreationTimestamp = metav1.Time{Time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}

			info := reconciler.isScheduledTimeReached(context.Background(), scan)
			Expect(info.reached).To(BeFalse())
			// Next midnight is Jan 2
			Expect(info.nextScheduleTime).To(Equal(time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)))
		})

		It("should handle starting deadline seconds", func() {
			scan := newTestClusterScan("test", "default")
			scan.Spec.Schedule = fiveMinuteCronSchedule
			scan.Spec.StartingDeadlineSeconds = ptr.To(int64(60)) // 1 minute deadline
			scan.CreationTimestamp = metav1.Time{Time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}

			info := reconciler.isScheduledTimeReached(context.Background(), scan)
			// We just verify it doesn't panic and returns valid values
			Expect(info.reached).To(BeElementOf(true, false))
		})

		It("should return zero values for invalid schedule", func() {
			scan := newTestClusterScan("test", "default")
			scan.Spec.Schedule = "not-a-cron"
			scan.CreationTimestamp = metav1.Time{Time: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}

			info := reconciler.isScheduledTimeReached(context.Background(), scan)
			Expect(info.reached).To(BeFalse())
			Expect(info.waitDuration).To(Equal(time.Duration(0)))
			Expect(info.nextScheduleTime.IsZero()).To(BeTrue())
		})
	})

	// ============================================================
	// updateScanHistory Tests
	// ============================================================

	Describe("updateScanHistory", func() {
		var reconciler *ClusterScanReconciler

		BeforeEach(func() {
			reconciler = &ClusterScanReconciler{
				Clock: &mockClock{currentTime: time.Now()},
			}
		})

		It("should not modify history for no finished jobs", func() {
			scan := newTestClusterScan("test", "default")
			scan.Status.History = []scanv1alpha1.ScanResult{}

			reconciler.updateScanHistory(context.Background(), scan, nil, nil)

			Expect(scan.Status.History).To(BeEmpty())
		})

		It("should add successful job to history", func() {
			scan := newTestClusterScan("test", "default")
			now := time.Now()
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "job1"},
				Status: batchv1.JobStatus{
					StartTime:      &metav1.Time{Time: now.Add(-1 * time.Minute)},
					CompletionTime: &metav1.Time{Time: now},
					Succeeded:      1,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
					},
				},
			}

			reconciler.updateScanHistory(context.Background(), scan, []*batchv1.Job{job}, nil)

			Expect(scan.Status.History).To(HaveLen(1))
			Expect(scan.Status.History[0].JobName).To(Equal("job1"))
			Expect(scan.Status.History[0].Succeeded).To(BeTrue())
		})

		It("should add failed job to history", func() {
			scan := newTestClusterScan("test", "default")
			now := time.Now()
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "failed-job"},
				Status: batchv1.JobStatus{
					StartTime:      &metav1.Time{Time: now.Add(-1 * time.Minute)},
					CompletionTime: &metav1.Time{Time: now},
					Failed:         1,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
					},
				},
			}

			reconciler.updateScanHistory(context.Background(), scan, nil, []*batchv1.Job{job})

			Expect(scan.Status.History).To(HaveLen(1))
			Expect(scan.Status.History[0].Succeeded).To(BeFalse())
		})

		It("should not duplicate existing history entries", func() {
			scan := newTestClusterScan("test", "default")
			now := time.Now()
			scan.Status.History = []scanv1alpha1.ScanResult{
				{JobName: "existing-job", Succeeded: true},
			}

			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{Name: "existing-job"},
				Status: batchv1.JobStatus{
					CompletionTime: &metav1.Time{Time: now},
					Succeeded:      1,
					Conditions: []batchv1.JobCondition{
						{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
					},
				},
			}

			reconciler.updateScanHistory(context.Background(), scan, []*batchv1.Job{job}, nil)

			Expect(scan.Status.History).To(HaveLen(1))
		})

		It("should trim history to limit", func() {
			scan := newTestClusterScan("test", "default")
			scan.Spec.HistoryLimit = ptr.To(int32(2))
			now := time.Now()

			// Add 3 jobs
			jobs := []*batchv1.Job{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "job1"},
					Status: batchv1.JobStatus{
						StartTime:      &metav1.Time{Time: now.Add(-3 * time.Minute)},
						CompletionTime: &metav1.Time{Time: now.Add(-2 * time.Minute)},
						Succeeded:      1,
						Conditions:     []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "job2"},
					Status: batchv1.JobStatus{
						StartTime:      &metav1.Time{Time: now.Add(-2 * time.Minute)},
						CompletionTime: &metav1.Time{Time: now.Add(-1 * time.Minute)},
						Succeeded:      1,
						Conditions:     []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "job3"},
					Status: batchv1.JobStatus{
						StartTime:      &metav1.Time{Time: now.Add(-1 * time.Minute)},
						CompletionTime: &metav1.Time{Time: now},
						Succeeded:      1,
						Conditions:     []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
					},
				},
			}

			reconciler.updateScanHistory(context.Background(), scan, jobs, nil)

			Expect(scan.Status.History).To(HaveLen(2))
			// Should keep the most recent (job2 and job3)
			Expect(scan.Status.History[0].JobName).To(Equal("job2"))
			Expect(scan.Status.History[1].JobName).To(Equal("job3"))
		})

		It("should set LastScanResult to most recent", func() {
			scan := newTestClusterScan("test", "default")
			now := time.Now()

			jobs := []*batchv1.Job{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "older-job"},
					Status: batchv1.JobStatus{
						StartTime:      &metav1.Time{Time: now.Add(-2 * time.Minute)},
						CompletionTime: &metav1.Time{Time: now.Add(-1 * time.Minute)},
						Succeeded:      1,
						Conditions:     []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{Name: "newer-job"},
					Status: batchv1.JobStatus{
						StartTime:      &metav1.Time{Time: now.Add(-1 * time.Minute)},
						CompletionTime: &metav1.Time{Time: now},
						Succeeded:      1,
						Conditions:     []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}},
					},
				},
			}

			reconciler.updateScanHistory(context.Background(), scan, jobs, nil)

			Expect(scan.Status.LastScanResult).NotTo(BeNil())
			Expect(scan.Status.LastScanResult.JobName).To(Equal("newer-job"))
		})
	})

	// ============================================================
	// Integration Tests with envtest
	// Note: Full integration tests require SetupWithManager to be called first
	// to set up field indexers. These tests focus on simpler scenarios.
	// ============================================================

	Context("Integration with envtest", func() {
		var (
			testNs string
		)

		BeforeEach(func() {
			testNs = "default"
		})

		AfterEach(func() {
			// Clean up all ClusterScans in the test namespace
			scanList := &scanv1alpha1.ClusterScanList{}
			if err := k8sClient.List(ctx, scanList, client.InNamespace(testNs)); err == nil {
				for _, scan := range scanList.Items {
					scan.Finalizers = nil
					_ = k8sClient.Update(ctx, &scan)
					_ = k8sClient.Delete(ctx, &scan)
				}
			}

			// Clean up all Jobs
			jobList := &batchv1.JobList{}
			if err := k8sClient.List(ctx, jobList, client.InNamespace(testNs)); err == nil {
				for _, job := range jobList.Items {
					_ = k8sClient.Delete(ctx, &job, client.PropagationPolicy(metav1.DeletePropagationBackground))
				}
			}
		})

		Describe("ClusterScan CRUD operations", func() {
			It("should create a ClusterScan resource", func() {
				scan := newTestClusterScan("crud-create", testNs)
				Expect(k8sClient.Create(ctx, scan)).To(Succeed())

				// Verify it was created
				created := &scanv1alpha1.ClusterScan{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: scan.Name, Namespace: testNs}, created)).To(Succeed())
				Expect(created.Spec.ScanTemplate.Image).To(Equal("busybox:latest"))
			})

			It("should update a ClusterScan resource", func() {
				scan := newTestClusterScan("crud-update", testNs)
				Expect(k8sClient.Create(ctx, scan)).To(Succeed())

				// Update
				scan.Spec.Schedule = "*/10 * * * *"
				Expect(k8sClient.Update(ctx, scan)).To(Succeed())

				// Verify update
				updated := &scanv1alpha1.ClusterScan{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: scan.Name, Namespace: testNs}, updated)).To(Succeed())
				Expect(updated.Spec.Schedule).To(Equal("*/10 * * * *"))
			})

			It("should delete a ClusterScan resource", func() {
				scan := newTestClusterScan("crud-delete", testNs)
				Expect(k8sClient.Create(ctx, scan)).To(Succeed())

				// Delete
				Expect(k8sClient.Delete(ctx, scan)).To(Succeed())

				// Verify deletion
				Eventually(func() bool {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: scan.Name, Namespace: testNs}, &scanv1alpha1.ClusterScan{})
					return errors.IsNotFound(err)
				}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())
			})
		})

		Describe("ClusterScan validation", func() {
			It("should accept valid cron schedule", func() {
				scan := newTestClusterScan("valid-schedule", testNs)
				scan.Spec.Schedule = fiveMinuteCronSchedule
				Expect(k8sClient.Create(ctx, scan)).To(Succeed())
			})

			It("should accept concurrency policy", func() {
				scan := newTestClusterScan("concurrency-policy", testNs)
				scan.Spec.ConcurrencyPolicy = scanv1alpha1.ForbidConcurrent
				Expect(k8sClient.Create(ctx, scan)).To(Succeed())

				created := &scanv1alpha1.ClusterScan{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: scan.Name, Namespace: testNs}, created)).To(Succeed())
				Expect(created.Spec.ConcurrencyPolicy).To(Equal(scanv1alpha1.ForbidConcurrent))
			})
		})
	})
})
