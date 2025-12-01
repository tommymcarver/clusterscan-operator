//go:build e2e
// +build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/tommymcarver/clusterscan-operator/test/utils"
)

// namespace where the project is deployed in
const namespace = "clusterscan-operator-system"

// testNamespace where test resources are created
const testNamespace = "default"

// serviceAccountName created for the project
const serviceAccountName = "clusterscan-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "clusterscan-operator-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "clusterscan-operator-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", projectImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=clusterscan-operator-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": ["curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics"],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks
	})

	// ============================================================
	// ClusterScan E2E Tests
	// ============================================================

	Context("ClusterScan", func() {
		// Helper function to create ClusterScan YAML
		createClusterScanYAML := func(name string, spec string) string {
			return fmt.Sprintf(`
apiVersion: scan.spectrocloud.com/v1alpha1
kind: ClusterScan
metadata:
  name: %s
  namespace: %s
spec:
%s`, name, testNamespace, spec)
		}

		// Helper to apply YAML
		applyYAML := func(yaml string) error {
			cmd := exec.Command("kubectl", "apply", "-f", "-")
			cmd.Stdin = strings.NewReader(yaml)
			_, err := utils.Run(cmd)
			return err
		}

		// Helper to delete ClusterScan
		deleteClusterScan := func(name string) {
			cmd := exec.Command("kubectl", "delete", "clusterscan", name, "-n", testNamespace, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		}

		// Helper to get ClusterScan status
		getClusterScanStatus := func(name string, jsonPath string) (string, error) {
			cmd := exec.Command("kubectl", "get", "clusterscan", name, "-n", testNamespace, "-o", fmt.Sprintf("jsonpath=%s", jsonPath))
			return utils.Run(cmd)
		}

		// Helper to count jobs for a ClusterScan
		countJobsForScan := func(name string) (int, error) {
			cmd := exec.Command("kubectl", "get", "jobs", "-l", fmt.Sprintf("scan.spectrocloud.com/clusterscan=%s", name), "-n", testNamespace, "-o", "name")
			output, err := utils.Run(cmd)
			if err != nil {
				return 0, err
			}
			if output == "" {
				return 0, nil
			}
			return len(utils.GetNonEmptyLines(output)), nil
		}

		AfterEach(func() {
			// Clean up test ClusterScans
			for _, name := range []string{
				"e2e-oneoff-scan",
				"e2e-scheduled-scan",
				"e2e-triggernow-scan",
				"e2e-annotation-scan",
				"e2e-suspend-scan",
				"e2e-finalizer-scan",
				"e2e-history-scan",
			} {
				deleteClusterScan(name)
			}

			// Wait for jobs to be cleaned up
			time.Sleep(2 * time.Second)
		})

		Describe("One-off Scan", func() {
			It("should create a Job immediately for a one-off scan", func() {
				scanName := "e2e-oneoff-scan"
				yaml := createClusterScanYAML(scanName, `
  scanTemplate:
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'E2E one-off scan completed'; exit 0"]
    activeDeadlineSeconds: 60
    backoffLimit: 1`)

				By("creating the ClusterScan")
				err := applyYAML(yaml)
				Expect(err).NotTo(HaveOccurred())

				By("verifying a Job is created")
				verifyJobCreated := func(g Gomega) {
					count, err := countJobsForScan(scanName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(count).To(BeNumerically(">=", 1), "Expected at least 1 job")
				}
				Eventually(verifyJobCreated, 30*time.Second).Should(Succeed())

				By("verifying the scan status is updated")
				verifyStatus := func(g Gomega) {
					phase, err := getClusterScanStatus(scanName, "{.status.phase}")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(phase).To(BeElementOf("Running", "Completed", "Pending"))
				}
				Eventually(verifyStatus, 30*time.Second).Should(Succeed())
			})

			It("should not create additional jobs for one-off scan after completion", func() {
				scanName := "e2e-oneoff-scan"
				yaml := createClusterScanYAML(scanName, `
  scanTemplate:
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'Quick scan'; exit 0"]
    activeDeadlineSeconds: 30
    backoffLimit: 1`)

				By("creating the ClusterScan")
				err := applyYAML(yaml)
				Expect(err).NotTo(HaveOccurred())

				By("waiting for job to complete")
				verifyJobCompleted := func(g Gomega) {
					phase, err := getClusterScanStatus(scanName, "{.status.phase}")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(phase).To(Equal("Completed"))
				}
				Eventually(verifyJobCompleted, 60*time.Second).Should(Succeed())

				By("recording initial job count")
				initialCount, err := countJobsForScan(scanName)
				Expect(err).NotTo(HaveOccurred())

				By("waiting and verifying no new jobs are created")
				time.Sleep(5 * time.Second)
				finalCount, err := countJobsForScan(scanName)
				Expect(err).NotTo(HaveOccurred())
				Expect(finalCount).To(Equal(initialCount), "No new jobs should be created")
			})
		})

		Describe("TriggerNow", func() {
			It("should create a Job when triggerNow is set", func() {
				scanName := "e2e-triggernow-scan"

				By("creating a ClusterScan without triggerNow (already ran)")
				yaml := createClusterScanYAML(scanName, `
  scanTemplate:
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'Initial scan'; exit 0"]
    activeDeadlineSeconds: 30
    backoffLimit: 1`)
				err := applyYAML(yaml)
				Expect(err).NotTo(HaveOccurred())

				By("waiting for first job to complete")
				verifyFirstJobComplete := func(g Gomega) {
					phase, err := getClusterScanStatus(scanName, "{.status.phase}")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(phase).To(Equal("Completed"))
				}
				Eventually(verifyFirstJobComplete, 60*time.Second).Should(Succeed())

				By("recording job count")
				initialCount, err := countJobsForScan(scanName)
				Expect(err).NotTo(HaveOccurred())

				By("setting triggerNow to true")
				cmd := exec.Command("kubectl", "patch", "clusterscan", scanName, "-n", testNamespace,
					"--type=merge", "-p", `{"spec":{"triggerNow":true}}`)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())

				By("verifying a new Job is created")
				verifyNewJob := func(g Gomega) {
					count, err := countJobsForScan(scanName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(count).To(BeNumerically(">", initialCount), "Expected new job to be created")
				}
				Eventually(verifyNewJob, 30*time.Second).Should(Succeed())

				By("verifying triggerNow is reset to false")
				verifyTriggerReset := func(g Gomega) {
					triggerNow, err := getClusterScanStatus(scanName, "{.spec.triggerNow}")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(triggerNow).To(BeElementOf("false", ""))
				}
				Eventually(verifyTriggerReset, 30*time.Second).Should(Succeed())
			})
		})

		Describe("Trigger Annotation", func() {
			It("should create a Job when trigger annotation is set", func() {
				scanName := "e2e-annotation-scan"

				By("creating a ClusterScan")
				yaml := createClusterScanYAML(scanName, `
  scanTemplate:
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'Annotation scan'; exit 0"]
    activeDeadlineSeconds: 30
    backoffLimit: 1`)
				err := applyYAML(yaml)
				Expect(err).NotTo(HaveOccurred())

				By("waiting for first job to complete")
				verifyFirstJobComplete := func(g Gomega) {
					phase, err := getClusterScanStatus(scanName, "{.status.phase}")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(phase).To(Equal("Completed"))
				}
				Eventually(verifyFirstJobComplete, 60*time.Second).Should(Succeed())

				By("recording job count")
				initialCount, err := countJobsForScan(scanName)
				Expect(err).NotTo(HaveOccurred())

				By("adding trigger annotation")
				timestamp := fmt.Sprintf("%d", time.Now().Unix())
				cmd := exec.Command("kubectl", "annotate", "clusterscan", scanName, "-n", testNamespace,
					fmt.Sprintf("scan.spectrocloud.com/trigger=%s", timestamp), "--overwrite")
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())

				By("verifying a new Job is created")
				verifyNewJob := func(g Gomega) {
					count, err := countJobsForScan(scanName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(count).To(BeNumerically(">", initialCount), "Expected new job to be created")
				}
				Eventually(verifyNewJob, 30*time.Second).Should(Succeed())
			})
		})

		Describe("Suspend", func() {
			It("should not create Jobs when suspended", func() {
				scanName := "e2e-suspend-scan"

				By("creating a suspended ClusterScan")
				yaml := createClusterScanYAML(scanName, `
  suspend: true
  triggerNow: true
  scanTemplate:
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'Should not run'; exit 0"]
    activeDeadlineSeconds: 30
    backoffLimit: 1`)
				err := applyYAML(yaml)
				Expect(err).NotTo(HaveOccurred())

				By("waiting to ensure no jobs are created")
				time.Sleep(10 * time.Second)

				By("verifying no jobs exist")
				count, err := countJobsForScan(scanName)
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(0), "No jobs should be created when suspended")
			})
		})

		Describe("Finalizer and Cleanup", func() {
			It("should clean up Jobs when ClusterScan is deleted", func() {
				scanName := "e2e-finalizer-scan"

				By("creating a ClusterScan")
				yaml := createClusterScanYAML(scanName, `
  scanTemplate:
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'Finalizer test scan'; sleep 30; exit 0"]
    activeDeadlineSeconds: 120
    backoffLimit: 1`)
				err := applyYAML(yaml)
				Expect(err).NotTo(HaveOccurred())

				By("verifying finalizer is added")
				verifyFinalizer := func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "clusterscan", scanName, "-n", testNamespace,
						"-o", "jsonpath={.metadata.finalizers}")
					output, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(output).To(ContainSubstring("scan.spectrocloud.com/finalizer"))
				}
				Eventually(verifyFinalizer, 30*time.Second).Should(Succeed())

				By("waiting for job to start")
				verifyJobStarted := func(g Gomega) {
					count, err := countJobsForScan(scanName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(count).To(BeNumerically(">=", 1))
				}
				Eventually(verifyJobStarted, 30*time.Second).Should(Succeed())

				By("deleting the ClusterScan")
				cmd := exec.Command("kubectl", "delete", "clusterscan", scanName, "-n", testNamespace)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())

				By("verifying ClusterScan is deleted")
				verifyScanDeleted := func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "clusterscan", scanName, "-n", testNamespace)
					_, err := utils.Run(cmd)
					g.Expect(err).To(HaveOccurred(), "ClusterScan should be deleted")
				}
				Eventually(verifyScanDeleted, 60*time.Second).Should(Succeed())

				By("verifying Jobs are cleaned up")
				verifyJobsDeleted := func(g Gomega) {
					count, err := countJobsForScan(scanName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(count).To(Equal(0), "Jobs should be cleaned up")
				}
				Eventually(verifyJobsDeleted, 60*time.Second).Should(Succeed())
			})
		})

		Describe("Status Updates", func() {
			It("should update status with scan history", func() {
				scanName := "e2e-history-scan"

				By("creating a ClusterScan with history limit")
				yaml := createClusterScanYAML(scanName, `
  historyLimit: 3
  retainLogs: true
  scanTemplate:
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'History test scan'; exit 0"]
    activeDeadlineSeconds: 30
    backoffLimit: 1`)
				err := applyYAML(yaml)
				Expect(err).NotTo(HaveOccurred())

				By("waiting for job to complete")
				verifyCompleted := func(g Gomega) {
					phase, err := getClusterScanStatus(scanName, "{.status.phase}")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(phase).To(Equal("Completed"))
				}
				Eventually(verifyCompleted, 60*time.Second).Should(Succeed())

				By("verifying lastScanResult is populated")
				verifyLastResult := func(g Gomega) {
					jobName, err := getClusterScanStatus(scanName, "{.status.lastScanResult.jobName}")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(jobName).NotTo(BeEmpty())
				}
				Eventually(verifyLastResult, 30*time.Second).Should(Succeed())

				By("verifying history is populated")
				verifyHistory := func(g Gomega) {
					cmd := exec.Command("kubectl", "get", "clusterscan", scanName, "-n", testNamespace,
						"-o", "jsonpath={.status.history}")
					output, err := utils.Run(cmd)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(output).NotTo(BeEmpty())
				}
				Eventually(verifyHistory, 30*time.Second).Should(Succeed())
			})
		})

		Describe("Scheduled Scan", func() {
			It("should create Jobs on schedule", func() {
				scanName := "e2e-scheduled-scan"

				By("creating a scheduled ClusterScan (every minute)")
				yaml := createClusterScanYAML(scanName, `
  schedule: "* * * * *"
  concurrencyPolicy: Forbid
  scanTemplate:
    image: busybox:latest
    command: ["/bin/sh", "-c"]
    args: ["echo 'Scheduled scan at $(date)'; exit 0"]
    activeDeadlineSeconds: 30
    backoffLimit: 1`)
				err := applyYAML(yaml)
				Expect(err).NotTo(HaveOccurred())

				By("verifying schedule is set in status")
				verifySchedule := func(g Gomega) {
					schedule, err := getClusterScanStatus(scanName, "{.spec.schedule}")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(schedule).To(Equal("* * * * *"))
				}
				Eventually(verifySchedule, 10*time.Second).Should(Succeed())

				By("waiting for at least one job to be created (may take up to 1 minute)")
				verifyJobCreated := func(g Gomega) {
					count, err := countJobsForScan(scanName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(count).To(BeNumerically(">=", 1), "Expected at least 1 scheduled job")
				}
				Eventually(verifyJobCreated, 90*time.Second, 5*time.Second).Should(Succeed())

				By("verifying nextScheduleTime is set")
				verifyNextSchedule := func(g Gomega) {
					nextTime, err := getClusterScanStatus(scanName, "{.status.nextScheduleTime}")
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(nextTime).NotTo(BeEmpty())
				}
				Eventually(verifyNextSchedule, 30*time.Second).Should(Succeed())
			})
		})
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	// Temporary file to store the token request
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		// Execute kubectl command to create the token
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		// Parse the JSON output to extract the token
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
