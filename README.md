# ClusterScan Operator

A Kubernetes operator for managing scheduled and on-demand cluster scanning jobs. Define scans as custom resources and let the operator handle scheduling, execution, history, and cleanup.

## Description

The ClusterScan Operator extends Kubernetes with a `ClusterScan` custom resource that declaratively defines cluster scanning workloads. It provides:

- **Scheduled Scans**: Run scans on a cron schedule (e.g., daily security scans)
- **One-off Scans**: Run a scan once immediately upon creation
- **Manual Triggers**: Re-run scans on demand via annotation or field patch
- **History Management**: Automatically retain and clean up past scan jobs and results
- **Log Capture**: Optionally store scan output in the resource status
- **Concurrency Control**: Configure how overlapping scans are handled

### Use Cases

- **Security Scanning**: Run tools like Trivy, Popeye, or Kubevious on a schedule
- **Compliance Audits**: Periodic cluster configuration validation
- **Health Checks**: Regular cluster diagnostics and reporting
- **Backup Verification**: Scheduled backup integrity checks

## Quick Start

### 1. Install the CRDs

```sh
make install
```

### 2. Run the Controller (Development)

```sh
make run
```

### 3. Create a ClusterScan

```yaml
apiVersion: scan.spectrocloud.com/v1alpha1
kind: ClusterScan
metadata:
  name: quick-scan
  namespace: default
spec:
  scanTemplate:
    image: busybox
    command: ["sh", "-c", "echo 'Scan complete!' && exit 0"]
```

```sh
kubectl apply -f config/samples/scan_v1alpha1_clusterscan.yaml
```

### 4. Check Results

```sh
# View scan status
kubectl get clusterscan

# Detailed status with history
kubectl get clusterscan quick-scan -o yaml

# View scan job logs
kubectl logs job/quick-scan-<timestamp>
```

## Examples

### Scheduled Security Scan (Every 6 Hours)

```yaml
apiVersion: scan.spectrocloud.com/v1alpha1
kind: ClusterScan
metadata:
  name: security-scan
spec:
  schedule: "0 */6 * * *"
  historyLimit: 10
  retainLogs: true
  scanTemplate:
    image: aquasec/trivy:latest
    command: ["trivy"]
    args: ["k8s", "--report", "summary", "cluster"]
    serviceAccountName: scanner-sa
    activeDeadlineSeconds: 600
```

### Popeye Cluster Sanitizer

```yaml
apiVersion: scan.spectrocloud.com/v1alpha1
kind: ClusterScan
metadata:
  name: popeye-scan
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  retainLogs: true
  logsMaxBytes: 50000
  scanTemplate:
    image: derailed/popeye:latest
    args: ["-o", "standard", "--force-exit-zero"]
    serviceAccountName: cluster-scanner
```

### One-off Scan with Log Capture

```yaml
apiVersion: scan.spectrocloud.com/v1alpha1
kind: ClusterScan
metadata:
  name: one-time-audit
spec:
  # No schedule = runs once immediately
  retainLogs: true
  logsMaxBytes: 20000
  scanTemplate:
    image: my-audit-tool:latest
    serviceAccountName: auditor
```

## Triggering Scans

### On Schedule (Cron)

```yaml
spec:
  schedule: "*/30 * * * *"  # Every 30 minutes
```

### Manual Trigger (Annotation)

```sh
kubectl annotate clusterscan my-scan scan.spectrocloud.com/trigger=$(date +%s)
```

### Manual Trigger (Field Patch)

```sh
kubectl patch clusterscan my-scan --type=merge -p '{"spec":{"triggerNow":true}}'
```

## Spec Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schedule` | string | - | Cron expression. Empty = one-off scan |
| `triggerNow` | bool | false | Trigger immediate scan (auto-resets) |
| `suspend` | bool | false | Pause scheduled scans |
| `concurrencyPolicy` | enum | Forbid | `Allow`, `Forbid`, or `Replace` |
| `historyLimit` | int32 | 5 | Jobs/results to retain (0-100) |
| `retainLogs` | bool | false | Store logs in status |
| `logsMaxBytes` | int32 | 10000 | Max log size per scan |
| `startingDeadlineSeconds` | int64 | - | Deadline for missed schedules |
| `scanTemplate` | object | **required** | Job configuration |

### ScanTemplate Fields

| Field | Type | Description |
|-------|------|-------------|
| `image` | string | **Required.** Container image |
| `command` | []string | Container entrypoint |
| `args` | []string | Arguments to entrypoint |
| `env` | []EnvVar | Environment variables |
| `resources` | ResourceRequirements | CPU/memory limits |
| `serviceAccountName` | string | ServiceAccount for the pod |
| `activeDeadlineSeconds` | int64 | Job timeout |
| `backoffLimit` | int32 | Retry count (default: 3) |

## Status Reference

```sh
kubectl get clusterscan my-scan -o jsonpath='{.status}' | jq
```

| Field | Description |
|-------|-------------|
| `phase` | `Pending`, `Running`, `Completed`, or `Failed` |
| `active` | Currently running job references |
| `completedJobs` | Total successful scan count |
| `failedJobs` | Total failed scan count |
| `lastScheduleTime` | When last scan was scheduled |
| `lastSuccessfulTime` | When last scan succeeded |
| `lastScanResult` | Most recent scan result details |
| `history` | Array of past scan results |
| `nextScheduleTime` | Next scheduled scan time |

## Getting Started

### Prerequisites

- Go version v1.23+
- Docker version 17.03+
- kubectl version v1.11.3+
- Access to a Kubernetes v1.11.3+ cluster

### To Deploy on the cluster

**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/clusterscan-operator:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don't work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/clusterscan-operator:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall

**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Development

### Run Tests

```sh
# Unit and integration tests
make test

# E2E tests (requires Kind)
make test-e2e

# With coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Run Locally

```sh
# Install CRDs
make install

# Run controller against current kubeconfig context
make run
```

### Build

```sh
# Build binary
make build

# Build Docker image
make docker-build IMG=my-registry/clusterscan-operator:dev
```

## Project Structure

```
├── api/v1alpha1/           # CRD type definitions
│   └── clusterscan_types.go
├── cmd/                    # Main entrypoint
│   └── main.go
├── config/
│   ├── crd/               # Generated CRD manifests
│   ├── manager/           # Controller deployment
│   ├── rbac/              # RBAC configuration
│   └── samples/           # Example ClusterScan resources
├── internal/controller/    # Controller implementation
│   ├── clusterscan_controller.go
│   ├── clusterscan_controller_test.go
│   └── README.md          # Controller documentation
└── test/e2e/              # End-to-end tests
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/clusterscan-operator:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/clusterscan-operator/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v2-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`make test`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

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
