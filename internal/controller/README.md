# ClusterScan Controller

This package implements the Kubernetes controller for the `ClusterScan` custom resource.

## Overview

The ClusterScan controller watches `ClusterScan` resources and manages Kubernetes Jobs to execute cluster scanning workloads. It supports scheduled scans (cron-based), one-off scans, and manual triggers.

## ClusterScan Resource

### Spec Fields

| Field | Type | Description |
|-------|------|-------------|
| `schedule` | string | Cron expression (e.g., `*/5 * * * *`). Empty = one-off scan |
| `triggerNow` | bool | Set to `true` to trigger immediate scan (auto-resets) |
| `suspend` | bool | Pause scheduled scans without deleting |
| `concurrencyPolicy` | enum | `Forbid` (default), `Allow`, or `Replace` |
| `historyLimit` | int32 | Number of past Jobs/results to retain (default: 5) |
| `retainLogs` | bool | Capture scan logs in status |
| `logsMaxBytes` | int32 | Max log size per scan (default: 10KB) |
| `startingDeadlineSeconds` | int64 | Deadline for missed scheduled scans |
| `scanTemplate` | object | Job configuration (image, command, resources, etc.) |

### ScanTemplate Fields

| Field | Type | Description |
|-------|------|-------------|
| `image` | string | **Required.** Container image for the scan |
| `imagePullPolicy` | enum | `Always`, `Never`, or `IfNotPresent` (default) |
| `imagePullSecrets` | []LocalObjectReference | Secrets for private registries |
| `command` | []string | Container entrypoint |
| `args` | []string | Arguments to entrypoint |
| `env` | []EnvVar | Environment variables |
| `envFrom` | []EnvFromSource | Environment from ConfigMaps/Secrets |
| `resources` | ResourceRequirements | CPU/memory requests and limits |
| `serviceAccountName` | string | ServiceAccount for the scan pod |
| `nodeSelector` | map[string]string | Node selection constraints |
| `tolerations` | []Toleration | Pod tolerations |
| `affinity` | Affinity | Scheduling affinity rules |
| `activeDeadlineSeconds` | int64 | Job timeout |
| `backoffLimit` | int32 | Retry count before marking failed (default: 3) |
| `volumes` | []Volume | Volumes to mount |
| `volumeMounts` | []VolumeMount | Volume mount points |
| `securityContext` | PodSecurityContext | Pod security settings |

### Status Fields

| Field | Type | Description |
|-------|------|-------------|
| `phase` | enum | `Pending`, `Running`, `Completed`, or `Failed` |
| `active` | []ObjectReference | Currently running Jobs |
| `completedJobs` | int32 | Total successful scans |
| `failedJobs` | int32 | Total failed scans |
| `lastScheduleTime` | Time | When last scan was scheduled |
| `lastSuccessfulTime` | Time | When last scan succeeded |
| `lastScanResult` | ScanResult | Most recent scan result |
| `history` | []ScanResult | Historical scan results |
| `nextScheduleTime` | Time | Next scheduled scan time |
| `lastTriggeredTime` | Time | Last manual trigger time |

### ScanResult Fields

| Field | Type | Description |
|-------|------|-------------|
| `jobName` | string | Kubernetes Job name |
| `startTime` | Time | When scan started |
| `completionTime` | Time | When scan finished |
| `succeeded` | bool | Whether scan succeeded |
| `message` | string | Human-readable result message |
| `findings` | int32 | Number of issues found (via annotation) |
| `logs` | string | Captured container logs |
| `logsTruncated` | bool | Whether logs were truncated |

## Reconciliation Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Reconcile Loop                                │
├─────────────────────────────────────────────────────────────────┤
│  1. Fetch ClusterScan                                           │
│     └─ If not found → stop (deleted)                            │
│                                                                  │
│  2. Handle Finalizer                                            │
│     ├─ If being deleted → cleanup child Jobs → remove finalizer │
│     └─ If new → add finalizer                                   │
│                                                                  │
│  3. List Child Jobs                                             │
│     └─ Categorize: active, successful, failed                   │
│                                                                  │
│  4. Update Status                                               │
│     ├─ Phase (Pending/Running/Completed/Failed)                 │
│     ├─ Job counts                                               │
│     ├─ Last successful time                                     │
│     └─ History (from completed Jobs)                            │
│                                                                  │
│  5. Cleanup Old Jobs                                            │
│     └─ Delete Jobs exceeding historyLimit                       │
│                                                                  │
│  6. Check if New Job Needed                                     │
│     ├─ TriggerNow → create immediately                          │
│     ├─ Trigger annotation → create immediately                  │
│     ├─ One-off (no schedule, never run) → create                │
│     └─ Scheduled → check cron expression                        │
│                                                                  │
│  7. Apply Concurrency Policy                                    │
│     ├─ Forbid: skip if active job exists                        │
│     ├─ Replace: delete active job, then create                  │
│     └─ Allow: create regardless of active jobs                  │
│                                                                  │
│  8. Create Job (if needed)                                      │
│     └─ Set owner reference, labels, annotations                 │
│                                                                  │
│  9. Update Status & Requeue                                     │
│     └─ Requeue for next schedule time                           │
└─────────────────────────────────────────────────────────────────┘
```

## Triggering Scans

### 1. Scheduled (Cron)

```yaml
spec:
  schedule: "0 */6 * * *"  # Every 6 hours
```

### 2. One-off (Immediate)

```yaml
spec:
  # No schedule = runs once immediately
  scanTemplate:
    image: my-scanner:latest
```

### 3. TriggerNow Field

```bash
kubectl patch clusterscan my-scan --type=merge -p '{"spec":{"triggerNow":true}}'
```

### 4. Trigger Annotation

```bash
kubectl annotate clusterscan my-scan scan.spectrocloud.com/trigger=$(date +%s)
```

## Concurrency Policies

| Policy | Behavior |
|--------|----------|
| `Forbid` | Skip new scan if one is already running (default) |
| `Allow` | Run multiple scans concurrently |
| `Replace` | Cancel running scan, start new one |

## Capturing Findings

Scan jobs can report findings by annotating themselves:

```bash
# In your scan script
kubectl annotate job $JOB_NAME scan.spectrocloud.com/findings=$COUNT --overwrite
```

The controller reads this annotation and stores it in `status.lastScanResult.findings`.

## Labels and Annotations

### Labels (on Jobs)

| Label | Value |
|-------|-------|
| `scan.spectrocloud.com/clusterscan` | ClusterScan name |

### Annotations (on Jobs)

| Annotation | Description |
|------------|-------------|
| `scan.spectrocloud.com/scheduled-at` | Scheduled timestamp (RFC3339) |
| `scan.spectrocloud.com/findings` | Number of issues found |

### Annotations (on ClusterScan)

| Annotation | Description |
|------------|-------------|
| `scan.spectrocloud.com/trigger` | Trigger value (set by user) |
| `scan.spectrocloud.com/last-trigger` | Last processed trigger value |

## RBAC Requirements

The controller requires these permissions:

```yaml
# ClusterScan resources
- apiGroups: ["scan.spectrocloud.com"]
  resources: ["clusterscans", "clusterscans/status", "clusterscans/finalizers"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]

# Jobs for scan execution
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]

# Pods for log capture
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list"]
```

## Example ClusterScan

```yaml
apiVersion: scan.spectrocloud.com/v1alpha1
kind: ClusterScan
metadata:
  name: security-scan
  namespace: default
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM
  concurrencyPolicy: Forbid
  historyLimit: 10
  retainLogs: true
  logsMaxBytes: 50000
  scanTemplate:
    image: aquasec/trivy:latest
    command: ["trivy"]
    args: ["k8s", "--report", "summary", "cluster"]
    serviceAccountName: scanner-sa
    resources:
      requests:
        memory: "256Mi"
        cpu: "100m"
      limits:
        memory: "512Mi"
        cpu: "500m"
    activeDeadlineSeconds: 600
    backoffLimit: 2
```

## Testing

```bash
# Unit tests
go test ./internal/controller/...

# With coverage
go test -coverprofile=coverage.out ./internal/controller/...
go tool cover -html=coverage.out
```

