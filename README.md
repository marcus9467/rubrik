# Rubrik Automation Scripts

Production-tested automation for [Rubrik](https://www.rubrik.com/) CDM and Rubrik Security Cloud (RSC).

Every script here started as a specific operational problem in a customer environment — a migration that stalled, a report that didn't exist, a recovery that had to be proven before go-live — and was then generalized and hardened for reuse.

**Contents:** 39 PowerShell scripts · 26 reusable PowerShell functions · 12 Python scripts · 3 shell scripts

---

## Start here

If you're skimming, these five are the most representative of how I work:

| Script | Why it's interesting |
|--------|---------------------|
| [`Get-SnapshotDetails.ps1`](Powershell/Get-SnapshotDetails.ps1) | Multi-threaded snapshot inventory across 9 object types, with archive/replication lag and size estimation. Handles environments too large to enumerate serially. |
| [`EventParser.py`](Python/EventParser.py) | Streams high-confidence ransomware alerts from RSC to a syslog target, with a `--test` mode so you can prove SIEM connectivity before you trust it. |
| [`OnboardMSSQLHosts.ps1`](Powershell/OnboardMSSQLHosts.ps1) | Bulk SQL onboarding as a staged pipeline — generate, review, assign, validate — batching up to 50 instances per API call. The human-review step is deliberate. |
| [`DetectDoubleDippersRSC.ps1`](Powershell/DetectDoubleDippersRSC.ps1) | Finds objects protected through more than one path and recommends how to consolidate. Written after a customer discovered they were paying twice. |
| [`CloudChargeBack.ps1`](Powershell/CloudChargeBack.ps1) | Joins Rubrik's ingested cloud tags against capacity endpoints to produce chargeback by business unit. |

---

## By problem

### Onboarding & protection at scale
Getting workloads under protection when clicking through the UI isn't viable.

- `OnboardMSSQLHosts.ps1` — bulk SQL Server host registration and SLA assignment
- `OnboardingVMsCDM.ps1` — VMware VM registration with file-level recovery agent setup
- `OnboardOneDrive.ps1` — Microsoft 365 OneDrive user protection, batched
- `ProtectAzureVM.ps1` / `SampleAzureVM.ps1` — Azure VM protection automation
- `Set-vSphereVMPrefix.ps1` — SLA assignment by VM name prefix

### Day-2 operations
Snapshot lifecycle, retention, and the reporting that keeps audits quiet.

- `BackupComplianceRangeReport.ps1` — compliance and snapshot data across all clusters in an RSC tenant
- `Get-SnapshotDetails.ps1` — threaded snapshot inventory with archive and replication metadata
- `Update-SnapshotRetention.ps1` — bulk retention SLA changes or deletion
- `Delete-SnapshotsInBulk.ps1` — CSV-driven mass snapshot cleanup
- `Update-LogPolicy.ps1` — inherit log policy across existing databases
- `Refresh-WindowsHost.ps1` / `ReinstallRBS.ps1` — agent fleet maintenance

### Recovery & live mount
Workflows that get exercised under pressure, so they're built to be re-run safely.

- `LiveMountSQL.ps1` — CSV-driven SQL live mount and unmount in one modular script
- `Invoke-EhrGcpRecovery.ps1` — guided mass recovery for EHR workloads on GCP
- `MassVMwareLiveMountRemove.ps1` — bulk live mount teardown
- `invoke-filesetrestore.sh` — Linux fileset restore via REST

### Ransomware, security & audit
- `EventParser.py` / `AnomalyParser.py` — high-confidence ransomware alerts forwarded to syslog
- `EnableRadar.py` — enable Radar per cluster
- `Get-RubrikClusterEncryptionStatus.ps1` — encryption posture across all attached clusters
- `ssoGroupSummary.ps1` — SSO group membership export for access review

### Cost, capacity & reporting
- `CloudChargeBack.ps1` — cloud storage chargeback by tag
- `DetectDoubleDippersRSC.ps1` — duplicate protection detection
- `RubrikComputeSizing.py` — compute sizing analysis
- `Get-RscReportDownload.ps1` — programmatic report export from RSC

### Application-consistent & on-demand backup
- `MVBegin-EndSnapsCDM.ps1` / `MVSnapshotRSC.ps1` — managed volume open/close for app-consistent backups
- `SQLOnDemand.ps1` — snapshot triggered from a dataloader pre-script
- `OnDemandGCESnapshot.py` / `TakeOnDemandEBSVolume.py` — cloud-native on-demand snapshots
- `NutanixVMOnDemand.ps1`, `OnDemandVG.ps1`, `Bash/OnDemandBackup.sh`

### Reusable functions
[`Powershell/Functions/`](Powershell/Functions) holds 26 single-purpose functions — RSC auth, SLA lookup, host registration, live mount primitives, legal hold — that the larger scripts compose. Source them directly if you're building your own workflow.

---

## Getting started

### Prerequisites

- A Rubrik CDM cluster or Rubrik Security Cloud tenant
- An RSC service account JSON (most newer scripts) or CDM credentials (legacy CDM scripts)
- PowerShell 7+ for the PowerShell scripts
- Python 3.8+ with `requests` for the Python scripts

### Authentication

Newer scripts authenticate to RSC with a service account JSON file and take it as a parameter:

```powershell
$serviceAccountJson = "./rsc-service-account.json"
./Powershell/Get-HostInfo.ps1 -ServiceAccountJson $serviceAccountJson
```

Older CDM-era scripts prompt for credentials or accept a cluster address:

```powershell
./Powershell/Get-SnapshotDetails.ps1 -threadcount 16 -rubrikAddress <cluster-address> -AllObjects
```

Every script has full comment-based help:

```powershell
Get-Help ./Powershell/OnboardMSSQLHosts.ps1 -Full
```

---

## Conventions

- **Read before write.** Scripts that change state generally have a generate/review step that emits a CSV for human inspection before anything is applied.
- **CSV in, CSV out.** Bulk operations take a CSV and emit a timestamped CSV, so runs are auditable and re-runnable.
- **No embedded credentials.** Authentication is always by parameter — service account JSON, credential file, or prompt.

---

## Contributing

Issues and pull requests welcome. Test in a non-production environment first.

## License

MIT — see [LICENSE](LICENSE).

## Disclaimer

Community scripts, not officially supported by Rubrik. Provided as-is, without warranty. Use in accordance with your organization's change management policies.
