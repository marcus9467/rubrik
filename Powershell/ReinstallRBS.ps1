param (
    [string]$Rubrik
)

if (-not $Rubrik) {
    Write-Error "Rubrik server address is required. Use the -Rubrik flag to specify it."
    exit 1
}

# Define the URL and file paths
$url = "https://$Rubrik/connector/RubrikBackupService.zip"
$zipFile = "$env:TEMP\RubrikBackupService.zip"
$extractPath = "$env:TEMP\RubrikBackupService"
$msiFile = "$extractPath\RubrikBackupService.msi"

# Skip SSL certificate checks
Add-Type @"
using System;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

public class TrustAllCertsPolicy {
    public static void IgnoreBadCertificates() {
        ServicePointManager.ServerCertificateValidationCallback = new RemoteCertificateValidationCallback(
            delegate { return true; });
    }
}
"@
[TrustAllCertsPolicy]::IgnoreBadCertificates()

# Download the ZIP file using WebClient
$webClient = New-Object System.Net.WebClient
$webClient.DownloadFile($url, $zipFile)

# Extract the ZIP file
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Compression.ZipFile]::ExtractToDirectory($zipFile, $extractPath)

# Function to check if the Rubrik Backup Service is installed
function Test-ServiceInstalled {
    param (
        [string]$serviceName
    )
    $service = Get-WmiObject -Class Win32_Product | Where-Object { $_.Name -eq $serviceName }
    return $service -ne $null
}

# Function to uninstall the Rubrik Backup Service
function Uninstall-Service {
    param (
        [string]$serviceName
    )
    $service = Get-WmiObject -Class Win32_Product | Where-Object { $_.Name -eq $serviceName }
    if ($service) {
        $service.Uninstall()
    }
}

# Function to install the Rubrik Backup Service
function Install-Service {
    param (
        [string]$msiPath
    )
    Start-Process -FilePath "msiexec.exe" -ArgumentList "/i", $msiPath, "/quiet", "/norestart" -Wait
}

# Service name of the Rubrik Backup Service
$serviceName = "Rubrik Backup Service"

# Check if the service is already installed
if (Test-ServiceInstalled -serviceName $serviceName) {
    # Uninstall the service if it is installed
    Uninstall-Service -serviceName $serviceName
}

# Install the new service package
Install-Service -msiPath $msiFile

# Clean up temporary files
Remove-Item -Path $zipFile, $extractPath -Recurse -Force

Write-Output "Rubrik Backup Service has been reinstalled successfully."
