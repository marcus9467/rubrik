function Request-FullVMwareSnapshotCDM {
    <#
    .SYNOPSIS
        Requests a full snapshot for the next backup job of a VMware virtual machine.
    .DESCRIPTION
        This function makes a POST request to the Rubrik API to force a full snapshot.
        It requires the VM's ID and an array of objects containing virtual disk information.
    .PARAMETER vmId
        The ID of the VMware virtual machine. This is a required string.
    .PARAMETER virtualDiskInfos
        An array of objects, where each object contains the virtualDiskId (string) and
        a boolean value for shouldDedupe. This is a required parameter.
    
    .PARAMETER Rubrik
        The hostname or IP address of the Rubrik cluster. This is a required string.
    .EXAMPLE
        # Define the virtual disk information
        $disks = @(
            @{ virtualDiskId = "disk_id_1"; shouldDedupe = $true },
            @{ virtualDiskId = "disk_id_2"; shouldDedupe = $false }
        )
        # Call the function
        Request-FullVMwareSnapshotCDM -vmId "vm_id_abc" -virtualDiskInfos $disks -Rubrik "my-rubrik-cluster.com"
    .NOTES
        Assumes the $AUTH_HEADER variable is set from the 'authenticate' function.
    #>
    param (
        [Parameter(Mandatory = $true)]
        [string]$vmId,
        [Parameter(Mandatory = $true)]
        [array]$virtualDiskInfos,
        [Parameter(Mandatory = $true)]
        [string]$Rubrik
    )
    # Construct the request body as a PowerShell object
    # This will be automatically converted to the required JSON structure
    $body = @{
        virtualDiskInfos = $virtualDiskInfos
    } | ConvertTo-Json
    # Check if the authentication header is set from the authenticate function
    if (-not $script:AUTH_HEADER) {
        Write-Error "Error: Authentication header is not set. Please run the 'authenticate' function first."
        exit 1
    }
    $uri = "https://$Rubrik/api/v1/vmware/vm/$vmId/request/force_full_snapshot"
    Write-Host "Requesting full snapshot for VM '$vmId'..."
    try {
        # Perform the API call
        $response = Invoke-RestMethod -Uri $uri `
            -Method Post `
            -Headers @{ "Authorization" = $script:AUTH_HEADER } `
            -ContentType "application/json" `
            -Body $body `
            -SkipCertificateCheck `
            -ErrorAction Stop
        Write-Host "Full snapshot request successful. Response:"
        $response | Format-List
    }
    catch {
        Write-Error "Error: Failed to request a full snapshot."
        Write-Error $_.Exception.Message
        exit 1
    }
}
