function authenticateCDM {
    param (
        [string]$Rubrik,
        [string]$UserId,
        [string]$Secret
    )

    $body = @{
        serviceAccountId = $UserId
        secret           = $Secret
    } | ConvertTo-Json

    try {
        $auth_response = Invoke-RestMethod -Uri "https://$Rubrik/api/v1/service_account/session" `
            -Method Post `
            -ContentType "application/json" `
            -Body $body `
            -SkipCertificateCheck `
            -ErrorAction Stop

        $script:TOKEN = $auth_response.token
        $script:AUTH_HEADER = "Authorization: Bearer $script:TOKEN"
        $script:AUTH_TIME = (Get-Date).ToUniversalTime().ToFileTime()
    }
    catch {
        Write-Error "Error: Failed to retrieve the authentication token."
        exit 1
    }
}
