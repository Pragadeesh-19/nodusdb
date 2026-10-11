param(
    [Parameter(Mandatory = $true, Position = 0)][string]$CommandLine,
    [int]$Cores = 2,
    [int]$Spinners = 4
)

$mask = [IntPtr]((1 -shl $Cores) - 1)
[System.Diagnostics.Process]::GetCurrentProcess().ProcessorAffinity = $mask

$running = @()
$status = 1
try {
    for ($index = 0; $index -lt $Spinners; $index++) {
        $spinner = Start-Process powershell -ArgumentList '-NoProfile', '-Command', 'while ($true) { }' -WindowStyle Hidden -PassThru
        $spinner.ProcessorAffinity = $mask
        $running += $spinner
    }
    cmd.exe /c $CommandLine
    $status = $LASTEXITCODE
}
finally {
    $running | Stop-Process -Force -ErrorAction SilentlyContinue
}
exit $status
