# Compatibility entry point for the existing desktop shortcut.
$Target = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) 'WeatherPulse.ps1'
& $Target
exit 0
