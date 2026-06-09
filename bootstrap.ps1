# HeavyDrops Transcoder - Windows one-liner bootstrap.
#
# Usage (paste into PowerShell):
#
#   iwr https://raw.githubusercontent.com/luisimperator/rep01/main/bootstrap.ps1 -UseBasicParsing | iex
#
# For an EDITORS' machine (Heavy1-6) that should only chip in OVERNIGHT -
# pre-configured for NVIDIA NVENC, night mode (18:00-09:00, yields the instant
# someone uses the PC), shared-Dropbox claim, and a low disk footprint - set
# HD_WORKER first:
#
#   $env:HD_WORKER=1; iwr https://raw.githubusercontent.com/luisimperator/rep01/main/bootstrap.ps1 -UseBasicParsing | iex
#
# To also bake in the Dropbox credentials so you don't retype them on every
# machine, set them too - they go into the command you paste, NEVER into the
# repo. Use the durable refresh-token trio (copy the three values from a working
# machine's config.yaml, e.g. HEAVY7); a plain access token expires in ~4h:
#
#   $env:HD_WORKER=1
#   $env:HD_DROPBOX_APP_KEY='...'; $env:HD_DROPBOX_APP_SECRET='...'; $env:HD_DROPBOX_REFRESH_TOKEN='...'
#   iwr https://raw.githubusercontent.com/luisimperator/rep01/main/bootstrap.ps1 -UseBasicParsing | iex
#
# What it does:
#   1. Ensures Git and Python 3.12 are installed (via winget, user scope).
#   2. Clones this repo to %USERPROFILE%\HeavyDrops (or updates if it already exists).
#   3. Creates a Python virtualenv and pip-installs the package in editable mode.
#   4. Downloads the BtbN FFmpeg build and unpacks ffmpeg.exe + ffprobe.exe
#      into %USERPROFILE%\HeavyDrops\bin\.
#   5. Writes %USERPROFILE%\HeavyDrops\config.yaml from config.example.yaml with
#      paths substituted for this user. Prompts once for the Dropbox token.
#   6. Registers the HeavyDropsDaemon scheduled task so the daemon auto-starts
#      at logon and restarts on failure. Runs as the current user (no admin).
#   7. Creates a Desktop shortcut "HeavyDrops" that opens the dashboard in
#      your default browser.
#
# Everything installs under %USERPROFILE%; no administrator rights required.

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version 2

# Worker mode: editors' machines get the production profile (NVENC, night mode,
# shared Dropbox, low disk). Selected via `$env:HD_WORKER=1` before piping to iex.
$WorkerMode = [bool]$env:HD_WORKER

# --- Paths ----------------------------------------------------------------

$InstallDir = Join-Path $env:USERPROFILE 'HeavyDrops'
$VenvDir    = Join-Path $InstallDir '.venv'
$BinDir     = Join-Path $InstallDir 'bin'
$DataDir    = Join-Path $InstallDir 'data'
$LogDir     = Join-Path $DataDir 'logs'
$StageDir   = Join-Path $DataDir 'staging'
$ConfigPath = Join-Path $InstallDir 'config.yaml'
$TaskName   = 'HeavyDropsDaemon'
$RepoUrl    = 'https://github.com/luisimperator/rep01.git'
$RepoBranch = 'main'
$FFmpegUrl  = 'https://github.com/BtbN/FFmpeg-Builds/releases/latest/download/ffmpeg-master-latest-win64-gpl.zip'

function Info([string]$msg)  { Write-Host "[bootstrap] $msg" -ForegroundColor Cyan }
function Warn([string]$msg)  { Write-Host "[bootstrap] $msg" -ForegroundColor Yellow }
function Die([string]$msg)   { Write-Host "[bootstrap] $msg" -ForegroundColor Red; exit 1 }

# --- 1. Prerequisites -----------------------------------------------------

function Ensure-Tool([string]$exe, [string]$wingetId, [string]$label) {
    if (Get-Command $exe -ErrorAction SilentlyContinue) {
        Info "$label already installed."
        return
    }
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        Die "winget is not available. Install $label manually and rerun."
    }
    Info "Installing $label via winget (user scope)..."
    winget install --id $wingetId -e --accept-source-agreements --accept-package-agreements --scope user | Out-Null

    # Refresh PATH for this session so the new binary is findable.
    $env:Path = [Environment]::GetEnvironmentVariable('Path', 'Machine') + ';' + `
                [Environment]::GetEnvironmentVariable('Path', 'User')
    if (-not (Get-Command $exe -ErrorAction SilentlyContinue)) {
        Die "$label install finished but '$exe' is still not on PATH. Close and reopen PowerShell, then rerun."
    }
}

Ensure-Tool 'git'    'Git.Git'           'Git'
Ensure-Tool 'python' 'Python.Python.3.12' 'Python 3.12'

# --- 2. Clone or update the repo -----------------------------------------

if (Test-Path (Join-Path $InstallDir '.git')) {
    Info "Updating existing checkout at $InstallDir..."
    git -C $InstallDir fetch origin $RepoBranch | Out-Null
    git -C $InstallDir checkout $RepoBranch      | Out-Null
    git -C $InstallDir pull --ff-only origin $RepoBranch | Out-Null
} else {
    if (Test-Path $InstallDir) {
        Die "$InstallDir exists but is not a git checkout. Remove it or rerun from a clean state."
    }
    Info "Cloning repository to $InstallDir..."
    git clone --branch $RepoBranch --single-branch $RepoUrl $InstallDir | Out-Null
}

foreach ($d in @($BinDir, $DataDir, $LogDir, $StageDir)) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}

# --- 3. Virtualenv + pip install -e . ------------------------------------

if (-not (Test-Path (Join-Path $VenvDir 'Scripts\python.exe'))) {
    Info "Creating virtualenv at $VenvDir..."
    python -m venv $VenvDir
}

$VenvPy  = Join-Path $VenvDir 'Scripts\python.exe'
$VenvPip = Join-Path $VenvDir 'Scripts\pip.exe'

Info "Upgrading pip and installing the transcoder package..."
& $VenvPy -m pip install --upgrade pip | Out-Null
& $VenvPip install -e $InstallDir | Out-Null

# --- 4. FFmpeg ------------------------------------------------------------

$FFmpegExe  = Join-Path $BinDir 'ffmpeg.exe'
$FFprobeExe = Join-Path $BinDir 'ffprobe.exe'

if (-not (Test-Path $FFmpegExe) -or -not (Test-Path $FFprobeExe)) {
    Info "Downloading FFmpeg..."
    $zip = Join-Path $env:TEMP 'hd-ffmpeg.zip'
    $expand = Join-Path $env:TEMP 'hd-ffmpeg-unpack'
    if (Test-Path $expand) { Remove-Item -Recurse -Force $expand }
    Invoke-WebRequest -Uri $FFmpegUrl -OutFile $zip -UseBasicParsing
    Expand-Archive -Path $zip -DestinationPath $expand -Force

    $srcFf = Get-ChildItem -Path $expand -Recurse -Filter 'ffmpeg.exe'  | Select-Object -First 1
    $srcFp = Get-ChildItem -Path $expand -Recurse -Filter 'ffprobe.exe' | Select-Object -First 1
    if (-not $srcFf -or -not $srcFp) {
        Die "FFmpeg archive did not contain ffmpeg.exe/ffprobe.exe; aborting."
    }
    Copy-Item $srcFf.FullName $FFmpegExe  -Force
    Copy-Item $srcFp.FullName $FFprobeExe -Force
    Remove-Item -Recurse -Force $expand
    Remove-Item -Force $zip
    Info "FFmpeg installed at $BinDir."
}

# --- 5. Config ------------------------------------------------------------

$ExampleConfig = Join-Path $InstallDir 'config.example.yaml'
if (-not (Test-Path $ExampleConfig)) {
    Die "config.example.yaml not found in the checkout; the repo is incomplete."
}

if (-not (Test-Path $ConfigPath)) {
    Info "Writing config.yaml..."
    if ($env:HD_DROPBOX_TOKEN) {
        $token = $env:HD_DROPBOX_TOKEN
        Info "Using Dropbox access token from HD_DROPBOX_TOKEN."
    } elseif ($env:HD_DROPBOX_REFRESH_TOKEN) {
        $token = ''   # durable refresh-token auth set below; no short-lived token needed
        Info "Using Dropbox refresh-token credentials from HD_DROPBOX_* env vars."
    } else {
        $token = Read-Host 'Paste your Dropbox access token (leave blank to set later)'
    }
    $raw = Get-Content -Raw -LiteralPath $ExampleConfig

    # User-scoped paths
    $raw = $raw -replace 'local_staging_dir: .*', ('local_staging_dir: "' + $StageDir.Replace('\','/') + '"')
    $raw = $raw -replace 'local_output_dir: .*',  ('local_output_dir: "'  + (Join-Path $DataDir 'output').Replace('\','/') + '"')
    $raw = $raw -replace 'database_path: .*',     ('database_path: "'     + (Join-Path $DataDir 'transcoder.db').Replace('\','/') + '"')
    $raw = $raw -replace 'lockfile_path: .*',     ('lockfile_path: "'     + (Join-Path $DataDir 'transcoder.lock').Replace('\','/') + '"')
    $raw = $raw -replace 'log_dir: .*',           ('log_dir: "'           + $LogDir.Replace('\','/') + '"')
    $raw = $raw -replace 'ffmpeg_path: .*',       ('ffmpeg_path: "'       + $FFmpegExe.Replace('\','/') + '"')
    $raw = $raw -replace 'ffprobe_path: .*',      ('ffprobe_path: "'      + $FFprobeExe.Replace('\','/') + '"')
    if ($token) {
        $raw = $raw -replace 'dropbox_token: .*', ("dropbox_token: `"$token`"")
    }
    # Durable refresh-token auth (recommended - short-lived tokens die in ~4h).
    # Copy these three values from a working machine's config.yaml (e.g. HEAVY7).
    if ($env:HD_DROPBOX_APP_KEY) {
        $raw = $raw -replace 'dropbox_app_key: .*', ("dropbox_app_key: `"$($env:HD_DROPBOX_APP_KEY)`"")
    }
    if ($env:HD_DROPBOX_APP_SECRET) {
        $raw = $raw -replace 'dropbox_app_secret: .*', ("dropbox_app_secret: `"$($env:HD_DROPBOX_APP_SECRET)`"")
    }
    if ($env:HD_DROPBOX_REFRESH_TOKEN) {
        $raw = $raw -replace 'dropbox_refresh_token: .*', ("dropbox_refresh_token: `"$($env:HD_DROPBOX_REFRESH_TOKEN)`"")
    }

    Set-Content -LiteralPath $ConfigPath -Value $raw -Encoding UTF8
    Info "config.yaml written. Edit it later at: $ConfigPath"
} else {
    Info "config.yaml already present; not overwriting."
}

# --- 5b. Worker profile (editors' machines) -------------------------------

if ($WorkerMode) {
    Info "Applying production-machine profile (NVENC, night mode 18:00-09:00, shared Dropbox, low disk)..."
    $WorkerScript = Join-Path $InstallDir 'installer\apply_worker_profile.py'
    if (Test-Path $WorkerScript) {
        & $VenvPy $WorkerScript $ConfigPath
    } else {
        Warn "apply_worker_profile.py not found; skipping profile. Set it from the dashboard instead."
    }
}

# --- 6. Scheduled task ----------------------------------------------------

$TaskXmlTemplate = Join-Path $InstallDir 'installer\tasks\HeavyDropsDaemon.xml'
if (-not (Test-Path $TaskXmlTemplate)) {
    Die "Task template not found at $TaskXmlTemplate; the repo is incomplete."
}

$UserName = "$env:USERDOMAIN\$env:USERNAME"
$UserSid  = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value

$TaskXml = Get-Content -Raw -LiteralPath $TaskXmlTemplate
$TaskXml = $TaskXml.Replace('{USER_ID}',     $UserName)
$TaskXml = $TaskXml.Replace('{USER_SID}',    $UserSid)
$TaskXml = $TaskXml.Replace('{INSTALL_DIR}', $InstallDir)
$TaskXml = $TaskXml.Replace('{CONFIG_PATH}', $ConfigPath)

$TaskXmlTmp = Join-Path $env:TEMP "hd-task-$PID.xml"
# schtasks expects UTF-16 LE with BOM for XML files
[System.IO.File]::WriteAllText($TaskXmlTmp, $TaskXml, [System.Text.Encoding]::Unicode)

Info "Registering scheduled task '$TaskName'..."
schtasks /Create /TN $TaskName /XML $TaskXmlTmp /F | Out-Null
Remove-Item -Force $TaskXmlTmp

Info "Starting daemon via Task Scheduler..."
schtasks /Run /TN $TaskName | Out-Null

# --- 7. Desktop shortcut --------------------------------------------------

$Desktop = [Environment]::GetFolderPath('Desktop')
$LnkPath = Join-Path $Desktop 'HeavyDrops.lnk'
$ShellApp = New-Object -ComObject WScript.Shell
$Shortcut = $ShellApp.CreateShortcut($LnkPath)
$Shortcut.TargetPath  = Join-Path $VenvDir 'Scripts\hd-gui.exe'
$Shortcut.WorkingDirectory = $InstallDir
$Shortcut.IconLocation = "$env:SystemRoot\System32\imageres.dll,109"
$Shortcut.Description = 'Open the HeavyDrops transcoder dashboard.'
$Shortcut.Save()

Info ""
Info "Install complete."
Info "  Dashboard: http://127.0.0.1:9123/"
Info "  Shortcut:  $LnkPath"
Info "  Config:    $ConfigPath"
Info "  Logs:      $LogDir"
Info ""
if ($WorkerMode) {
    Info "This machine is set up as a NIGHT WORKER:"
    Info "  - encodes only 18:00-09:00, and pauses the instant someone uses it"
    Info "  - shares the Dropbox pool (no duplicate work) - same claims folder as the others"
    Info "  - NVIDIA NVENC, low disk footprint"
    Info "  Make sure it points at the SAME Dropbox account/folder as the dedicated box."
    Info "  Tweak anything from the dashboard's Settings > Fleet section - no files to edit."
    Info ""
}
Info "The daemon auto-starts at each logon. To stop or restart now:"
Info "  schtasks /End /TN $TaskName"
Info "  schtasks /Run /TN $TaskName"
