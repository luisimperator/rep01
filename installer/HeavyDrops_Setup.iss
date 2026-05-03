; HeavyDrops Transcoder - Inno Setup Script
; Creates a professional Windows installer with wizard interface

#define MyAppName "HeavyDrops Transcoder"
#define MyAppVersion "6.5.1"
#define MyAppPublisher "HeavyDrops"
#define MyAppURL "https://github.com/luisimperator/rep01"
#define MyAppExeName "HeavyDrops_Transcoder.exe"

[Setup]
; NOTE: The value of AppId uniquely identifies this application.
AppId={{B8E5F7A2-3C4D-4E6F-8A9B-1C2D3E4F5A6B}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppVerName={#MyAppName} {#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
; Output settings
OutputDir=output
OutputBaseFilename=HeavyDrops_Transcoder_v{#MyAppVersion}_Setup
; Compression
Compression=lzma2/ultra64
SolidCompression=yes
; Require admin for FFmpeg installation
PrivilegesRequired=admin
; Wizard style
WizardStyle=modern
WizardSizePercent=110
; Uninstaller
UninstallDisplayIcon={app}\{#MyAppExeName}
; Minimum Windows version (Windows 10)
MinVersion=10.0

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"
Name: "portuguese"; MessagesFile: "compiler:Languages\Portuguese.isl"
Name: "brazilianportuguese"; MessagesFile: "compiler:Languages\BrazilianPortuguese.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "quicklaunchicon"; Description: "{cm:CreateQuickLaunchIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked; OnlyBelowVersion: 6.1; Check: not IsAdminInstallMode
Name: "installffmpeg"; Description: "Download and install FFmpeg (required)"; GroupDescription: "Dependencies:"; Flags: checkedonce

[Files]
; Main application
Source: "dist\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion
; Additional files if needed
; Source: "README.txt"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon
Name: "{userappdata}\Microsoft\Internet Explorer\Quick Launch\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: quicklaunchicon

[Run]
; Run application after install (optional)
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[Code]
var
  DownloadPage: TDownloadWizardPage;
  FFmpegInstalled: Boolean;

function IsFFmpegInstalled(): Boolean;
var
  ResultCode: Integer;
begin
  Result := Exec('cmd.exe', '/c ffmpeg -version', '', SW_HIDE, ewWaitUntilTerminated, ResultCode) and (ResultCode = 0);
end;

function OnDownloadProgress(const Url, FileName: String; const Progress, ProgressMax: Int64): Boolean;
begin
  if Progress = ProgressMax then
    Log(Format('Successfully downloaded file to {tmp}: %s', [FileName]));
  Result := True;
end;

procedure InitializeWizard;
begin
  FFmpegInstalled := IsFFmpegInstalled();

  DownloadPage := CreateDownloadPage(SetupMessage(msgWizardPreparing), SetupMessage(msgPreparingDesc), @OnDownloadProgress);
end;

function NextButtonClick(CurPageID: Integer): Boolean;
var
  ResultCode: Integer;
  FFmpegZip: String;
  FFmpegDir: String;
  FFmpegBin: String;
  OldPath: String;
  NewPath: String;
begin
  Result := True;

  if (CurPageID = wpReady) and WizardIsTaskSelected('installffmpeg') and (not FFmpegInstalled) then
  begin
    DownloadPage.Clear;
    DownloadPage.Add('https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip', 'ffmpeg.zip', '');
    DownloadPage.Show;
    try
      try
        DownloadPage.Download;

        FFmpegZip := ExpandConstant('{tmp}\ffmpeg.zip');
        FFmpegDir := ExpandConstant('{pf}\FFmpeg');
        FFmpegBin := FFmpegDir + '\bin';

        // Create FFmpeg directory
        if not DirExists(FFmpegDir) then
          ForceDirectories(FFmpegDir);

        // Extract using PowerShell
        DownloadPage.SetText('Extracting FFmpeg...', '');
        Exec('powershell.exe', '-ExecutionPolicy Bypass -Command "Expand-Archive -Path ''' + FFmpegZip + ''' -DestinationPath ''' + FFmpegDir + ''' -Force"', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);

        // Move files from nested folder to FFmpeg dir
        DownloadPage.SetText('Configuring FFmpeg...', '');
        Exec('powershell.exe', '-ExecutionPolicy Bypass -Command "Get-ChildItem ''' + FFmpegDir + ''' -Directory | ForEach-Object { Move-Item -Path $_.FullName\* -Destination ''' + FFmpegDir + ''' -Force; Remove-Item $_.FullName -Force }"', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);

        // Add to PATH
        if RegQueryStringValue(HKLM, 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment', 'Path', OldPath) then
        begin
          if Pos(FFmpegBin, OldPath) = 0 then
          begin
            NewPath := OldPath + ';' + FFmpegBin;
            RegWriteStringValue(HKLM, 'SYSTEM\CurrentControlSet\Control\Session Manager\Environment', 'Path', NewPath);
            Log('Added FFmpeg to PATH: ' + FFmpegBin);
          end;
        end;

        Result := True;
      except
        if DownloadPage.AbortedByUser then
          Log('Download aborted by user.')
        else
          SuppressibleMsgBox(AddPeriod(GetExceptionMessage), mbCriticalError, MB_OK, IDOK);
        Result := False;
      end;
    finally
      DownloadPage.Hide;
    end;
  end;
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    // Broadcast environment change to update PATH in running processes
    // This requires a logoff/logon to take full effect
  end;
end;

function UpdateReadyMemo(Space, NewLine, MemoUserInfoInfo, MemoDirInfo, MemoTypeInfo, MemoComponentsInfo, MemoGroupInfo, MemoTasksInfo: String): String;
var
  S: String;
begin
  S := '';

  S := S + 'Installation Directory:' + NewLine;
  S := S + Space + WizardDirValue + NewLine + NewLine;

  if WizardIsTaskSelected('installffmpeg') then
  begin
    if FFmpegInstalled then
      S := S + 'FFmpeg: Already installed (will skip download)' + NewLine
    else
      S := S + 'FFmpeg: Will be downloaded and installed' + NewLine;
  end;

  S := S + NewLine;
  S := S + 'Click Install to proceed with the installation.';

  Result := S;
end;
