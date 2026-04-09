; Inno Setup Script for DocPro Agent
; Generate DocProInstaller.exe

[Setup]
AppName=DocPro Agent
AppVersion=1.2.0
AppPublisher=DocPro Solutions
AppPublisherURL=http://localhost:8000
AppSupportURL=http://localhost:8000
AppUpdatesURL=http://localhost:8000
DefaultDirName={pf}\DocPro Agent
DefaultGroupName=DocPro Agent
OutputBaseFilename=DocProInstaller
OutputDir=dist
Compression=lzma
SolidCompression=yes
AppMutex=DocProAgent_Instance_Mutex
SetupIconFile=..\assets\app.ico

[Code]
// Force-kill the process before install to avoid "File In Use" errors
function InitializeSetup(): Boolean;
var
  ResultCode: Integer;
begin
  Result := True;
  // Silent Force Kill
  Exec('taskkill.exe', '/f /im docpro_agent.exe', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

[Files]
Source: "..\dist\docpro_agent.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "config.example.json"; DestDir: "{app}"; DestName: "config.json"; Flags: onlyifdoesntexist

[Icons]
Name: "{group}\DocPro Agent"; Filename: "{app}\docpro_agent.exe"
Name: "{commondesktop}\DocPro Agent"; Filename: "{app}\docpro_agent.exe"

[Run]
Filename: "{app}\docpro_agent.exe"; Description: "Launch DocPro Agent"; Flags: nowait postinstall skipifsilent

[Registry]
Root: HKCR; Subkey: "docpro"; ValueType: string; ValueData: "URL:DocPro Protocol"; Flags: uninsdeletekey
Root: HKCR; Subkey: "docpro"; ValueName: "URL Protocol"; ValueType: string; ValueData: ""; Flags: uninsdeletekey
Root: HKCR; Subkey: "docpro\DefaultIcon"; ValueType: string; ValueData: "{app}\docpro_agent.exe,0"
Root: HKCR; Subkey: "docpro\shell\open\command"; ValueType: string; ValueData: """{app}\docpro_agent.exe"" ""%1"""

[UninstallDelete]
Type: files; Name: "{app}\config.json"
Type: files; Name: "{app}\agent.log"
