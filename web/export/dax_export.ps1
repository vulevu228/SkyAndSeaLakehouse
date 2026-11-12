# Read-only export of the Power BI model (Databricks gold) to JSON for the web app.
# Runs DAX queries against the local Analysis Services instance of an OPEN Power BI Desktop session.
# Needs Microsoft.AnalysisServices.AdomdClient.dll (NuGet: Microsoft.AnalysisServices.AdomdClient.retail.amd64, lib\net45) in .\lib
param([string]$OutDir = "$PSScriptRoot\..\data", [string]$QueryDir = "$PSScriptRoot\queries", [string]$AdomdDll = "$PSScriptRoot\lib\Microsoft.AnalysisServices.AdomdClient.dll")
$ErrorActionPreference = "Stop"
[void][Reflection.Assembly]::LoadFrom($AdomdDll)
$roots = @("$env:USERPROFILE\Microsoft\Power BI Desktop Store App\AnalysisServicesWorkspaces", "$env:LOCALAPPDATA\Microsoft\Power BI Desktop\AnalysisServicesWorkspaces")
$ws = $roots | Where-Object { Test-Path $_ } | ForEach-Object { Get-ChildItem $_ -Directory } |
      Where-Object { Test-Path (Join-Path $_.FullName "Data\msmdsrv.port.txt") } | Sort-Object LastWriteTime -Descending | Select-Object -First 1
$port = (Get-Content (Join-Path $ws.FullName "Data\msmdsrv.port.txt") -Encoding Unicode).Trim()
$cn = New-Object Microsoft.AnalysisServices.AdomdClient.AdomdConnection "Data Source=localhost:$port"
$cn.Open()
"connected to port $port, catalog $($cn.Database)"
New-Item -ItemType Directory -Force $OutDir | Out-Null
foreach ($q in Get-ChildItem $QueryDir -Filter *.dax) {
  $cmd = $cn.CreateCommand(); $cmd.CommandText = Get-Content $q.FullName -Raw
  $rd = $cmd.ExecuteReader()
  $names = 0..($rd.FieldCount - 1) | ForEach-Object { ($rd.GetName($_) -replace '^.*\[', '' -replace '\]$', '') }
  $rows = New-Object System.Collections.ArrayList
  while ($rd.Read()) {
    $o = [ordered]@{}
    for ($i = 0; $i -lt $rd.FieldCount; $i++) { $v = $rd.GetValue($i); if ($v -is [DBNull]) { $v = $null } elseif ($v -is [datetime]) { $v = $v.ToString("yyyy-MM-ddTHH:mm") }; $o[$names[$i]] = $v }
    [void]$rows.Add($o)
  }
  $rd.Close()
  $json = ConvertTo-Json -InputObject @($rows) -Depth 3 -Compress
  [IO.File]::WriteAllText((Join-Path $OutDir ($q.BaseName + ".json")), $json, (New-Object Text.UTF8Encoding $false))
  "{0}: {1} rows" -f $q.BaseName, $rows.Count
}
$cn.Close()
