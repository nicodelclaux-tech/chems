<#
  _update_testdata.ps1
  Pulls real NL chemicals data from public APIs and writes testdata/
  Sources:
    - Eurostat JSON-stat API   (chem_output_idx, chem_ppi_idx)
    - FRED CSV downloads        (brent_usd_per_bbl, ttf_gas_usd_per_mmbtu)
    - Eurostat teibs070 (hardcoded; only available from 2023-Q2 onwards)
#>
param(
    [string]$StartDate = '2015-01-01'
)
Set-StrictMode -Version 1
$ErrorActionPreference = 'Stop'

$WorkDir  = Split-Path $PSScriptRoot -Parent
$OutCsv   = Join-Path $WorkDir 'testdata\nl_chem_inputs_monthly.csv'
$OutMeta  = Join-Path $WorkDir 'testdata\pull_metadata.json'
$StartDt  = [DateTime]::ParseExact($StartDate, 'yyyy-MM-dd', $null)

# ── helpers ──────────────────────────────────────────────────────────────────

function MonthEnd([int]$y, [int]$m) {
    $d = [DateTime]::DaysInMonth($y, $m)
    return '{0}-{1:D2}-{2:D2}' -f $y, $m, $d
}

# Fetch Eurostat JSON-stat filtered to a single time series (all other dims pinned)
function Get-EurostatSeries([string]$Url) {
    Write-Host "  GET $Url"
    $r = Invoke-RestMethod -Uri $Url -UserAgent 'NL-Chem-Pull/2.0' -TimeoutSec 90

    # time index: period-string -> integer position
    $tIdx = @{}
    $r.dimension.time.category.index.PSObject.Properties |
        ForEach-Object { $tIdx[$_.Name] = [int]$_.Value }

    # value dict: integer position -> double
    $vals = @{}
    $r.value.PSObject.Properties |
        ForEach-Object { $vals[[int]$_.Name] = [double]$_.Value }

    $h = @{}
    foreach ($kv in $tIdx.GetEnumerator()) {
        if (-not $vals.ContainsKey($kv.Value)) { continue }
        if ($kv.Key -notmatch '^(\d{4})-(\d{2})$') { continue }
        $key = MonthEnd ([int]$Matches[1]) ([int]$Matches[2])
        $h[$key] = $vals[$kv.Value]
    }
    return $h
}

# ── pull ──────────────────────────────────────────────────────────────────────

Write-Host 'Pulling chem_output_idx (Eurostat sts_inpr_m, NL, C20, SCA, I21)...'
$coH = Get-EurostatSeries `
    'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sts_inpr_m?lang=EN&geo=NL&nace_r2=C20&freq=M&s_adj=SCA&unit=I21'
Write-Host "  -> $($coH.Count) months"

Write-Host 'Pulling chem_ppi_idx (Eurostat sts_inpp_m, NL, C20, NSA, I21)...'
$ppiH = Get-EurostatSeries `
    'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sts_inpp_m?lang=EN&geo=NL&nace_r2=C20&freq=M&s_adj=NSA&unit=I21'
Write-Host "  -> $($ppiH.Count) months"

# ── EIA Brent crude (hard-coded from EIA HTML fetch 2026-04-16) ──────────────
# Source: https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?f=m&n=pet&s=rbrte
Write-Host 'Loading brent_usd_per_bbl (EIA, hard-coded 2015-2026)...'
$brentRaw = @{
    '2015'=@(47.76,58.10,55.89,59.52,64.08,61.48,56.56,46.52,47.62,48.43,44.27,38.01)
    '2016'=@(30.70,32.18,38.21,41.58,46.74,48.25,44.95,45.84,46.57,49.52,44.73,53.31)
    '2017'=@(54.58,54.87,51.59,52.31,50.33,46.37,48.48,51.70,56.15,57.51,62.71,64.37)
    '2018'=@(69.08,65.32,66.02,72.11,76.98,74.41,74.25,72.53,78.89,81.03,64.75,57.36)
    '2019'=@(59.41,63.96,66.14,71.23,71.32,64.22,63.92,59.04,62.83,59.71,63.21,67.31)
    '2020'=@(63.65,55.66,32.01,18.38,29.38,40.27,43.24,44.74,40.91,40.19,42.69,49.99)
    '2021'=@(54.77,62.28,65.41,64.81,68.53,73.16,75.17,70.75,74.49,83.54,81.05,74.17)
    '2022'=@(86.51,97.13,117.25,104.58,113.34,122.71,111.93,100.45,89.76,93.33,91.42,80.92)
    '2023'=@(82.50,82.59,78.43,84.64,75.47,74.84,80.11,86.15,93.72,90.60,82.94,77.63)
    '2024'=@(80.12,83.48,85.41,89.94,81.75,82.25,85.15,80.36,74.02,75.63,74.35,73.86)
    '2025'=@(79.27,75.44,72.73,68.13,64.45,71.44,71.04,67.87,67.99,64.54,63.80,62.54)
    '2026'=@(66.60,70.89,103.13)
}
$brH = @{}
foreach ($yr in $brentRaw.Keys) {
    $vals = $brentRaw[$yr]
    for ($m = 1; $m -le $vals.Count; $m++) {
        $key = MonthEnd ([int]$yr) $m
        $brH[$key] = $vals[$m-1]
    }
}
Write-Host "  -> $($brH.Count) months"

# ── World Bank TTF gas price (from CMO Monthly XLSX) ─────────────────────────
Write-Host 'Pulling ttf_gas_usd_per_mmbtu (World Bank CMO Monthly XLSX)...'
function Get-WorldBankTTF {
    $xlsxUrl = 'https://thedocs.worldbank.org/en/doc/74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/CMO-Historical-Data-Monthly.xlsx'
    Write-Host "  GET $xlsxUrl"
    $tmp = [System.IO.Path]::GetTempPath() + 'cmo_monthly.xlsx'
    Invoke-WebRequest -Uri $xlsxUrl -OutFile $tmp -UseBasicParsing -TimeoutSec 90

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [System.IO.Compression.ZipFile]::OpenRead($tmp)

    # Load shared strings (handle both plain <si><t> and rich-text <si><r><t> elements)
    $ssEntry = $zip.Entries | Where-Object { $_.FullName -eq 'xl/sharedStrings.xml' }
    $ssXml = [xml]([System.IO.StreamReader]::new($ssEntry.Open()).ReadToEnd())
    $strings = @( $ssXml.sst.si | ForEach-Object {
        if ($null -ne $_.t) { $_.t }
        elseif ($null -ne $_.r) { ($_.r | ForEach-Object { $_.t }) -join '' }
        else { '' }
    })

    # Find "Natural gas, Europe" spot price string (not the composite index)
    $gasIdx = -1
    for ($i = 0; $i -lt $strings.Count; $i++) {
        $s = $strings[$i]
        # Match spot price label: contains "natural gas" + "europe", short, no "index"/"average"/"LNG"
        if ($s -match '(?i)natural\s+gas' -and $s -match '(?i)europe' -and
            $s -notmatch '(?i)index' -and $s -notmatch '(?i)average' -and
            $s -notmatch '(?i)LNG' -and $s.Length -lt 80) {
            $gasIdx = $i; break
        }
    }
    if ($gasIdx -lt 0) {
        # fallback: any natural gas + europe string
        for ($i = 0; $i -lt $strings.Count; $i++) {
            if ($strings[$i] -match '(?i)natural\s+gas' -and $strings[$i] -match '(?i)europe') {
                $gasIdx = $i; break
            }
        }
    }
    Write-Host "  Gas string index: $gasIdx  = '$($strings[$gasIdx])'"

    $h = @{}
    # Sheet structure: row=date (col A = "YYYYMmm"), columns=commodities (row 5 = labels, rows 7+ = data)
    $wsEntry = $zip.Entries | Where-Object { $_.FullName -match '^xl/worksheets/sheet\d+\.xml$' } |
        Where-Object {
            $c = [System.IO.StreamReader]::new($_.Open()).ReadToEnd()
            $c -match "v>$gasIdx</v>"
        } | Select-Object -First 1

    if ($null -ne $wsEntry) {
        $wsXml = [xml]([System.IO.StreamReader]::new($wsEntry.Open()).ReadToEnd())
        $ns = [System.Xml.XmlNamespaceManager]::new($wsXml.NameTable)
        $ns.AddNamespace('x', 'http://schemas.openxmlformats.org/spreadsheetml/2006/main')

        # Find the column letter where gasIdx appears in the header row
        $gasCol = $null
        $labelRow = $wsXml.SelectSingleNode("//x:row[x:c[x:v='$gasIdx'][@t='s']]", $ns)
        if ($null -ne $labelRow) {
            foreach ($cell in $labelRow.SelectNodes('x:c', $ns)) {
                $vNode = $cell.SelectSingleNode('x:v', $ns)
                if ($null -ne $vNode -and $cell.GetAttribute('t') -eq 's' -and [int]$vNode.'#text' -eq $gasIdx) {
                    $gasCol = $cell.GetAttribute('r') -replace '\d+', ''
                    break
                }
            }
        }
        Write-Host "  Gas column: $gasCol"

        if ($null -ne $gasCol) {
            # Iterate data rows: col A has date string "YYYYMmm", gasCol has price
            foreach ($row in $wsXml.SelectNodes('//x:row', $ns)) {
                $rowNum = [int]$row.GetAttribute('r')
                if ($rowNum -lt 7) { continue }
                $aCell = $row.SelectSingleNode("x:c[starts-with(@r,'A')]", $ns)
                if ($null -eq $aCell) { continue }
                $aV = $aCell.SelectSingleNode('x:v', $ns)
                if ($null -eq $aV -or $aCell.GetAttribute('t') -ne 's') { continue }
                $dateStr = $strings[[int]$aV.'#text']
                if ($null -eq $dateStr -or $dateStr -notmatch '^(\d{4})M(\d{2})$') { continue }
                $yr = [int]$Matches[1]; $mo = [int]$Matches[2]

                $gCell = $row.SelectSingleNode("x:c[starts-with(@r,'$gasCol')]", $ns)
                if ($null -eq $gCell) { continue }
                $gV = $gCell.SelectSingleNode('x:v', $ns)
                if ($null -eq $gV) { continue }
                $v = 0.0
                if ([double]::TryParse($gV.'#text', [ref]$v) -and $v -gt 0) {
                    $h[(MonthEnd $yr $mo)] = $v
                }
            }
        }
    }
    $zip.Dispose()
    Remove-Item $tmp -ErrorAction SilentlyContinue
    return $h
}

$ttfH = Get-WorldBankTTF
Write-Host "  -> $($ttfH.Count) months"

# ── capacity utilisation – teibs070 (2023-Q2 to 2026-Q1 only) ────────────────
# Values from Eurostat teibs070 (fetched 2026-04-16):
#   2023-Q2=82.5  2023-Q3=81.6  2023-Q4=81.0
#   2024-Q1=78.4  2024-Q2=79.6  2024-Q3=78.0  2024-Q4=77.1
#   2025-Q1=71.2  2025-Q2=77.5  2025-Q3=77.7  2025-Q4=77.2
#   2026-Q1=77.9
$capH = @{
    '2023-04-30'=82.5; '2023-05-31'=82.5; '2023-06-30'=82.5
    '2023-07-31'=81.6; '2023-08-31'=81.6; '2023-09-30'=81.6
    '2023-10-31'=81.0; '2023-11-30'=81.0; '2023-12-31'=81.0
    '2024-01-31'=78.4; '2024-02-29'=78.4; '2024-03-31'=78.4
    '2024-04-30'=79.6; '2024-05-31'=79.6; '2024-06-30'=79.6
    '2024-07-31'=78.0; '2024-08-31'=78.0; '2024-09-30'=78.0
    '2024-10-31'=77.1; '2024-11-30'=77.1; '2024-12-31'=77.1
    '2025-01-31'=71.2; '2025-02-28'=71.2; '2025-03-31'=71.2
    '2025-04-30'=77.5; '2025-05-31'=77.5; '2025-06-30'=77.5
    '2025-07-31'=77.7; '2025-08-31'=77.7; '2025-09-30'=77.7
    '2025-10-31'=77.2; '2025-11-30'=77.2; '2025-12-31'=77.2
    '2026-01-31'=77.9; '2026-02-28'=77.9; '2026-03-31'=77.9
}

# ── merge ─────────────────────────────────────────────────────────────────────

Write-Host 'Merging series...'

# Master date list: union of all series, filtered >= StartDate
$allDates = ($coH.Keys + $ppiH.Keys + $brH.Keys + $ttfH.Keys) |
    Sort-Object -Unique |
    Where-Object { [DateTime]::ParseExact($_, 'yyyy-MM-dd', $null) -ge $StartDt }

$lines = [System.Collections.Generic.List[string]]::new()
$lines.Add('date,chem_output_idx,chem_ppi_idx,brent_usd_per_bbl,ttf_gas_usd_per_mmbtu,capacity_util_pct')

foreach ($d in $allDates) {
    $co  = if ($coH.ContainsKey($d))  { $coH[$d]  } else { $null }
    $ppi = if ($ppiH.ContainsKey($d)) { $ppiH[$d] } else { $null }
    # skip dates where both primary Eurostat series are missing
    if (($null -eq $co) -and ($null -eq $ppi)) { continue }
    $br  = if ($brH.ContainsKey($d))  { $brH[$d]  } else { '' }
    $ttf = if ($ttfH.ContainsKey($d)) { $ttfH[$d] } else { '' }
    $cap = if ($capH.ContainsKey($d)) { $capH[$d] } else { '' }
    $coStr  = if ($null -ne $co)  { $co  } else { '' }
    $ppiStr = if ($null -ne $ppi) { $ppi } else { '' }
    $lines.Add("$d,$coStr,$ppiStr,$br,$ttf,$cap")
}

[System.IO.File]::WriteAllLines($OutCsv, $lines, [System.Text.Encoding]::UTF8)
Write-Host "Wrote $($lines.Count - 1) rows -> $OutCsv"

# ── metadata ──────────────────────────────────────────────────────────────────

$meta = [ordered]@{
    pulled_at_utc = [DateTime]::UtcNow.ToString('o')
    country       = 'NL'
    series        = [ordered]@{
        chem_output = [ordered]@{
            source  = 'Eurostat'
            dataset = 'sts_inpr_m'
            filters = [ordered]@{ geo='NL'; nace_r2='C20'; freq='M'; s_adj='SCA'; unit='I21' }
        }
        chem_ppi = [ordered]@{
            source  = 'Eurostat'
            dataset = 'sts_inpp_m'
            filters = [ordered]@{ geo='NL'; nace_r2='C20'; freq='M'; s_adj='NSA'; unit='I21' }
        }
        brent = [ordered]@{
            source = 'U.S. Energy Information Administration'
            url    = 'https://www.eia.gov/dnav/pet/hist/LeafHandler.ashx?f=m&n=pet&s=rbrte'
            note   = 'Europe Brent Spot Price FOB, USD/barrel'
        }
        ttf_gas = [ordered]@{
            source = 'World Bank Pink Sheet'
            url    = 'https://thedocs.worldbank.org/en/doc/74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/CMO-Historical-Data-Monthly.xlsx'
            note   = 'Natural gas, Europe (TTF), USD/mmbtu'
        }
        capacity_util = [ordered]@{
            source  = 'Eurostat'
            dataset = 'teibs070'
            filters = [ordered]@{ geo='NL'; freq='Q' }
            note    = 'Only available from 2023-Q2 onwards; quarterly value repeated across each quarter month'
        }
    }
}
$meta | ConvertTo-Json -Depth 5 | Set-Content -Path $OutMeta -Encoding UTF8
Write-Host "Wrote metadata -> $OutMeta"
Write-Host 'Done.'
