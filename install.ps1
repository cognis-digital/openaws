# openaws installer (Windows / PowerShell)
#
# Installs openaws from source (it is not published to PyPI). Prefers pipx,
# then uv, then a plain pip install. Set $env:OPENAWS_REF to pin a revision.

$ErrorActionPreference = "Stop"

$repo = "git+https://github.com/cognis-digital/openaws.git"
$ref  = $env:OPENAWS_REF
$spec = $repo
if ($ref) { $spec = "$repo@$ref" }

Write-Host "openaws installer"
Write-Host "source: $spec"

function Have($name) { $null -ne (Get-Command $name -ErrorAction SilentlyContinue) }

if (Have "pipx") {
    Write-Host "==> installing with pipx"
    pipx install $spec
} elseif (Have "uv") {
    Write-Host "==> installing with uv"
    uv tool install $spec
} elseif (Have "pip") {
    Write-Host "==> installing with pip"
    pip install $spec
} elseif (Have "python") {
    Write-Host "==> installing with python -m pip"
    python -m pip install $spec
} else {
    Write-Error "None of pipx, uv, pip, or python were found on PATH. Install Python 3.10+ first."
    exit 1
}

Write-Host ""
Write-Host "Installed. Try:"
Write-Host "  openaws serve"
Write-Host "  openaws --help"
