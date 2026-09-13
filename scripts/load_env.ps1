# Exporta las variables de .env al proceso actual de PowerShell.
#
# dbt no lee .env (eso lo hace python-dotenv dentro de los scripts de Python),
# asi que antes de cualquier comando dbt hay que cargarlas en el entorno.
# Se invoca con punto y espacio, para que las variables queden en la sesion:
#
#     . .\scripts\load_env.ps1
#
# Nunca imprime valores: solo cuantas variables cargo y cuales quedaron vacias.

$envFile = Join-Path $PSScriptRoot '..\.env'
if (-not (Test-Path -LiteralPath $envFile)) {
    throw "No existe $envFile. Copiar .env.example a .env y completarlo."
}

$loaded = 0
$empty = @()
foreach ($line in Get-Content -LiteralPath $envFile -Encoding UTF8) {
    $trimmed = $line.Trim()
    if ($trimmed -eq '' -or $trimmed.StartsWith('#')) { continue }

    $separator = $trimmed.IndexOf('=')
    if ($separator -lt 1) { continue }

    $name = $trimmed.Substring(0, $separator).Trim()
    $value = $trimmed.Substring($separator + 1).Trim()
    if ($value.Length -ge 2 -and (
            ($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'")))) {
        $value = $value.Substring(1, $value.Length - 2)
    }

    # Vacia en .env = no definida. Evita que SNOWFLAKE_PRIVATE_KEY vacia conviva
    # con SNOWFLAKE_PRIVATE_KEY_PATH, y que LANDING_DIR vacia pise el default.
    if ($value -eq '') {
        Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
        $empty += $name
        continue
    }

    Set-Item -LiteralPath "Env:$name" -Value $value
    $loaded++
}

Write-Host "Variables cargadas desde .env: $loaded"
if ($empty.Count -gt 0) {
    Write-Host "Vacias (no definidas en la sesion): $($empty -join ', ')"
}
