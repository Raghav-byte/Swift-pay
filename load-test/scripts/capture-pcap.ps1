# Start a PCAP capture while you run k6 in another terminal.
# Requires Wireshark (tshark on PATH).

$resultsDir = Join-Path $PSScriptRoot "..\results"
New-Item -ItemType Directory -Force -Path $resultsDir | Out-Null
$outFile = Join-Path $resultsDir "swiftpay-load.pcapng"

Write-Host "Listing interfaces (pick loopback index for -i):"
tshark -D

Write-Host ""
Write-Host "Example (replace -i N with loopback index):"
Write-Host "  tshark -i N -f `"tcp port 8080`" -w `"$outFile`""
Write-Host ""
Write-Host "Then run k6, stop tshark with Ctrl+C, open the file in Wireshark."
Write-Host "Filter: http.request.uri contains `/v1/payments`"
