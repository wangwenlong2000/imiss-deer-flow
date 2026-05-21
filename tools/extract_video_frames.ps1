param(
  [Parameter(Mandatory=$true)][string]$VideoPath,
  [Parameter(Mandatory=$true)][string]$OutDir,
  [double[]]$Times = @(0,5,10,15,20,25,30,35,40,45,50,55,60,65)
)

Add-Type -AssemblyName PresentationCore
Add-Type -AssemblyName PresentationFramework
Add-Type -AssemblyName WindowsBase

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$player = New-Object System.Windows.Media.MediaPlayer
$player.Open([Uri]::new($VideoPath))
Start-Sleep -Milliseconds 1500

$width = [Math]::Max(1, [int]$player.NaturalVideoWidth)
$height = [Math]::Max(1, [int]$player.NaturalVideoHeight)

foreach ($time in $Times) {
  $player.Position = [TimeSpan]::FromSeconds($time)
  Start-Sleep -Milliseconds 700
  $drawingVisual = New-Object System.Windows.Media.DrawingVisual
  $context = $drawingVisual.RenderOpen()
  $context.DrawVideo($player, [Windows.Rect]::new(0, 0, $width, $height))
  $context.Close()

  $bitmap = New-Object System.Windows.Media.Imaging.RenderTargetBitmap(
    $width, $height, 96, 96,
    [System.Windows.Media.PixelFormats]::Pbgra32
  )
  $bitmap.Render($drawingVisual)

  $encoder = New-Object System.Windows.Media.Imaging.PngBitmapEncoder
  $encoder.Frames.Add([System.Windows.Media.Imaging.BitmapFrame]::Create($bitmap))
  $name = ('frame_{0:000.0}.png' -f $time).Replace('.', '_')
  $stream = [System.IO.File]::Create((Join-Path $OutDir $name))
  $encoder.Save($stream)
  $stream.Close()
}

$player.Close()
Write-Output "Extracted $($Times.Count) frame(s) at ${width}x${height}."
