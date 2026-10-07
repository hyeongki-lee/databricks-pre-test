# rclone 증분 복제 스크립트
# rcd (rclone remote control) 모드로 실행되어 API 형식으로 동작
# chk 파일 확인 후 copy 모드로 증분 복제 수행

param(
    [string]$SourcePath = "C:\Users\lee21\OneDrive\문서\Default Project\databricks-pre-test\data",
    [string]$S3Bucket = "databricks-test-lhk",
    [string]$RcHost = "http://localhost:5572",
    [string]$RcUser = "admin",
    [string]$RcPass = "rclone123"
)

# rcd 인증 토큰 생성
$pair = "$RcUser`:$RcPass"
$bytes = [System.Text.Encoding]::ASCII.GetBytes($pair)
$base64 = [Convert]::ToBase64String($bytes)
$authHeader = "Basic $base64"

# chk 파일 확인 함수
function Test-ChkFiles {
    param([string]$Path)
    
    $chkFiles = Get-ChildItem -Path $Path -Filter "chk" -Recurse -ErrorAction SilentlyContinue
    return $chkFiles.Count -gt 0
}

# rclone API 호출 함수
function Invoke-RcloneApi {
    param(
        [string]$Method,
        [string]$Body = "{}"
    )
    
    $headers = @{
        "Authorization" = $authHeader
        "Content-Type" = "application/json"
    }
    
    $response = Invoke-RestMethod -Uri "$RcHost/$Method" -Method Post -Headers $headers -Body $Body
    return $response
}

# 메인 로직
Write-Host "rclone 증분 복제 시작..."

# 1. chk 파일 확인
Write-Host "chk 파일 확인 중..."
if (-not (Test-ChkFiles -Path $SourcePath)) {
    Write-Host "chk 파일이 없습니다. 복제를 건너뜁니다."
    exit 0
}
Write-Host "chk 파일이 확인되었습니다. 복제를 시작합니다."

# 2. rclone copy 작업 실행 (증분 복제)
$copyParams = {
    "srcFs" = $SourcePath
    "dstFs" = "s3:$S3Bucket"
    "_async" = $true
} | ConvertTo-Json

Write-Host "rclone copy 작업 시작..."
try {
    $result = Invoke-RcloneApi -Method "operations/copyfile" -Body $copyParams
    Write-Host "복제 완료: $result"
} catch {
    Write-Host "복제 실패: $_"
    exit 1
}

Write-Host "rclone 증분 복제 완료"
