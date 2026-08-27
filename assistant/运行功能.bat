@echo off
chcp 65001 >nul
title BetterGI 远程辅助程序
setlocal enabledelayedexpansion

:: ============================================================
:: BetterGI 远程辅助程序启动脚本（客户端模式）
:: ============================================================
:: 辅助程序主动连接 AstrBot，无需开放端口
:: 只需配置 AstrBot 地址即可

set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%"

:: ============================================================
:: 查找 Python 路径
:: ============================================================
set "PYTHON=python"
set "PYTHONW=pythonw"
where python >nul 2>nul
if errorlevel 1 (
    echo [错误] 未找到 Python，请先安装 Python 3.9+
    pause
    exit /b 1
)

:: 查找 pythonw 路径
for /f "delims=" %%i in ('where pythonw 2^>nul') do (
    set "PYTHONW=%%i"
    goto :found_pythonw
)
:found_pythonw

:: ============================================================
:: 从 config.yaml 读取 astrbot_url 用于连接测试
:: 注意：URL 含冒号，必须用 tokens=1,* 取冒号后的完整内容
:: ============================================================
set "ASTRBOT_URL=http://127.0.0.1:6185"
for /f "tokens=1,* delims=: " %%a in ('findstr /i "^astrbot_url:" "%SCRIPT_DIR%config.yaml" 2^>nul') do (
    set "ASTRBOT_URL=%%b"
)
:: 去除引号和空格
set "ASTRBOT_URL=%ASTRBOT_URL:"=%"
set "ASTRBOT_URL=%ASTRBOT_URL: =%"

:menu
cls
echo ============================================================
echo   BetterGI 远程辅助程序（客户端模式）
echo ============================================================
echo   AstrBot 地址: %ASTRBOT_URL%
echo ============================================================
echo.
echo   1. 启动辅助程序（前台）
echo   2. 注册开机自启（后台运行）
echo   3. 取消开机自启
echo   4. 查看自启状态
echo   5. 测试连接 AstrBot
echo   6. 退出
echo.
set /p choice=请选择操作 (1-6): 

if "%choice%"=="1" goto start_foreground
if "%choice%"=="2" goto install_autostart
if "%choice%"=="3" goto uninstall_autostart
if "%choice%"=="4" goto check_autostart
if "%choice%"=="5" goto test_connection
if "%choice%"=="6" goto end
goto menu

:: ============================================================
:: 1. 启动辅助程序（前台）
:: ============================================================
:start_foreground
echo.
echo 正在启动辅助程序（前台模式）...
echo 按 Ctrl+C 停止
echo.
"%PYTHON%" "%SCRIPT_DIR%server.py"
echo.
echo 辅助程序已停止
pause
goto menu

:: ============================================================
:: 2. 注册开机自启
:: ============================================================
:install_autostart
echo.
echo 正在注册开机自启...

schtasks /create /tn "BetterGIAssistant" /tr "\"%PYTHONW%\" \"%SCRIPT_DIR%server.py\"" /sc onlogon /rl highest /f >nul 2>&1

if errorlevel 1 (
    echo [失败] 注册失败，请以管理员身份运行
) else (
    echo [成功] 已注册开机自启，登录时自动后台运行
)
echo.
pause
goto menu

:: ============================================================
:: 3. 取消开机自启
:: ============================================================
:uninstall_autostart
echo.
echo 正在取消开机自启...

schtasks /delete /tn "BetterGIAssistant" /f >nul 2>&1

if errorlevel 1 (
    echo [失败] 任务不存在或删除失败
) else (
    echo [成功] 已取消开机自启
)
echo.
pause
goto menu

:: ============================================================
:: 4. 查看自启状态
:: ============================================================
:check_autostart
echo.
echo 正在查询自启状态...

schtasks /query /tn "BetterGIAssistant" /v /fo list 2>nul | findstr /i "任务名 状态 上次运行" >nul

if errorlevel 1 (
    echo   状态: 未注册
) else (
    echo   状态: 已注册
    schtasks /query /tn "BetterGIAssistant" /v /fo list 2>nul | findstr /i "状态 上次运行"
)
echo.
pause
goto menu

:: ============================================================
:: 5. 测试连接 AstrBot
:: ============================================================
:test_connection
echo.
echo 正在测试连接 AstrBot...
echo   地址: %ASTRBOT_URL%
echo.

:: 先检查配置文件
if not exist "%SCRIPT_DIR%config.yaml" (
    echo [错误] 配置文件 config.yaml 不存在
    pause
    goto menu
)

if "%ASTRBOT_URL%"=="" (
    echo [错误] 配置文件中 astrbot_url 为空
    pause
    goto menu
)

:: 调用健康检查接口
set "HEALTH_URL=%ASTRBOT_URL%/api/v1/plugins/extensions/bettergi/remote/health"

powershell -Command "try { $r = Invoke-WebRequest -Uri '%HEALTH_URL%' -TimeoutSec 5 -UseBasicParsing; Write-Host '  连接成功'; Write-Host '  响应:' $r.Content } catch { Write-Host '  连接失败'; Write-Host '  可能原因:'; Write-Host '    1. AstrBot 未启动'; Write-Host '    2. astrbot_url 配置错误'; Write-Host '    3. 网络不通'; Write-Host '    4. 插件未加载' }"

echo.
pause
goto menu

:end
endlocal
