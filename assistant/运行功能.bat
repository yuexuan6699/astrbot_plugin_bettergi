@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

:: BetterGI 辅助程序 运行功能
:: 交互式菜单：启动、注册开机自启、取消开机自启、查看状态

set "TASK_NAME=BetterGI_Assistant"
set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

:menu
cls
echo ╔══════════════════════════════════════╗
echo ║    BetterGI 辅助程序 运行功能       ║
echo ╠══════════════════════════════════════╣
echo ║                                      ║
echo ║  1. 启动辅助程序                     ║
echo ║  2. 注册开机自启                     ║
echo ║  3. 取消开机自启                     ║
echo ║  4. 查看自启状态                     ║
echo ║  5. 查看运行状态                     ║
echo ║  6. 退出                             ║
echo ║                                      ║
echo ╚══════════════════════════════════════╝
echo.
set /p choice=请选择 [1-6]:

if "%choice%"=="1" goto start
if "%choice%"=="2" goto register
if "%choice%"=="3" goto unregister
if "%choice%"=="4" goto status
if "%choice%"=="5" goto health
if "%choice%"=="6" exit
goto menu

:: ============================================================
:: 1. 启动辅助程序
:: ============================================================
:start
echo.
echo 正在检查 Python 环境...

:: 查找 python（前台运行，可看到输出）
set "PYTHON="
for /f "delims=" %%i in ('where python 2^>nul') do (
    if not defined PYTHON set "PYTHON=%%i"
)
if not defined PYTHON (
    echo [错误] 未找到 python，请确保 Python 已安装并添加到系统 PATH
    echo.
    pause
    goto menu
)

echo 正在启动辅助程序...
echo 按 Ctrl+C 可停止程序
echo.

:: 检查 config.yaml 是否存在
if not exist "%SCRIPT_DIR%\config.yaml" (
    echo [错误] 未找到 config.yaml，请确保配置文件与 运行功能.bat 在同一目录
    echo.
    pause
    goto menu
)

"%PYTHON%" "%SCRIPT_DIR%\server.py"
echo.
echo 辅助程序已停止
pause
goto menu

:: ============================================================
:: 2. 注册开机自启（Windows 计划任务）
:: ============================================================
:register
echo.
echo 正在查找 Python 环境...

:: 查找 pythonw（后台运行，无窗口）
set "PYTHONW="
for /f "delims=" %%i in ('where pythonw 2^>nul') do (
    if not defined PYTHONW set "PYTHONW=%%i"
)
if not defined PYTHONW (
    echo [错误] 未找到 pythonw，请确保 Python 已安装并添加到系统 PATH
    echo.
    pause
    goto menu
)

:: 检查 config.yaml 是否存在
if not exist "%SCRIPT_DIR%\config.yaml" (
    echo [错误] 未找到 config.yaml，请确保配置文件与 运行功能.bat 在同一目录
    echo.
    pause
    goto menu
)

:: 检查是否已注册
schtasks /query /tn "%TASK_NAME%" >nul 2>&1
if %errorlevel% equ 0 (
    echo [提示] 开机自启已存在，正在重新注册...
    schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1
)

:: 创建计划任务（登录时运行，最高权限）
schtasks /create /tn "%TASK_NAME%" /tr "\"%PYTHONW%\" \"%SCRIPT_DIR%\server.py\"" /sc onlogon /rl highest /f >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 注册开机自启失败，请尝试右键以管理员身份运行此脚本
    echo.
    pause
    goto menu
)

echo [成功] 开机自启已注册！
echo.
echo  任务名称: %TASK_NAME%
echo  运行方式: pythonw（后台运行，无窗口）
echo  触发条件: 用户登录时
echo  脚本路径: %SCRIPT_DIR%\server.py
echo.
echo  提示: 现在可以选择 [1] 立即启动，或重新登录后自动启动
echo.
pause
goto menu

:: ============================================================
:: 3. 取消开机自启
:: ============================================================
:unregister
echo.
schtasks /query /tn "%TASK_NAME%" >nul 2>&1
if %errorlevel% neq 0 (
    echo [提示] 未找到开机自启任务，无需取消
    echo.
    pause
    goto menu
)

schtasks /delete /tn "%TASK_NAME%" /f >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 取消开机自启失败，请尝试右键以管理员身份运行此脚本
    echo.
    pause
    goto menu
)

echo [成功] 开机自启已取消！
echo.
pause
goto menu

:: ============================================================
:: 4. 查看自启状态
:: ============================================================
:status
echo.
schtasks /query /tn "%TASK_NAME%" >nul 2>&1
if %errorlevel% neq 0 (
    echo  开机自启: 未注册
    echo.
    pause
    goto menu
)

echo  开机自启: 已注册
echo  ─────────────────────────────
schtasks /query /tn "%TASK_NAME%" /fo list 2>nul | findstr /i "TaskName Status RunAsUser"
echo  ─────────────────────────────
echo.
pause
goto menu

:: ============================================================
:: 5. 查看运行状态（调用 /health 接口）
:: ============================================================
:health
echo.

:: 检查 config.yaml 是否存在
if not exist "%SCRIPT_DIR%\config.yaml" (
    echo [错误] 未找到 config.yaml，请确保配置文件与 运行功能.bat 在同一目录
    echo.
    pause
    goto menu
)

:: 从 config.yaml 读取端口
set "PORT=9099"
for /f "tokens=2 delims=: " %%a in ('findstr /i "^port:" "%SCRIPT_DIR%\config.yaml" 2^>nul') do (
    set "PORT=%%a"
)
:: 去除可能的引号和空格
set "PORT=%PORT:"=%"
set "PORT=%PORT: =%"

if not defined PORT (
    echo [错误] 无法从 config.yaml 读取端口，请检查配置文件
    echo.
    pause
    goto menu
)

echo 正在检查辅助程序运行状态...
echo  请求地址: http://127.0.0.1:%PORT%/health
echo.

:: 尝试请求 /health 接口
powershell -Command "try { $r = Invoke-WebRequest -Uri 'http://127.0.0.1:%PORT%/health' -TimeoutSec 3 -UseBasicParsing; Write-Host '  运行状态: 运行中'; Write-Host '  响应内容:' $r.Content } catch { Write-Host '  运行状态: 未运行'; Write-Host ''; Write-Host '  可能原因:'; Write-Host '    1. 辅助程序尚未启动，请先选择 [1] 启动'; Write-Host '    2. 端口被其他程序占用'; Write-Host '    3. 防火墙阻止了本地访问' }"
echo.
pause
goto menu
