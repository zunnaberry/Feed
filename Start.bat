@echo off
cd /d C:\Workspace\Feed

:: Activate virtual environment (assumes it's in .\venv\Scripts\activate)
call .venv\Scripts\activate.bat

:: Run the Python script
python main.py

:: Optional: pause so you can see the output
pause
