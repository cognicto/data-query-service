# Windows Setup Guide - Sensor Data Query Service

This guide walks you through setting up the Sensor Data Query Service on Windows 10/11.

## Prerequisites

### 1. Install Python 3.9+
- Download from [python.org](https://www.python.org/downloads/)
- **Important**: Check "Add Python to PATH" during installation
- Verify installation:
```cmd
python --version
pip --version
```

### 2. Install Git (Optional but Recommended)
- Download from [git-scm.com](https://git-scm.com/download/win)
- Use default installation settings

### 3. Install Visual Studio Build Tools (For Azure dependencies)
- Download "Microsoft C++ Build Tools" from [Visual Studio Downloads](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
- Select "C++ build tools" workload during installation
- This is required for some Python packages with C extensions

## Setup Instructions

### Step 1: Clone or Download the Repository
```cmd
# Option A: Using Git (if installed)
git clone <repository-url>
cd sensor-data-query-service

# Option B: Download ZIP and extract
# Download the ZIP file and extract to a folder like C:\sensor-data-query-service
# Open Command Prompt and navigate to the folder:
cd C:\sensor-data-query-service
```

### Step 2: Create Python Virtual Environment
```cmd
# Create virtual environment
python -m venv venv

# Activate virtual environment
venv\Scripts\activate

# Your prompt should now show (venv) indicating the virtual environment is active
```

### Step 3: Install Dependencies
```cmd
# Upgrade pip first
python -m pip install --upgrade pip

# Install project dependencies
pip install -r requirements.txt

# If you encounter issues with Azure dependencies, try:
pip install --upgrade azure-storage-blob azure-core
```

### Step 4: Configure Environment Variables

Create a `.env` file by copying the example:
```cmd
copy .env.example .env
```

Edit the `.env` file using Notepad or any text editor:
```cmd
notepad .env
```

**Configure these key settings:**

For **Local Storage Mode** (recommended for testing):
```env
STORAGE_MODE=local
LOCAL_STORAGE_PATH=C:\sensor-data\raw
AZURE_STORAGE_ACCOUNT=
AZURE_STORAGE_KEY=
```

For **Azure Storage Mode**:
```env
STORAGE_MODE=azure
AZURE_STORAGE_ACCOUNT=your_storage_account_name
AZURE_STORAGE_KEY=your_storage_account_key
AZURE_CONTAINER_NAME=sensor-data-cold-storage
```

For **Hybrid Mode** (uses both):
```env
STORAGE_MODE=hybrid
LOCAL_STORAGE_PATH=C:\sensor-data\raw
AZURE_STORAGE_ACCOUNT=your_storage_account_name
AZURE_STORAGE_KEY=your_storage_account_key
```

### Step 5: Create Data Directory (for local storage)
```cmd
# Create the data directory
mkdir C:\sensor-data\raw

# Create sample directory structure
mkdir C:\sensor-data\raw\asset_001
mkdir C:\sensor-data\raw\asset_001\2024\01\15\14
```

### Step 6: Verify Installation
```cmd
# Run tests to verify everything works
python -m pytest tests/ -v

# Check if the service can start
python -c "from app.main import main; print('✓ Import successful')"
```

### Step 7: Run the Service
```cmd
# Start the development server
python -m app.main

# Or use the make-like commands with Python
python -c "import subprocess; subprocess.run(['python', '-m', 'uvicorn', 'app.main:create_test_service', '--host', '0.0.0.0', '--port', '8080', '--reload'])"
```

The service will be available at:
- **API Documentation**: http://localhost:8080/docs
- **Health Check**: http://localhost:8080/health
- **Main API**: http://localhost:8080/api/v1/

### Step 8: Test the Service
```cmd
# Run integration tests
python scripts/test-queries.py

# Or test manually with PowerShell:
# Invoke-WebRequest -Uri "http://localhost:8080/health" -Method GET
```

## Windows-Specific Notes

### Using PowerShell Instead of Command Prompt
If you prefer PowerShell, use these activation commands:
```powershell
# Activate virtual environment in PowerShell
venv\Scripts\Activate.ps1

# If you get execution policy error, run:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Path Issues
- Use forward slashes `/` or double backslashes `\\\\` in configuration paths
- Example: `LOCAL_STORAGE_PATH=C:/sensor-data/raw` or `C:\\\\sensor-data\\\\raw`

### Windows Firewall
If you plan to access the service from other machines:
1. Go to Windows Firewall settings
2. Allow Python or the specific port (8080) through the firewall

### Performance on Windows
- For better performance, consider using WSL2 (Windows Subsystem for Linux)
- Native Windows performance is good for development and small deployments

## Make-style Commands for Windows

Since Windows doesn't have `make`, create a `run.bat` file for common commands:

**Create `run.bat`:**
```batch
@echo off
if "%1"=="install" (
    pip install -r requirements.txt
) else if "%1"=="test" (
    python -m pytest tests/ -v
) else if "%1"=="run" (
    python -m app.main
) else if "%1"=="format" (
    python -m black app/ tests/ scripts/
    python -m isort app/ tests/ scripts/
) else if "%1"=="lint" (
    python -m flake8 app/ tests/
    python -m mypy app/
) else if "%1"=="test-api" (
    python scripts/test-queries.py
) else (
    echo Usage: run.bat [install^|test^|run^|format^|lint^|test-api]
)
```

**Usage:**
```cmd
run.bat install
run.bat test
run.bat run
```

## Troubleshooting

### Common Issues

**"Python is not recognized"**
- Reinstall Python with "Add to PATH" checked
- Or manually add Python to your PATH environment variable

**Azure dependencies fail to install**
- Install Visual Studio Build Tools
- Try: `pip install --only-binary=all azure-storage-blob`

**Permission denied errors**
- Run Command Prompt as Administrator
- Or use `--user` flag: `pip install --user -r requirements.txt`

**Import errors**
- Ensure virtual environment is activated (`venv` should show in prompt)
- Verify all dependencies installed: `pip list`

**Service won't start**
- Check if port 8080 is in use: `netstat -an | findstr 8080`
- Try a different port in `.env`: `API_PORT=8081`

### Getting Help

1. Check the logs in `C:\temp\query-service.log`
2. Run health check: `curl http://localhost:8080/health` or visit in browser
3. Enable debug mode in `.env`: `API_DEBUG=true`

## Development Workflow

1. **Always activate virtual environment first**:
   ```cmd
   venv\Scripts\activate
   ```

2. **Make changes to code**

3. **Run tests**:
   ```cmd
   run.bat test
   ```

4. **Format code**:
   ```cmd
   run.bat format
   ```

5. **Test the API**:
   ```cmd
   run.bat test-api
   ```

6. **Restart service** (Ctrl+C to stop, then `run.bat run`)

## Production Deployment on Windows

For production on Windows Server:

1. **Install Python as a Windows Service** using tools like `python-windows-service`
2. **Use IIS with FastCGI** for better performance
3. **Configure Windows Task Scheduler** for automated starts
4. **Set up log rotation** using Windows tools
5. **Configure Windows Firewall** properly

## Optional: Docker on Windows

If you have Docker Desktop for Windows:

```cmd
# Build image
docker build -t sensor-query-service .

# Run container
docker run -p 8080:8080 --env-file .env sensor-query-service

# Or use docker-compose
docker-compose up -d
```

This provides a Linux environment that matches the production deployment exactly.