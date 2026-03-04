#!/usr/bin/env python3
"""
AnyGen OpenAPI Client

Usage:
    python3 anygen.py create --api-key sk-xxx --operation slide --prompt "..."
    python3 anygen.py poll --api-key sk-xxx --task-id task_xxx
    python3 anygen.py download --api-key sk-xxx --task-id task_xxx --output ./
    python3 anygen.py run --api-key sk-xxx --operation slide --prompt "..." --output ./
"""

import argparse
import base64
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("[ERROR] requests library not found. Install with: pip3 install requests")
    sys.exit(1)


API_BASE = "https://www.anygen.io"
POLL_INTERVAL = 3  # seconds
MAX_POLL_TIME = 600  # 10 minutes
CONFIG_DIR = Path.home() / ".config" / "anygen"
CONFIG_FILE = CONFIG_DIR / "config.json"
ENV_API_KEY = "ANYGEN_API_KEY"


def load_config():
    """Load configuration from file."""
    if not CONFIG_FILE.exists():
        return {}
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_config(config):
    """Save configuration to file."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)
    # Set file permissions to owner read/write only (600)
    CONFIG_FILE.chmod(0o600)


def get_api_key(args_api_key=None):
    """Get API key with priority: command line > env var > config file."""
    # 1. Command line argument
    if args_api_key:
        return args_api_key

    # 2. Environment variable
    env_key = os.environ.get(ENV_API_KEY)
    if env_key:
        return env_key

    # 3. Config file
    config = load_config()
    return config.get("api_key")


def log_info(msg):
    print(f"[INFO] {msg}")


def log_success(msg):
    print(f"[SUCCESS] {msg}")


def log_error(msg):
    print(f"[ERROR] {msg}")


def log_progress(status, progress):
    print(f"[PROGRESS] Status: {status}, Progress: {progress}%")


def format_timestamp(ts):
    """Convert Unix timestamp to readable datetime."""
    if not ts:
        return "N/A"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def parse_headers(header_list):
    """Parse header list from command line into dict."""
    if not header_list:
        return None
    headers = {}
    for h in header_list:
        if ":" in h:
            key, value = h.split(":", 1)
            headers[key.strip()] = value.strip()
    return headers if headers else None


def encode_file(file_path):
    """Encode file to base64."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(path, "rb") as f:
        content = f.read()

    # Determine MIME type
    suffix = path.suffix.lower()
    mime_types = {
        ".pdf": "application/pdf",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".txt": "text/plain",
        ".doc": "application/msword",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".ppt": "application/vnd.ms-powerpoint",
        ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    }
    mime_type = mime_types.get(suffix, "application/octet-stream")

    return {
        "file_name": path.name,
        "file_type": mime_type,
        "file_data": base64.b64encode(content).decode("utf-8")
    }


def create_task(api_key, operation, prompt, language=None, slide_count=None,
                template=None, ratio=None, doc_format=None, files=None, extra_headers=None, style=None):
    """Create an async generation task."""
    log_info("Creating task...")

    # Build auth token
    auth_token = api_key if api_key.startswith("Bearer ") else f"Bearer {api_key}"

    # Enhance prompt with style if provided
    final_prompt = prompt
    if style:
        final_prompt = f"{prompt}\n\nStyle requirement: {style}"
        log_info(f"Style applied: {style}")

    # Build request body
    body = {
        "auth_token": auth_token,
        "operation": operation,
        "prompt": final_prompt
    }

    if language:
        body["language"] = language

    # Slide-specific parameters
    if operation == "slide":
        if slide_count:
            body["slide_count"] = slide_count
        if template:
            body["template"] = template
        if ratio:
            body["ratio"] = ratio

    # Doc-specific parameters
    if operation == "doc":
        if doc_format:
            body["doc_format"] = doc_format

    # Process files
    if files:
        encoded_files = []
        for file_path in files:
            try:
                encoded_files.append(encode_file(file_path))
                log_info(f"Attachment added: {file_path}")
            except FileNotFoundError as e:
                log_error(str(e))
                return None
        if encoded_files:
            body["files"] = encoded_files

    # Build headers
    headers = {"Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)

    # Send request
    try:
        log_info(f"Request URL: {API_BASE}/v1/openapi/tasks")
        if extra_headers:
            log_info(f"Extra headers: {extra_headers}")
        response = requests.post(
            f"{API_BASE}/v1/openapi/tasks",
            json=body,
            headers=headers,
            timeout=30
        )
        log_info(f"Response status: {response.status_code}")
        log_info(f"Response body: {response.text[:500] if response.text else 'Empty'}")
        if response.status_code != 200:
            log_error(f"HTTP error: {response.status_code}")
            return None
        result = response.json()
    except requests.RequestException as e:
        log_error(f"Request failed: {e}")
        return None
    except json.JSONDecodeError:
        log_error(f"Response parse failed: {response.text[:500] if response.text else 'Empty'}")
        return None

    if result.get("success"):
        task_id = result.get("task_id")
        log_success("Task created successfully!")
        print(f"Task ID: {task_id}")
        return task_id
    else:
        log_error(f"Task creation failed: {result.get('error', 'Unknown error')}")
        return None


def query_task(api_key, task_id, extra_headers=None):
    """Query task status."""
    auth_token = api_key if api_key.startswith("Bearer ") else f"Bearer {api_key}"

    headers = {"Authorization": auth_token}
    if extra_headers:
        headers.update(extra_headers)

    try:
        response = requests.get(
            f"{API_BASE}/v1/openapi/tasks/{task_id}",
            headers=headers,
            timeout=30
        )
        return response.json()
    except requests.RequestException as e:
        log_error(f"Request failed: {e}")
        return None
    except json.JSONDecodeError:
        log_error(f"Response parse failed: {response.text}")
        return None


def poll_task(api_key, task_id, max_time=MAX_POLL_TIME, extra_headers=None, output_dir=None):
    """Poll task until completion or failure. Auto-downloads file if output_dir is provided."""
    log_info(f"Polling task status: {task_id}")

    start_time = time.time()
    last_progress = -1

    while True:
        elapsed = time.time() - start_time
        if elapsed > max_time:
            log_error(f"Polling timeout ({max_time}s)")
            return None

        task = query_task(api_key, task_id, extra_headers)
        if not task:
            time.sleep(POLL_INTERVAL)
            continue

        status = task.get("status")
        progress = task.get("progress", 0)

        # Only log progress if it changed
        if progress != last_progress:
            log_progress(status, progress)
            last_progress = progress

        if status == "completed":
            output = task.get("output", {})
            task_url = output.get("task_url", f"{API_BASE}/task/{task_id}")
            log_success("Task completed!")
            if output.get("slide_count"):
                print(f"Slide count: {output.get('slide_count')}")
            if output.get("word_count"):
                print(f"Word count: {output.get('word_count')}")

            # Auto-download file if output_dir is provided and file_url exists
            file_url = output.get("file_url")
            if output_dir and file_url:
                local_path = _download_to_local(file_url, output.get("file_name"), output_dir)
                if local_path:
                    print(f"[RESULT] Local file: {local_path}")
            elif file_url:
                # No output_dir, download to current directory
                local_path = _download_to_local(file_url, output.get("file_name"), ".")
                if local_path:
                    print(f"[RESULT] Local file: {local_path}")

            print(f"[RESULT] Task URL: {task_url}")
            return task

        elif status == "failed":
            log_error("Task failed!")
            print(f"Error: {task.get('error', 'Unknown error')}")
            return task

        time.sleep(POLL_INTERVAL)


def _download_to_local(file_url, file_name, output_dir):
    """Download file from URL to local directory. Returns local file path or None."""
    if not file_url:
        return None

    log_info("Downloading file...")

    try:
        response = requests.get(file_url, timeout=120)
        response.raise_for_status()
    except requests.RequestException as e:
        log_error(f"Download failed: {e}")
        return None

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    file_path = output_path / (file_name or "output")
    with open(file_path, "wb") as f:
        f.write(response.content)

    log_success(f"File saved: {file_path}")
    return str(file_path)


def download_file(api_key, task_id, output_dir, extra_headers=None):
    """Download the generated file. Returns local file path or False."""
    # First query task to get file URL
    task = query_task(api_key, task_id, extra_headers)
    if not task:
        return False

    if task.get("status") != "completed":
        log_error(f"Task not completed, current status: {task.get('status')}")
        return False

    output = task.get("output", {})
    file_url = output.get("file_url")
    file_name = output.get("file_name")
    task_url = output.get("task_url", f"{API_BASE}/task/{task_id}")

    if not file_url:
        log_error("Unable to get download URL")
        return False

    local_path = _download_to_local(file_url, file_name, output_dir)
    if local_path:
        print(f"[RESULT] Local file: {local_path}")
        print(f"[RESULT] Task URL: {task_url}")
        return local_path
    return False


def run_full_workflow(api_key, operation, prompt, output_dir, extra_headers=None, style=None, **kwargs):
    """Run the full workflow: create -> poll -> auto download."""
    # Create task
    task_id = create_task(api_key, operation, prompt, extra_headers=extra_headers, style=style, **kwargs)
    if not task_id:
        return False

    # Poll for completion (auto-downloads if output_dir is provided)
    task = poll_task(api_key, task_id, extra_headers=extra_headers, output_dir=output_dir or ".")
    if not task or task.get("status") != "completed":
        return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="AnyGen OpenAPI Client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create a slide task
  python3 anygen.py create -k sk-xxx -o slide -p "AI trends presentation"

  # Poll task status
  python3 anygen.py poll -k sk-xxx --task-id task_xxx

  # Download generated file
  python3 anygen.py download -k sk-xxx --task-id task_xxx --output ./

  # Run full workflow
  python3 anygen.py run -k sk-xxx -o slide -p "AI trends presentation" --output ./
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Common arguments
    def add_common_args(p):
        p.add_argument("--api-key", "-k", help="AnyGen API Key (sk-xxx). Can also use env ANYGEN_API_KEY or config file")
        p.add_argument("--header", "-H", action="append", dest="headers",
                       help="Extra HTTP header (format: 'Key:Value', can be used multiple times)")

    # Create command
    create_parser = subparsers.add_parser("create", help="Create a generation task")
    add_common_args(create_parser)
    create_parser.add_argument("--operation", "-o", required=True,
                               choices=["chat", "slide", "doc", "storybook", "data_analysis", "website"],
                               help="Operation type: chat, slide, doc, storybook, data_analysis, website")
    create_parser.add_argument("--prompt", "-p", required=True, help="Content prompt")
    create_parser.add_argument("--language", "-l", help="Language (zh-CN, en-US)")
    create_parser.add_argument("--slide-count", "-c", type=int, help="Number of slides")
    create_parser.add_argument("--template", "-t", help="Slide template")
    create_parser.add_argument("--ratio", "-r", choices=["16:9", "4:3"], help="Slide ratio")
    create_parser.add_argument("--doc-format", "-f", choices=["docx", "pdf"], help="Document format")
    create_parser.add_argument("--file", action="append", dest="files", help="Attachment file path (can be used multiple times)")
    create_parser.add_argument("--style", "-s", help="Style preference (e.g., 'business formal', 'minimalist modern', 'tech')")

    # Poll command
    poll_parser = subparsers.add_parser("poll", help="Poll task status until completion and auto-download")
    add_common_args(poll_parser)
    poll_parser.add_argument("--task-id", required=True, help="Task ID to poll")
    poll_parser.add_argument("--output", help="Output directory for auto-download (default: current directory)")

    # Download command
    download_parser = subparsers.add_parser("download", help="Download generated file")
    add_common_args(download_parser)
    download_parser.add_argument("--task-id", required=True, help="Task ID")
    download_parser.add_argument("--output", required=True, help="Output directory")

    # Run command (full workflow)
    run_parser = subparsers.add_parser("run", help="Run full workflow: create -> poll -> download")
    add_common_args(run_parser)
    run_parser.add_argument("--operation", "-o", required=True,
                           choices=["chat", "slide", "doc", "storybook", "data_analysis", "website"],
                           help="Operation type: chat, slide, doc, storybook, data_analysis, website")
    run_parser.add_argument("--prompt", "-p", required=True, help="Content prompt")
    run_parser.add_argument("--language", "-l", help="Language (zh-CN, en-US)")
    run_parser.add_argument("--slide-count", "-c", type=int, help="Number of slides")
    run_parser.add_argument("--template", "-t", help="Slide template")
    run_parser.add_argument("--ratio", "-r", choices=["16:9", "4:3"], help="Slide ratio")
    run_parser.add_argument("--doc-format", "-f", choices=["docx", "pdf"], help="Document format")
    run_parser.add_argument("--file", action="append", dest="files", help="Attachment file path")
    run_parser.add_argument("--style", "-s", help="Style preference (e.g., 'business formal', 'minimalist modern', 'tech')")
    run_parser.add_argument("--output", help="Output directory (optional)")

    # Config command
    config_parser = subparsers.add_parser("config", help="Manage configuration")
    config_subparsers = config_parser.add_subparsers(dest="config_action", help="Config actions")

    # config set
    config_set_parser = config_subparsers.add_parser("set", help="Set a config value")
    config_set_parser.add_argument("key", choices=["api_key", "default_language"], help="Config key")
    config_set_parser.add_argument("value", help="Config value")

    # config get
    config_get_parser = config_subparsers.add_parser("get", help="Get a config value")
    config_get_parser.add_argument("key", nargs="?", help="Config key (omit to show all)")

    # config delete
    config_delete_parser = config_subparsers.add_parser("delete", help="Delete a config value")
    config_delete_parser.add_argument("key", help="Config key to delete")

    # config path
    config_subparsers.add_parser("path", help="Show config file path")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Handle config command separately (doesn't need API key)
    if args.command == "config":
        if not args.config_action:
            config_parser.print_help()
            sys.exit(1)

        if args.config_action == "path":
            print(f"Config file: {CONFIG_FILE}")
            sys.exit(0)

        elif args.config_action == "set":
            config = load_config()
            config[args.key] = args.value
            save_config(config)
            # Mask API key in output
            display_value = args.value[:10] + "..." if args.key == "api_key" and len(args.value) > 10 else args.value
            log_success(f"Set {args.key} = {display_value}")
            sys.exit(0)

        elif args.config_action == "get":
            config = load_config()
            if args.key:
                value = config.get(args.key)
                if value:
                    # Mask API key
                    if args.key == "api_key" and len(value) > 10:
                        value = value[:10] + "..."
                    print(f"{args.key} = {value}")
                else:
                    print(f"{args.key} is not set")
            else:
                if config:
                    for k, v in config.items():
                        # Mask API key
                        if k == "api_key" and len(v) > 10:
                            v = v[:10] + "..."
                        print(f"{k} = {v}")
                else:
                    print("No config set")
            sys.exit(0)

        elif args.config_action == "delete":
            config = load_config()
            if args.key in config:
                del config[args.key]
                save_config(config)
                log_success(f"Deleted {args.key}")
            else:
                log_error(f"{args.key} not found in config")
            sys.exit(0)

    # For other commands, resolve API key
    api_key = get_api_key(getattr(args, 'api_key', None))
    if not api_key:
        log_error("API Key not found. Provide one via:")
        print("  1. Command line: --api-key sk-xxx")
        print(f"  2. Environment variable: export {ENV_API_KEY}=sk-xxx")
        print(f"  3. Config file: python3 anygen.py config set api_key sk-xxx")
        sys.exit(1)

    # Parse extra headers
    extra_headers = parse_headers(args.headers) if hasattr(args, 'headers') else None

    if args.command == "create":
        task_id = create_task(
            api_key=api_key,
            operation=args.operation,
            prompt=args.prompt,
            language=args.language,
            slide_count=args.slide_count,
            template=args.template,
            ratio=args.ratio,
            doc_format=args.doc_format,
            files=args.files,
            extra_headers=extra_headers,
            style=args.style
        )
        sys.exit(0 if task_id else 1)

    elif args.command == "poll":
        output_dir = getattr(args, 'output', None) or "."
        task = poll_task(api_key, args.task_id, extra_headers=extra_headers, output_dir=output_dir)
        if task and task.get("status") == "completed":
            sys.exit(0)
        else:
            sys.exit(1)

    elif args.command == "download":
        success = download_file(api_key, args.task_id, args.output, extra_headers=extra_headers)
        sys.exit(0 if success else 1)

    elif args.command == "run":
        success = run_full_workflow(
            api_key=api_key,
            operation=args.operation,
            prompt=args.prompt,
            output_dir=args.output,
            extra_headers=extra_headers,
            language=args.language,
            slide_count=args.slide_count,
            template=args.template,
            ratio=args.ratio,
            doc_format=args.doc_format,
            files=args.files,
            style=args.style
        )
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
