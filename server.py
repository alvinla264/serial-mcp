#!/usr/bin/env python3
from fastmcp import FastMCP
import serial
import time
import re

mcp = FastMCP("EmbeddedSerial")

# Global state
class SerialState:
    def __init__(self):
        self.ser: serial.Serial | None = None
        self.default_timeout = 5.0
        # Common busybox prompts: sh#, / #, ~ #
        self.prompt_pattern = re.compile(rb'[\#\$]\s*$')

state = SerialState()

@mcp.tool()
def connect(port: str, baudrate: int = 115200, timeout: float = 1.0) -> str:
    """Connect to the serial device.
    
    Args:
        port: The serial port to connect to (e.g., /dev/ttyUSB0).
        baudrate: Communication speed (default 115200).
        timeout: Read timeout in seconds.
    """
    try:
        if state.ser and state.ser.is_open:
            state.ser.close()
            
        state.ser = serial.Serial(port, baudrate, timeout=timeout)
        return f"Successfully connected to {port} at {baudrate} baud."
    except Exception as e:
        return f"Error connecting to {port}: {str(e)}"

@mcp.tool()
def disconnect() -> str:
    """Disconnect from the current serial device."""
    if state.ser and state.ser.is_open:
        state.ser.close()
        state.ser = None
        return "Disconnected."
    return "No active connection."

def _read_until_prompt_or_timeout(timeout: float) -> str:
    if not state.ser:
        return ""
        
    output = b""
    start_time = time.time()
    
    while (time.time() - start_time) < timeout:
        if state.ser.in_waiting:
            chunk = state.ser.read(state.ser.in_waiting)
            output += chunk
            
            # Check if we hit the prompt
            if state.prompt_pattern.search(output):
                break
        else:
            time.sleep(0.05)
            
    return output.decode('utf-8', errors='replace')

@mcp.tool()
def send_command(command: str, timeout: float = 10.0, expect_prompt: bool = True) -> str:
    """Send a shell command to the device and return the output.
    
    Args:
        command: The shell command to execute.
        timeout: Max time to wait for a response.
        expect_prompt: If True, waits for a shell prompt (# or $) to determine completion.
                       If False, simply waits for the timeout or silence.
    """
    if not state.ser or not state.ser.is_open:
        return "Error: Not connected to any device."

    # Clear input buffer to remove any old noise
    state.ser.reset_input_buffer()
    
    # Send the command
    cmd_str = f"{command}\n"
    state.ser.write(cmd_str.encode('utf-8'))
    state.ser.flush()
    
    if expect_prompt:
        return _read_until_prompt_or_timeout(timeout)
    else:
        # Just read until timeout
        time.sleep(0.1) # Wait a bit for processing
        output = b""
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            if state.ser.in_waiting:
                output += state.ser.read(state.ser.in_waiting)
            else:
                # If we have data and line is idle for a bit, maybe return?
                # For now, strict timeout is safer for "no prompt" mode
                time.sleep(0.1)
        return output.decode('utf-8', errors='replace')

@mcp.tool()
def write_file_content(filepath: str, content: str) -> str:
    """Create or overwrite a file on the remote device.
    Uses 'cat' with a heredoc to write content.
    
    Args:
        filepath: Absolute path to the file on the remote device.
        content: Text content to write.
    """
    # Use a unique delimiter for heredoc
    delimiter = "EOF_MCP_TRANSFER"
    # Escape existing delimiters in content to avoid early termination (basic check)
    safe_content = content.replace(delimiter, f"{delimiter}_ESCAPED")
    
    cmd = f"cat > {filepath} << '{delimiter}'\n{safe_content}\n{delimiter}"
    return send_command(cmd)

@mcp.tool()
def read_file_content(filepath: str) -> str:
    """Read a file from the remote device.
    
    Args:
        filepath: Path to the file to read.
    """
    return send_command(f"cat {filepath}")

@mcp.tool()
def set_prompt_pattern(regex_pattern: str) -> str:
    """Update the regex pattern used to detect the shell prompt.
    Default is '[\\#\\$]\\s*$' (matches # or $ at end of line).
    """
    try:
        state.prompt_pattern = re.compile(regex_pattern.encode('utf-8'))
        return f"Prompt pattern updated to: {regex_pattern}"
    except re.error as e:
        return f"Invalid regex pattern: {e}"

if __name__ == "__main__":
    mcp.run()
