"""Security utilities for WeRender."""

import gzip
import hashlib
import re
from pathlib import Path
from typing import Optional, Tuple


def new_file_hasher(expected_hash: str):
    """Select the declared digest; MD5 is for legacy transfer compatibility only."""
    if isinstance(expected_hash, str):
        if re.fullmatch(r"[0-9a-fA-F]{64}", expected_hash):
            return hashlib.sha256()
        if re.fullmatch(r"[0-9a-fA-F]{32}", expected_hash):
            return hashlib.md5(usedforsecurity=False)
    raise ValueError("Expected a SHA-256 or legacy MD5 hex digest")


def matches_file_hash(path: Path, expected_hash: str) -> bool:
    digest = new_file_hasher(expected_hash)
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest() == expected_hash.lower()


class BlendFileValidator:
    """Check file size and Blender header format, not scene safety."""

    # Magic bytes for .blend files
    BLEND_MAGIC = b'BLENDER'
    BLEND_MAGIC_64BIT = b'BLENDER'  # Same for 32 and 64 bit
    
    # Maximum file size (500 MB)
    MAX_FILE_SIZE = 500 * 1024 * 1024
    
    # File header structure
    HEADER_SIZE = 12  # Magic (7) + Pointer size (1) + Endianness (1) + Version (3)
    
    @staticmethod
    def validate_blend_file(file_path: Path) -> Tuple[bool, Optional[str]]:
        """
        Validate a .blend file for security risks.
        
        Args:
            file_path: Path to the .blend file
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not file_path.exists():
            return False, "File does not exist"
        
        # Check file size
        file_size = file_path.stat().st_size
        if file_size > BlendFileValidator.MAX_FILE_SIZE:
            return False, f"File too large ({file_size / 1024 / 1024:.1f} MB exceeds {BlendFileValidator.MAX_FILE_SIZE / 1024 / 1024} MB limit)"
        
        if file_size < BlendFileValidator.HEADER_SIZE:
            return False, "File too small to be a valid .blend file"
        
        # Check file extension
        if file_path.suffix.lower() != '.blend':
            return False, "File must have .blend extension"
        
        # Validate magic bytes
        try:
            with open(file_path, 'rb') as f:
                header = f.read(17)
                f.seek(0)
                if header.startswith(b"\x1f\x8b"):
                    with gzip.GzipFile(fileobj=f) as reader:
                        header = reader.read(17)
                elif header.startswith(b"\x28\xb5\x2f\xfd"):
                    try:
                        import zstandard
                    except ImportError:
                        return False, "Zstandard-compressed .blend validation requires the zstandard Python package; save uncompressed or install zstandard"
                    with zstandard.ZstdDecompressor(max_window_size=2**27).stream_reader(f) as reader:
                        header = reader.read(17)

                if not header.startswith(BlendFileValidator.BLEND_MAGIC):
                    return False, "Invalid .blend file: missing magic bytes"
                if header[7:9].isdigit():
                    if not re.fullmatch(rb"BLENDER17-01v[0-9]{4}", header):
                        return False, "Unsupported or invalid extended .blend file header"
                elif not re.fullmatch(rb"BLENDER[-_][vV][0-9]{3}", header[:12]):
                    return False, "Invalid pointer size, endianness, or version in .blend file header"
                
        except Exception as e:
            return False, f"Error reading .blend file header: {e}"
        
        # Use python-magic for deeper validation if available
        try:
            import magic
            mime = magic.Magic(mime=True)
            file_type = mime.from_file(str(file_path))
            
            # Blender files should be application/x-blender or application/octet-stream
            if file_type not in ['application/x-blender', 'application/octet-stream']:
                # Don't fail hard, but warn - some systems might not recognize .blend files
                print(f"⚠️  Warning: Unusual file type detected: {file_type}")
        except ImportError:
            # python-magic not available, skip this check
            pass
        except Exception as e:
            print(f"⚠️  Warning: Could not verify file type with libmagic: {e}")
        
        return True, None
    
    @staticmethod
    def calculate_file_hash(file_path: Path) -> str:
        """
        Calculate SHA256 hash of a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Hexadecimal hash string
        """
        hash_sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """
        Sanitize a filename to prevent path traversal attacks.
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename
        """
        # Remove path components
        filename = Path(filename).name
        
        # Remove any non-printable characters and limit length
        filename = ''.join(c for c in filename if c.isprintable() and c not in ['\0', '\n', '\r'])
        
        # Limit filename length
        if len(filename) > 255:
            filename = filename[:255]
        
        # Ensure it has .blend extension
        if not filename.lower().endswith('.blend'):
            filename += '.blend'
        
        return filename


class RateLimiter:
    """Simple in-memory rate limiter."""
    
    def __init__(self, max_requests: int = 100, window_seconds: int = 60):
        """
        Initialize rate limiter.
        
        Args:
            max_requests: Maximum requests per window
            window_seconds: Time window in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests = {}
    
    def is_allowed(self, identifier: str) -> bool:
        """
        Check if request is allowed.
        
        Args:
            identifier: Unique identifier (e.g., IP address)
            
        Returns:
            True if allowed, False otherwise
        """
        import time
        
        current_time = time.time()
        
        if identifier not in self.requests:
            self.requests[identifier] = []
        
        # Remove old requests outside the window
        self.requests[identifier] = [
            timestamp for timestamp in self.requests[identifier]
            if current_time - timestamp < self.window_seconds
        ]
        
        # Check if limit exceeded
        if len(self.requests[identifier]) >= self.max_requests:
            return False
        
        # Add current request
        self.requests[identifier].append(current_time)
        return True
    
    def reset(self, identifier: str) -> None:
        """Reset rate limit for identifier."""
        if identifier in self.requests:
            del self.requests[identifier]
