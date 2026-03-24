from __future__ import annotations
from typing import Optional
import hashlib

def make_fingerprint(company: str, title: str, apply_url: Optional[str], url: Optional[str]) -> str:
    key = f"{(company or '').lower().strip()}|{(title or '').lower().strip()}|{(apply_url or url or '').strip()}|"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()
