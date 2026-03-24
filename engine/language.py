from __future__ import annotations
import re

# High-signal non-English patterns common in job titles
GERMAN = re.compile(r"\b(m/w/d|d/w/m)\b|\b(buchhalter|kenntnisse|bewerbung|steuer|unternehmen)\b", re.I)
SPANISH = re.compile(r"\b(ingenier[oa]|contabilidad|administraci[oó]n|oferta|requisitos)\b", re.I)
PORT = re.compile(r"\b(vaga|requisitos|contabilidade|administra[cç][aã]o)\b", re.I)

def detect_language_gate(title: str|None, text: str|None) -> str:
    """
    returns "ok" or "block"
    block => not English/French (heuristic)
    """
    s = (title or "") + "\n" + (text or "")
    if GERMAN.search(s) or SPANISH.search(s) or PORT.search(s):
        return "block"
    return "ok"
