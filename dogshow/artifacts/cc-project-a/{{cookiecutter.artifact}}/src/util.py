from pathlib import Path

def report_md(name):
    report_dir = Path("reports")
    report_dir.mkdir(exist_ok=True)
    return report_dir / f"{name}.md"