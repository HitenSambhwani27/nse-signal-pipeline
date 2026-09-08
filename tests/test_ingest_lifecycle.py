"""Static validation of the single nse-ingest service + weekday start timer."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVICE = (ROOT / "deploy" / "vm" / "nse-ingest.service").read_text(encoding="utf-8")
TIMER = (ROOT / "deploy" / "vm" / "nse-ingest.timer").read_text(encoding="utf-8")
INSTALL = (ROOT / "deploy" / "vm" / "install_ingest_unit.sh").read_text(encoding="utf-8")
CUTOVER = (ROOT / "deploy" / "vm" / "cutover_ingest_to_systemd.sh").read_text(encoding="utf-8")
DEPLOY = (ROOT / "deploy" / "deploy.sh").read_text(encoding="utf-8")


def test_single_ingest_execstart_and_on_failure_restart() -> None:
    assert SERVICE.count("ExecStart=") == 1
    assert "01_run_ingestion.py" in SERVICE
    assert "Restart=on-failure" in SERVICE
    assert "Restart=always" not in SERVICE
    assert "SyslogIdentifier=nse-ingest" in SERVICE
    assert "nse-ingest.timer" in SERVICE


def test_timer_starts_service_before_session_without_catchup() -> None:
    assert "OnCalendar=Mon..Fri 08:45:00" in TIMER
    assert "Persistent=false" in TIMER
    assert "Unit=nse-ingest.service" in TIMER
    assert "WantedBy=timers.target" in TIMER
    assert "holiday" in TIMER.lower()


def test_install_and_deploy_wire_timer_without_starting_ingest() -> None:
    assert "nse-ingest.timer" in INSTALL
    assert "nse-ingest.timer" in CUTOVER
    assert "nse-ingest.timer" in DEPLOY
    assert "systemctl start nse-ingest.timer" in INSTALL
    assert "sudo systemctl start nse-ingest" in CUTOVER
    deploy_start_ingest = [
        line
        for line in DEPLOY.splitlines()
        if "systemctl start nse-ingest" in line and "timer" not in line
    ]
    assert deploy_start_ingest == []
    assert "systemctl restart nse-ingest" not in DEPLOY


def test_no_second_ingest_unit_file() -> None:
    units = sorted(p.name for p in (ROOT / "deploy" / "vm").glob("nse-ingest*.service"))
    assert units == ["nse-ingest.service"]
    example = ROOT / "deploy" / "vm" / "nse-ingest.service.example"
    assert example.is_file()
    assert "EXAMPLE" in example.read_text(encoding="utf-8")
    assert (ROOT / "deploy" / "vm" / "nse-ingest.timer").is_file()
    timers = sorted(p.name for p in (ROOT / "deploy" / "vm").glob("nse-ingest*.timer"))
    assert timers == ["nse-ingest.timer"]
