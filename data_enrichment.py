"""Run the three-thread near-real-time and periodic MESHJOIN pipeline.

Before starting:
    1. Run src/warehouse-schema.sql against PostgreSQL.
    2. Run data/static/feed_db.py to prepare files and source tables.
    3. Confirm .env contains the PostgreSQL connection and runtime settings.

Commands while running:
    online | periodic | both | peak | non-peak | status | quit
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import uuid
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from meshjoin.base import connect_database, load_runtime_config
from meshjoin.master_data import load_stores
from meshjoin.mesh_join import MeshJoinWorker
from meshjoin.stream_buffer import PriorityStreamBuffer
from meshjoin.streamer import OnlineThread, PeriodicThread, RuntimeState
from observatory import runtime_preflight


class DataEnrichmentApplication:
    def __init__(self, arrival_mode: str, online_file: Path | None = None) -> None:
        self.runtime = load_runtime_config()
        if online_file is not None:
            self.runtime = replace(self.runtime, online_file=online_file.resolve())
        self.state = RuntimeState(arrival_mode)
        self.buffer = PriorityStreamBuffer(self.runtime.stream_buffer_limit)
        self.online: OnlineThread | None = None
        self.periodic: PeriodicThread | None = None
        self.mesh_worker: MeshJoinWorker | None = None
        self.mesh_thread: threading.Thread | None = None
        self.telemetry_file: Path | None = None
        self._telemetry_write_error: str | None = None

    def start(self) -> None:
        if not self.runtime.online_file.is_file():
            raise FileNotFoundError(f"Online stream file not found: {self.runtime.online_file}")
        runtime_preflight(self.runtime.online_file)
        stores = load_stores(self.runtime.database)
        self.online = OnlineThread(
            self.runtime.online_file,
            self.buffer,
            self.state,
            self.runtime.peak_rate,
            self.runtime.non_peak_rate,
        )
        self.periodic = PeriodicThread(
            self.runtime.database,
            self.buffer,
            self.state,
            self.runtime.simulated_hour_seconds,
            self.runtime.periodic_group_count,
        )
        self.mesh_worker = MeshJoinWorker(self.runtime, self.buffer, self.state, stores)
        self.mesh_thread = threading.Thread(
            target=self.mesh_worker.run,
            name="MeshJoinThread",
            daemon=True,
        )

        print("Starting Data Warehouse MESHJOIN runtime")
        print(f"Rates: peak={self.runtime.peak_rate}/s, non-peak={self.runtime.non_peak_rate}/s")
        print(f"Simulated hour: {self.runtime.simulated_hour_seconds} seconds")
        print(f"Stream-buffer limit: {self.buffer.maxsize}")
        print(f"Online file: {self.runtime.online_file}")

        self.mesh_thread.start()
        self.online.start()
        self.periodic.start()

    def status(self) -> None:
        telemetry = self.telemetry()
        shed_disk = self._shed_count()
        print(
            "status | "
            f"mode={telemetry['source_mode']} | arrival={telemetry['arrival_mode']} "
            f"| speed={telemetry['rate_per_second']}/s "
            f"| stream_buffer={telemetry['stream_buffer']} "
            f"| H={telemetry['hash_transactions']} | shed_disk={shed_disk} "
            f"| online_fed={telemetry['online_fed']} "
            f"| periodic_fed={telemetry['periodic_fed']} "
            f"| periodic_shed={telemetry['periodic_shed']} "
            f"| warehouse_loaded={telemetry['warehouse_loaded']}"
        )
        self._report_errors()

    def command_loop(self) -> None:
        print("Commands: online, periodic, both, peak, non-peak, status, quit")
        while not self.state.stop_event.is_set():
            try:
                command = input("> ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                command = "quit"

            if command in {"online", "periodic", "both"}:
                self.state.set_source_mode(command)
                print(f"Source mode switched to {command}.")
            elif command in {"peak", "non-peak"}:
                self.state.set_arrival_mode(command)
                print(f"Arrival mode switched to {command}.")
            elif command == "status":
                self.status()
            elif command == "quit":
                self.stop()
            elif command:
                print("Unknown command. Use: online, periodic, both, peak, non-peak, status, quit")

    def watch_commands(self, control_file: Path) -> None:
        """Consume atomic JSON commands from the dashboard or CLI."""
        last_id = None
        while not self.state.stop_event.wait(0.3):
            try:
                payload = json.loads(control_file.read_text(encoding="utf-8"))
                if payload.get("id") == last_id:
                    continue
                last_id = payload.get("id")
                command = payload.get("command")
                if command in ("online", "periodic", "both"):
                    self.state.set_source_mode(command)
                elif command in ("peak", "non-peak"):
                    self.state.set_arrival_mode(command)
                elif command == "quit":
                    self.state.stop()
                elif command == "status":
                    self.status()
                else:
                    print(f"Ignored unrecognized control command: {command!r}")
                print(f"CONTROL | {command}", flush=True)
            except (FileNotFoundError, json.JSONDecodeError, OSError):
                continue

    def telemetry(self) -> dict[str, object]:
        """Return a serializable point-in-time view of the three workers."""

        source_mode, arrival_mode = self.state.snapshot()
        errors = {
            "OnlineThread": str(self.online.error)
            if self.online is not None and self.online.error is not None
            else None,
            "PeriodicThread": str(self.periodic.error)
            if self.periodic is not None and self.periodic.error is not None
            else None,
            "MeshJoinThread": str(self.mesh_worker.error)
            if self.mesh_worker is not None and self.mesh_worker.error is not None
            else None,
        }
        return {
            "updated_at": datetime.now(UTC).isoformat(),
            "source_mode": source_mode,
            "arrival_mode": arrival_mode,
            "rate_per_second": (
                self.runtime.peak_rate if arrival_mode == "peak" else self.runtime.non_peak_rate
            ),
            "stream_buffer": self.buffer.qsize(),
            "stream_buffer_limit": self.buffer.maxsize,
            "hash_transactions": (self.mesh_worker.hash_transactions if self.mesh_worker else 0),
            "online_fed": self.online.rows_loaded if self.online else 0,
            "periodic_fed": self.periodic.rows_loaded if self.periodic else 0,
            "periodic_shed": self.periodic.rows_shed if self.periodic else 0,
            "periodic_groups_released": (self.periodic.groups_released if self.periodic else 0),
            "warehouse_loaded": (self.mesh_worker.loaded_to_warehouse if self.mesh_worker else 0),
            "threads": {
                "OnlineThread": bool(self.online and self.online.is_alive()),
                "PeriodicThread": bool(self.periodic and self.periodic.is_alive()),
                "MeshJoinThread": bool(self.mesh_thread and self.mesh_thread.is_alive()),
            },
            "telemetry_write_error": self._telemetry_write_error,
            "errors": errors,
        }

    def _write_telemetry(self) -> None:
        if self.telemetry_file is None:
            return
        self.telemetry_file.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.telemetry(), indent=2)
        temporary = self.telemetry_file.with_name(
            f".{self.telemetry_file.name}.{uuid.uuid4().hex}.tmp"
        )
        last_error: OSError | None = None
        try:
            temporary.write_text(payload, encoding="utf-8")
            for attempt in range(6):
                try:
                    temporary.replace(self.telemetry_file)
                    self._telemetry_write_error = None
                    return
                except PermissionError as error:
                    last_error = error
                    if attempt < 5:
                        time.sleep(0.05 * (attempt + 1))

            # Windows readers can briefly hold the destination open.  A
            # direct write is a safe fallback for this small observability
            # snapshot and, importantly, must not terminate the worker.
            try:
                self.telemetry_file.write_text(payload, encoding="utf-8")
                self._telemetry_write_error = None
                return
            except OSError as error:
                last_error = error
        except OSError as error:
            last_error = error
        finally:
            temporary.unlink(missing_ok=True)

        self._telemetry_write_error = str(last_error) if last_error else "unknown telemetry write error"

    def run_without_console(self, telemetry_file: Path | None = None) -> None:
        """Keep threads alive for an external controller or test harness."""

        self.telemetry_file = telemetry_file.resolve() if telemetry_file else None
        while not self.state.stop_event.is_set():
            self._write_telemetry()
            self._report_errors()
            self.state.stop_event.wait(0.5)
        self._write_telemetry()

    def stop(self) -> None:
        self.state.stop()
        for thread in (self.online, self.periodic, self.mesh_thread):
            if thread is not None and thread.is_alive():
                thread.join(timeout=5)
        self._write_telemetry()
        self._report_errors()

    def _shed_count(self) -> int | str:
        try:
            connection = connect_database(self.runtime.database)
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT COUNT(*) FROM waiting_area.shed_table_physical "
                        "WHERE processed = FALSE"
                    )
                    return int(cursor.fetchone()[0])
            finally:
                connection.close()
        except Exception:
            return "unavailable"

    def _report_errors(self) -> None:
        errors = {
            "OnlineThread": self.online.error if self.online else None,
            "PeriodicThread": self.periodic.error if self.periodic else None,
            "MeshJoinThread": self.mesh_worker.error if self.mesh_worker else None,
        }
        for name, error in errors.items():
            if error is not None:
                print(f"{name} error: {error}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--arrival-mode",
        choices=("peak", "non-peak"),
        default="peak",
        help="Initial online-stream rate mode (default: peak).",
    )
    parser.add_argument("--online-file", type=Path, default=None)
    parser.add_argument("--control-file", type=Path, default=None)
    parser.add_argument("--telemetry-file", type=Path, default=None)
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="Run without the interactive command loop.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    application = DataEnrichmentApplication(args.arrival_mode, args.online_file)
    try:
        application.start()
        if args.control_file:
            threading.Thread(
                target=application.watch_commands, args=(args.control_file,), daemon=True
            ).start()
        if args.no_console:
            application.run_without_console(args.telemetry_file)
        else:
            application.command_loop()
    finally:
        application.stop()


if __name__ == "__main__":
    main()
