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
import threading
import time

from meshjoin.base import connect_database, load_runtime_config
from meshjoin.master_data import load_stores
from meshjoin.mesh_join import MeshJoinWorker
from meshjoin.stream_buffer import PriorityStreamBuffer
from meshjoin.streamer import OnlineThread, PeriodicThread, RuntimeState


class DataEnrichmentApplication:
    def __init__(self, arrival_mode: str) -> None:
        self.runtime = load_runtime_config()
        self.state = RuntimeState(arrival_mode)
        self.buffer = PriorityStreamBuffer(self.runtime.stream_buffer_limit)
        self.online: OnlineThread | None = None
        self.periodic: PeriodicThread | None = None
        self.mesh_worker: MeshJoinWorker | None = None
        self.mesh_thread: threading.Thread | None = None

    def start(self) -> None:
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
        print(
            f"Rates: peak={self.runtime.peak_rate}/s, "
            f"non-peak={self.runtime.non_peak_rate}/s"
        )
        print(f"Simulated hour: {self.runtime.simulated_hour_seconds} seconds")
        print(f"Stream-buffer limit: {self.buffer.maxsize}")
        print(f"Online file: {self.runtime.online_file}")

        self.mesh_thread.start()
        self.online.start()
        self.periodic.start()

    def status(self) -> None:
        mode, arrival = self.state.snapshot()
        online = self.online.rows_loaded if self.online else 0
        periodic = self.periodic.rows_loaded if self.periodic else 0
        shed_by_periodic = self.periodic.rows_shed if self.periodic else 0
        hash_transactions = self.mesh_worker.hash_transactions if self.mesh_worker else 0
        warehouse_rows = self.mesh_worker.loaded_to_warehouse if self.mesh_worker else 0
        shed_disk = self._shed_count()
        print(
            "status | "
            f"mode={mode} | arrival={arrival} | stream_buffer={self.buffer.qsize()} "
            f"| H={hash_transactions} | shed_disk={shed_disk} "
            f"| online_fed={online} | periodic_fed={periodic} "
            f"| periodic_shed={shed_by_periodic} | warehouse_loaded={warehouse_rows}"
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

    def run_without_console(self) -> None:
        """Keep threads alive for an external controller or test harness."""

        while not self.state.stop_event.is_set():
            self._report_errors()
            time.sleep(0.5)

    def stop(self) -> None:
        self.state.stop()
        for thread in (self.online, self.periodic, self.mesh_thread):
            if thread is not None and thread.is_alive():
                thread.join(timeout=5)
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
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="Run without the interactive command loop.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    application = DataEnrichmentApplication(args.arrival_mode)
    try:
        application.start()
        if args.no_console:
            application.run_without_console()
        else:
            application.command_loop()
    finally:
        application.stop()


if __name__ == "__main__":
    main()

