"""SLA checks and alert routing.

Three checks cover the failure modes that actually page someone:

freshness  - the table stopped updating (upstream died, job silently skipped)
volume     - row count deviates from the trailing median beyond a threshold
             (catches a truncated source file, which freshness alone misses)
quality    - DQ pass rate fell below the contract

Alerts are emitted as structured events. In Azure they are picked up from the job
run output by a Databricks job-failure webhook / Azure Monitor action group; the
`webhook_url` path lets the same code post directly to Teams or PagerDuty.
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

from lakehouse.logging_utils import get_logger, run_id, safe_extra

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger(__name__)


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Alert:
    name: str
    severity: Severity
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    raised_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def as_dict(self) -> dict[str, Any]:
        return {
            "alert": self.name,
            "severity": self.severity.value,
            "message": self.message,
            "run_id": run_id(),
            "raised_at": self.raised_at.isoformat(),
            **self.context,
        }


@dataclass
class AlertManager:
    """Evaluate SLAs and dispatch alerts."""

    spark: SparkSession
    metrics_table: str = ""
    webhook_url: str = ""
    emails: list[str] = field(default_factory=list)
    raised: list[Alert] = field(default_factory=list)

    # ------------------------------------------------------------- dispatch
    def raise_alert(self, alert: Alert) -> None:
        self.raised.append(alert)
        payload = alert.as_dict()
        record_extra = safe_extra(payload)
        if alert.severity is Severity.CRITICAL:
            log.error("alert.raised", extra=record_extra)
        elif alert.severity is Severity.WARNING:
            log.warning("alert.raised", extra=record_extra)
        else:
            log.info("alert.raised", extra=record_extra)
        if self.webhook_url:
            self._post(payload)

    def _post(self, payload: dict[str, Any]) -> None:
        try:
            body = json.dumps(
                {
                    "text": f"[{payload['severity'].upper()}] {payload['alert']}: {payload['message']}",
                    "payload": payload,
                }
            ).encode()
            req = urllib.request.Request(
                self.webhook_url, data=body, headers={"Content-Type": "application/json"}
            )
            urllib.request.urlopen(req, timeout=10)  # noqa: S310 - fixed internal URL
        except Exception as exc:  # noqa: BLE001 - alerting must never fail the job
            log.warning("alert.webhook_failed", extra={"reason": str(exc)[:200]})

    # --------------------------------------------------------------- checks
    def check_freshness(
        self, table: str, timestamp_column: str, max_age_hours: int, dataset: str = ""
    ) -> Alert | None:
        """Alert when the newest record is older than the SLA."""
        from pyspark.sql import functions as F

        row = self.spark.table(table).agg(F.max(timestamp_column).alias("latest")).collect()[0]
        latest = row["latest"]
        if latest is None:
            alert = Alert(
                "table_empty",
                Severity.CRITICAL,
                f"{table} contains no rows",
                {"table": table, "dataset": dataset},
            )
            self.raise_alert(alert)
            return alert
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - latest
        if age > timedelta(hours=max_age_hours):
            alert = Alert(
                "stale_data",
                Severity.CRITICAL,
                f"{table} is {age.total_seconds() / 3600:.1f}h old, SLA is {max_age_hours}h",
                {
                    "table": table,
                    "dataset": dataset,
                    "age_hours": round(age.total_seconds() / 3600, 2),
                    "sla_hours": max_age_hours,
                    "latest": latest.isoformat(),
                },
            )
            self.raise_alert(alert)
            return alert
        log.info(
            "freshness.ok",
            extra={"table": table, "age_hours": round(age.total_seconds() / 3600, 2)},
        )
        return None

    def check_volume(
        self,
        dataset: str,
        layer: str,
        current_rows: int,
        lookback_runs: int = 10,
        deviation_pct: float = 50.0,
    ) -> Alert | None:
        """Alert when this run's volume deviates sharply from recent history."""
        if not self.metrics_table:
            return None
        try:
            history = [
                r["rows_written"]
                for r in self.spark.table(self.metrics_table)
                .where(f"dataset = '{dataset}' AND layer = '{layer}' AND status = 'success'")
                .orderBy("finished_at", ascending=False)
                .limit(lookback_runs)
                .collect()
            ]
        except Exception:  # pragma: no cover - first ever run
            return None
        history = [h for h in history if h]
        if len(history) < 3:
            return None
        median = sorted(history)[len(history) // 2]
        if median == 0:
            return None
        deviation = abs(current_rows - median) / median * 100
        if deviation > deviation_pct:
            alert = Alert(
                "volume_anomaly",
                Severity.WARNING,
                f"{dataset}/{layer} wrote {current_rows:,} rows vs median {median:,} "
                f"({deviation:.0f}% deviation)",
                {
                    "dataset": dataset,
                    "layer": layer,
                    "rows": current_rows,
                    "median_rows": median,
                    "deviation_pct": round(deviation, 1),
                },
            )
            self.raise_alert(alert)
            return alert
        return None

    def check_quality(
        self, dataset: str, layer: str, pass_rate: float, min_pass_rate: float = 0.99
    ) -> Alert | None:
        """Alert when the DQ pass rate breaches the contract."""
        if pass_rate >= min_pass_rate:
            return None
        severity = Severity.CRITICAL if pass_rate < min_pass_rate - 0.05 else Severity.WARNING
        alert = Alert(
            "quality_degraded",
            severity,
            f"{dataset}/{layer} DQ pass rate {pass_rate:.2%} below threshold {min_pass_rate:.2%}",
            {
                "dataset": dataset,
                "layer": layer,
                "pass_rate": round(pass_rate, 4),
                "threshold": min_pass_rate,
            },
        )
        self.raise_alert(alert)
        return alert

    def check_job_duration(
        self, job_name: str, duration_s: float, max_duration_s: float
    ) -> Alert | None:
        """Alert on runtime regressions - usually the first sign of a cost problem."""
        if duration_s <= max_duration_s:
            return None
        alert = Alert(
            "slow_job",
            Severity.WARNING,
            f"{job_name} took {duration_s:.0f}s, budget is {max_duration_s:.0f}s",
            {"job_name": job_name, "duration_s": duration_s, "budget_s": max_duration_s},
        )
        self.raise_alert(alert)
        return alert

    @property
    def has_critical(self) -> bool:
        return any(a.severity is Severity.CRITICAL for a in self.raised)

    def summary(self) -> dict[str, Any]:
        return {
            "total": len(self.raised),
            "critical": sum(1 for a in self.raised if a.severity is Severity.CRITICAL),
            "warning": sum(1 for a in self.raised if a.severity is Severity.WARNING),
            "alerts": [a.as_dict() for a in self.raised],
        }
