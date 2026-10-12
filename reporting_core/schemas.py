from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class PeriodMeta(BaseModel):
    start: str
    end: str
    label: str
    token: str


class ReportMeta(BaseModel):
    report_type: str
    title: str
    period: PeriodMeta
    campus: str | None = None
    generated_at: datetime


class Kpi(BaseModel):
    key: str
    label: str
    value: float | int | str | None = None
    delta: float | None = None
    delta_label: str | None = None
    format: Literal["number", "percent", "ratio", "text"] = "number"
    hint: str | None = None


class ReportColumn(BaseModel):
    key: str
    label: str
    format: str | None = None


class ReportTable(BaseModel):
    key: str
    title: str
    columns: list[ReportColumn]
    rows: list[dict[str, Any]] = Field(default_factory=list)


class ReportPoint(BaseModel):
    x: str | int | float | None
    y: float | int | None
    group: str | None = None


class ReportSeries(BaseModel):
    key: str
    title: str
    type: Literal["line", "bar"]
    points: list[ReportPoint] = Field(default_factory=list)


class ReportEnvelope(BaseModel):
    meta: ReportMeta
    kpis: list[Kpi] = Field(default_factory=list)
    tables: list[ReportTable] = Field(default_factory=list)
    series: list[ReportSeries] = Field(default_factory=list)

class CampusOption(BaseModel):
    campus_id: str
    label: str


class DashboardEnvelope(ReportEnvelope):
    campuses: list[CampusOption] = Field(default_factory=list)


class ReportNotImplementedDetail(BaseModel):
    code: Literal["not_implemented"]
    report_type: str
    message: str


class ReportNotImplementedResponse(BaseModel):
    detail: ReportNotImplementedDetail


class ReportErrorResponse(BaseModel):
    detail: Any


class ReportCatalogParam(BaseModel):
    name: str
    required: bool = False
    allowed_values: list[str] | None = None


class ReportCatalogItem(BaseModel):
    report_type: str
    title: str
    description: str
    params: list[ReportCatalogParam]
    supports_export: list[str]
    implemented: bool


class ReportRunRecord(BaseModel):
    id: str
    org: str
    report_type: str
    params: dict[str, Any]
    payload: dict[str, Any]
    generated_by: str
    created_at: datetime | None = None


class ReportScheduleCreate(BaseModel):
    report_type: str
    frequency: Literal["daily", "weekly", "monthly"]
    time: str
    format: Literal["pdf", "csv", "xlsx"]
    recipients: list[str]
    weekday: str | None = None
    day_of_month: int | None = None


class ReportScheduleUpdate(BaseModel):
    report_type: str | None = None
    frequency: Literal["daily", "weekly", "monthly"] | None = None
    time: str | None = None
    format: Literal["pdf", "csv", "xlsx"] | None = None
    recipients: list[str] | None = None
    weekday: str | None = None
    day_of_month: int | None = None
    active: bool | None = None


class ReportScheduleRecord(BaseModel):
    model_config = {"extra": "allow"}

    id: str | None = None
    Organization: str | None = None
    report_type: str
    frequency: str
    weekday: str | None = None
    day_of_month: int | None = None
    time_of_day: str | None = None
    format: str
    recipients: list[str]
    active: bool
    created_by: str | None = None