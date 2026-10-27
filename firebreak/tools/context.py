"""Per-run context handed to the tool gateway and to tool handlers."""

from dataclasses import dataclass

from firebreak.models.evidence import EvidenceSink


@dataclass(frozen=True)
class ToolContext:
    """What a tool call needs to know about the run it belongs to.

    ``incident_id`` scopes evidence: everything recorded or checked through
    ``evidence`` belongs to this incident and no other.
    """

    incident_id: str
    evidence: EvidenceSink
