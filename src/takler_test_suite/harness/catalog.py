import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

TAXONOMY = {
    "definition": {"structure", "parameters", "templates", "serialization"},
    "scheduling": {"dependencies", "resources", "time_repeat"},
    "execution": {"deployment", "submission", "instances"},
    "operations": {"controls", "queries", "presentation"},
    "service": {"lifecycle", "transports", "client_config"},
    "security": {"operators", "children", "protection"},
    "recovery": {"saving", "restoring", "crash_windows"},
    "capacity": {"large_trees", "mixed_load", "endurance"},
}
ROLES = {"developer", "operator", "administrator"}
PROFILES = {"offline", "local"}
STATUSES = {"planned", "executable", "maintenance", "disabled"}
FIELDS = {
    "id",
    "title",
    "area",
    "category",
    "kind",
    "difficulty",
    "roles",
    "capabilities",
    "profiles",
    "status",
}


class CatalogError(ValueError):
    pass


@dataclass(frozen=True)
class Case:
    id: str
    title: str
    area: str
    category: str
    kind: str
    difficulty: str
    roles: tuple[str, ...]
    capabilities: tuple[str, ...]
    profiles: tuple[str, ...]
    status: str
    entry: Path | None


def load_catalog(path: Path) -> dict[str, Case]:
    path = path.resolve()
    try:
        with path.open("rb") as stream:
            catalog = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise CatalogError(f"Cannot read catalog: {exc}") from exc
    if (
        set(catalog) != {"schema_version", "cases"}
        or type(catalog["schema_version"]) is not int
        or catalog["schema_version"] != 1
    ):
        raise CatalogError("Expected catalog schema_version=1 and cases")
    if not isinstance(catalog["cases"], list) or not catalog["cases"]:
        raise CatalogError("Expected at least one [[cases]] entry")

    cases = {}
    entries = set()
    for index, row in enumerate(catalog["cases"], 1):
        if not isinstance(row, dict) or not FIELDS <= row.keys() <= FIELDS | {"entry"}:
            raise CatalogError(f"cases[{index}]: missing or unknown fields")
        case_id = row["id"]
        if (
            not isinstance(case_id, str)
            or re.fullmatch(r"[A-Z]{3}-[0-9]{3}", case_id) is None
        ):
            raise CatalogError(f"cases[{index}]: invalid case ID")
        if case_id in cases:
            raise CatalogError(f"{case_id}: duplicate ID")

        def choice(field, choices):
            value = row[field]
            if not isinstance(value, str) or value not in choices:
                raise CatalogError(f"{case_id}: invalid {field}")
            return value

        def tags(field, choices=None):
            values = row[field]
            if (
                not isinstance(values, list)
                or not values
                or any(
                    not isinstance(value, str)
                    or re.fullmatch(r"[a-z][a-z0-9_]*", value) is None
                    for value in values
                )
            ):
                raise CatalogError(f"{case_id}: invalid {field}")
            if len(set(values)) != len(values) or (
                choices is not None and set(values) - choices
            ):
                raise CatalogError(f"{case_id}: invalid or duplicate {field}")
            return tuple(values)

        if not isinstance(row["title"], str) or not row["title"].strip():
            raise CatalogError(f"{case_id}: invalid title")
        area = choice("area", TAXONOMY)
        category = choice("category", TAXONOMY[area])
        kind = choice("kind", {"offline", "service", "job", "integration"})
        difficulty = choice("difficulty", {"L1", "L2", "L3", "L4"})
        status = choice("status", STATUSES)
        roles = tags("roles", ROLES)
        capabilities = tags("capabilities")
        profiles = tags("profiles", PROFILES)
        expected = "offline" if kind == "offline" else "local"
        if profiles != (expected,):
            raise CatalogError(f"{case_id}: {kind} requires profile {expected}")

        entry = None
        if "entry" in row:
            prefix = case_id.lower().replace("-", "_")
            pattern = rf"cases/{area}/{category}/{prefix}_[a-z0-9_]+/case\.py"
            value = row["entry"]
            if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
                raise CatalogError(f"{case_id}: invalid entry path")
            entry = path.parent / value
            if entry.resolve() != entry or not entry.is_file() or entry in entries:
                raise CatalogError(f"{case_id}: missing, redirected or duplicate entry")
            entries.add(entry)
        if status == "executable" and entry is None:
            raise CatalogError(f"{case_id}: executable case requires an entry")
        if status == "planned" and entry is not None:
            raise CatalogError(f"{case_id}: planned case must have no entry")

        cases[case_id] = Case(
            case_id,
            row["title"],
            area,
            category,
            kind,
            difficulty,
            roles,
            capabilities,
            profiles,
            status,
            entry,
        )
    return cases
