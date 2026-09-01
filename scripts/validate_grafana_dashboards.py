#!/usr/bin/env python3

import json
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = ROOT / "grafana" / "dashboards"
DATASOURCE_DIR = ROOT / "grafana" / "provisioning" / "datasources"

ALLOWED_API_VERSIONS = {
    "dashboard.grafana.app/v1",
    "dashboard.grafana.app/v2",
}

BUILTIN_DATASOURCE_UIDS = {
    "-- Grafana --",
    "-- Mixed --",
    "-- Dashboard --",
    "-100",
    "__expr__",
    "grafana",
}

UID_LINE_RE = re.compile(
    r"""^\s*uid:\s*(?:"([^"]+)"|'([^']+)'|([^#\s]+))\s*(?:#.*)?$"""
)


def load_provisioned_datasource_uids():
    uids = set()

    if not DATASOURCE_DIR.exists():
        return uids

    files = sorted(
        list(DATASOURCE_DIR.glob("*.yml"))
        + list(DATASOURCE_DIR.glob("*.yaml"))
    )

    for path in files:
        for line in path.read_text(encoding="utf-8").splitlines():
            match = UID_LINE_RE.match(line)
            if not match:
                continue

            uid = next(
                value
                for value in match.groups()
                if value is not None
            )

            uids.add(uid)

    return uids


def is_variable_reference(value):
    return (
        isinstance(value, str)
        and (
            value.startswith("$")
            or "${" in value
        )
    )


def collect_datasource_references(node, location="$"):
    refs = []

    if isinstance(node, dict):
        datasource = node.get("datasource")

        if isinstance(datasource, dict) and "uid" in datasource:
            refs.append(
                (
                    datasource.get("uid"),
                    f"{location}.datasource.uid",
                )
            )

        if "datasourceUid" in node:
            refs.append(
                (
                    node.get("datasourceUid"),
                    f"{location}.datasourceUid",
                )
            )

        for key, value in node.items():
            refs.extend(
                collect_datasource_references(
                    value,
                    f"{location}.{key}",
                )
            )

    elif isinstance(node, list):
        for index, value in enumerate(node):
            refs.extend(
                collect_datasource_references(
                    value,
                    f"{location}[{index}]",
                )
            )

    return refs


def validate_dashboard(path, provisioned_uids):
    errors = []
    warnings = []

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [
            f"{path}: invalid JSON: "
            f"line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ], []

    if not isinstance(data, dict):
        return [f"{path}: top-level JSON value must be an object"], []

    api_version = data.get("apiVersion")
    kind = data.get("kind")
    metadata = data.get("metadata")
    spec = data.get("spec")

    if api_version not in ALLOWED_API_VERSIONS:
        errors.append(
            f"{path}: unsupported apiVersion {api_version!r}; "
            f"allowed: {sorted(ALLOWED_API_VERSIONS)}"
        )

    if kind != "Dashboard":
        errors.append(
            f"{path}: kind must be 'Dashboard', got {kind!r}"
        )

    if not isinstance(metadata, dict):
        errors.append(
            f"{path}: metadata must be an object"
        )
        metadata = {}

    resource_uid = metadata.get("name")

    if not isinstance(resource_uid, str) or not resource_uid.strip():
        errors.append(
            f"{path}: metadata.name must contain the Grafana dashboard UID"
        )
        resource_uid = None

    elif any(char.isspace() for char in resource_uid):
        errors.append(
            f"{path}: metadata.name must not contain whitespace: "
            f"{resource_uid!r}"
        )

    if not isinstance(spec, dict):
        errors.append(
            f"{path}: spec must be an object"
        )
        return errors, warnings

    title = spec.get("title")

    if not isinstance(title, str) or not title.strip():
        errors.append(
            f"{path}: spec.title must be a non-empty string"
        )

    #
    # Grafana resource format v1 normally contains spec.uid.
    # Resource format v2 does not require it.
    #
    spec_uid = spec.get("uid")

    if spec_uid is not None:
        if not isinstance(spec_uid, str) or not spec_uid.strip():
            errors.append(
                f"{path}: spec.uid is present but is not a valid string"
            )
        elif resource_uid and spec_uid != resource_uid:
            errors.append(
                f"{path}: metadata.name ({resource_uid}) "
                f"does not match spec.uid ({spec_uid})"
            )

    if api_version == "dashboard.grafana.app/v1":
        schema_version = spec.get("schemaVersion")

        if not isinstance(schema_version, int) or schema_version <= 0:
            errors.append(
                f"{path}: v1 dashboard must contain "
                f"a positive integer spec.schemaVersion"
            )

    allowed_datasource_uids = (
        provisioned_uids | BUILTIN_DATASOURCE_UIDS
    )

    for datasource_uid, location in collect_datasource_references(spec):
        if datasource_uid is None:
            continue

        if not isinstance(datasource_uid, str):
            errors.append(
                f"{path}: datasource UID at {location} "
                f"must be a string, got {datasource_uid!r}"
            )
            continue

        if not datasource_uid:
            continue

        if is_variable_reference(datasource_uid):
            continue

        if datasource_uid not in allowed_datasource_uids:
            errors.append(
                f"{path}: unknown datasource UID "
                f"{datasource_uid!r} at {location}"
            )

    return errors, warnings


def main():
    print("=== VALIDATE GRAFANA DASHBOARDS =========================")
    print(f"Repository: {ROOT}")
    print(f"Dashboard directory: {DASHBOARD_DIR}")
    print()

    if not DASHBOARD_DIR.is_dir():
        print(
            f"ERROR: dashboard directory does not exist: "
            f"{DASHBOARD_DIR}"
        )
        return 1

    dashboard_files = sorted(
        DASHBOARD_DIR.rglob("*.json")
    )

    if not dashboard_files:
        print("ERROR: no dashboard JSON files found")
        return 1

    provisioned_uids = load_provisioned_datasource_uids()

    print("Provisioned datasource UIDs:")
    if provisioned_uids:
        for uid in sorted(provisioned_uids):
            print(f"  {uid}")
    else:
        print("  none")

    print()
    print(f"Dashboard files: {len(dashboard_files)}")
    print()

    all_errors = []
    all_warnings = []
    resource_uids = {}

    for path in dashboard_files:
        rel = path.relative_to(ROOT)

        errors, warnings = validate_dashboard(
            path,
            provisioned_uids,
        )

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            uid = data.get("metadata", {}).get("name")
            title = data.get("spec", {}).get("title")
            api_version = data.get("apiVersion")
        except Exception:
            uid = None
            title = None
            api_version = None

        if uid:
            if uid in resource_uids:
                all_errors.append(
                    f"{rel}: duplicate dashboard UID {uid!r}; "
                    f"already used by {resource_uids[uid]}"
                )
            else:
                resource_uids[uid] = rel

        status = "PASS" if not errors else "FAIL"

        print(
            f"{status}: {rel} "
            f"uid={uid!r} "
            f"title={title!r} "
            f"apiVersion={api_version!r}"
        )

        all_errors.extend(errors)
        all_warnings.extend(warnings)

    print()

    if all_warnings:
        print("=== WARNINGS ============================================")
        for warning in all_warnings:
            print(f"WARNING: {warning}")
        print()

    if all_errors:
        print("=== ERRORS ==============================================")
        for error in all_errors:
            print(f"ERROR: {error}")

        print()
        print(
            f"VALIDATION RESULT: FAILED "
            f"({len(all_errors)} error(s))"
        )
        return 1

    print("=== VALIDATION SUMMARY ==================================")
    print(f"Dashboards: {len(dashboard_files)}")
    print(f"Unique UIDs: {len(resource_uids)}")
    print(f"Datasource UIDs: {len(provisioned_uids)}")
    print("VALIDATION RESULT: PASSED")

    return 0


if __name__ == "__main__":
    sys.exit(main())
