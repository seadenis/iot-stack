# Grafana dashboard workflow

Grafana dashboards are managed through Grafana Git Sync.

## Source of truth

Dashboard resources are stored in:

    grafana/dashboards/

The production Grafana instance synchronizes this directory from the `main`
branch.

## Editing workflow

1. Edit and save the dashboard in the Grafana UI.
2. Grafana creates a `dashboard/*` branch and Pull Request.
3. GitHub Actions validates the dashboard resource.
4. Merge the Pull Request into `main`.
5. Grafana Git Sync picks up the new revision automatically.

Do not manually deploy dashboard JSON files to the production host and do not
restore the legacy classic file provisioning under
`grafana/provisioning/dashboards/`.
