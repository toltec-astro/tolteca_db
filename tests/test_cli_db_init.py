"""Regression tests for idempotent database initialization."""

from sqlalchemy.orm import Session
from typer.testing import CliRunner

from tolteca_db.cli import app
from tolteca_db.db import get_engine
from tolteca_db.models.orm import Location

runner = CliRunner()


def test_init_adds_location_to_existing_schema(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'tolteca.sqlite'}"
    first = runner.invoke(app, ["db", "init", "--url", db_url, "--no-registry"])
    assert first.exit_code == 0, first.output

    data_root = tmp_path / "data_lmt"
    data_root.mkdir()
    second = runner.invoke(
        app,
        ["db", "init", "--url", db_url, "--data-root", str(data_root)],
    )
    assert second.exit_code == 0, second.output

    with Session(get_engine(db_url)) as session:
        location = session.query(Location).filter_by(label="LMT").one()
        assert location.root_uri == data_root.as_uri()


def test_init_reconciles_existing_location_path(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'tolteca.sqlite'}"
    old_root = tmp_path / "old"
    new_root = tmp_path / "new"
    old_root.mkdir()
    new_root.mkdir()

    first = runner.invoke(
        app,
        ["db", "init", "--url", db_url, "--data-root", str(old_root)],
    )
    assert first.exit_code == 0, first.output
    second = runner.invoke(
        app,
        ["db", "init", "--url", db_url, "--data-root", str(new_root)],
    )
    assert second.exit_code == 0, second.output

    with Session(get_engine(db_url)) as session:
        location = session.query(Location).filter_by(label="LMT").one()
        assert location.root_uri == new_root.as_uri()
