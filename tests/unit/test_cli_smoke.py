from chatstore.cli.app import build_parser


def test_parser_builds():
    assert build_parser().parse_args(["status"]).command == "status"


def test_version_command(capsys):
    import json

    from chatstore import __version__
    from chatstore.cli.app import main

    assert main(["version"]) == 0
    assert capsys.readouterr().out.startswith(f"chatstore {__version__}")
    assert main(["--json", "version"]) == 0
    env = json.loads(capsys.readouterr().out)
    assert env["ok"] and env["command"] == "version" and env["data"]["version"] == __version__
