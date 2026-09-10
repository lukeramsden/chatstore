from chatstore.cli.app import build_parser


def test_parser_builds():
    assert build_parser().parse_args(["status"]).command == "status"
