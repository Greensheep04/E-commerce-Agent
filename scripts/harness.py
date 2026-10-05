import argparse
import json

from shop.campaign import CampaignService
from shop.config import Settings


def main() -> None:
    parser = argparse.ArgumentParser(description="素材推荐任务")
    subparsers = parser.add_subparsers(dest="command", required=True)
    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("prompt")
    for command in ("run", "show", "cancel", "resume"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("task_id")
        if command == "run":
            subparser.add_argument("--hold-at-checkpoint", action="store_true")
            subparser.add_argument("--hold-after-reserve", action="store_true")
    args = parser.parse_args()
    service = CampaignService(Settings())
    if args.command == "create":
        result = service.create(args.prompt)
    elif args.command == "run":
        result = service.run(
            args.task_id, hold_at_checkpoint=args.hold_at_checkpoint,
            hold_after_reserve=args.hold_after_reserve,
        )
    else:
        result = getattr(service, args.command)(args.task_id)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
