import logging

from app.instruments.manager import InstrumentManager, SubscriptionDiff


def on_option_tokens_changed(diff: SubscriptionDiff) -> None:
    # Hook this into your websocket subscribe/unsubscribe workflow.
    print("Option token update:")
    print("  added:", diff.added)
    print("  removed:", diff.removed)
    print("  current:", len(diff.current))


def main() -> None:
    logging.basicConfig(level=logging.INFO)

    manager = InstrumentManager(
        env_name="UAT",
        use_env_creds=True,
        local_cache_csv="instrument_master_cache.csv",
        on_option_tokens_changed=on_option_tokens_changed,
    )

    snapshot = manager.get_subscription_tokens(nifty_price=22385.0)
    print("Initial token snapshot:")
    print(snapshot)

    # Dynamic ATM updates (trigger only if ATM shifts by >= 50 points).
    manager.update_atm(22390.0)  # likely no diff after initialization
    diff = manager.update_atm(22460.0)  # likely boundary change
    if diff:
        print("Resubscribe diff triggered:", diff)


if __name__ == "__main__":
    main()
