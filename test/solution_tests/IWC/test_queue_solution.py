from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue


def test_enqueue_size_dequeue_flow() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
            call_size().expect(1),
            call_dequeue().expect("companies_house", 1),
        ]
    )


def test_enqueue_rule_of_three() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=3)).expect(1),
            call_enqueue("bank_statements", 2, iso_ts(delta_minutes=3)).expect(2),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=3)).expect(3),
            call_enqueue("something_else", 1, iso_ts(delta_minutes=3)).expect(4),
            call_enqueue("id_verification", 2, iso_ts(delta_minutes=3)).expect(5),
        ]
    )


