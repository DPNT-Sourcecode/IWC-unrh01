from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue, call_age


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
            call_size().expect(5),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("something_else", 1),
            call_dequeue().expect("id_verification", 2),
            call_dequeue().expect("bank_statements", 2),
        ]
    )


def test_enqueue_timestamp_ordering() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("bank_statements", 2, iso_ts(delta_minutes=-5)).expect(2),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=-2)).expect(3),
            call_size().expect(3),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("bank_statements", 2),
        ]
    )


def test_enqueue_dependency_resolution() -> None:
    run_queue(
        [
            call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(2),
            call_size().expect(2),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("credit_check", 1),
        ]
    )


def test_enqueue_deduplication() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=2)).expect(1),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=4)).expect(2),
            call_enqueue("credit_check", 1, iso_ts(delta_minutes=0)).expect(3),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("credit_check", 1),
            call_dequeue().expect("id_verification", 1),
        ]
    )


def test_enqueue_bank_statements_deprioritised() -> None:
    run_queue(
        [
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=2)).expect(1),
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(2),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=5)).expect(3),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("bank_statements", 1),
        ]
    )


def test_enqueue_bank_statements_deployed_1() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=0)).expect(2),
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(3),
            call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(4),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("bank_statements", 1),
            call_dequeue().expect("companies_house", 2),
        ]
    )


def test_enqueue_bank_statements_deployed_2() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=1)).expect(2),
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=2)).expect(3),
            call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(4),
            call_dequeue().expect("id_verification", 1),
            call_dequeue().expect("companies_house", 1),
            call_dequeue().expect("bank_statements", 1),
            call_dequeue().expect("companies_house", 2),
        ]
    )


def test_age_multiple() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_enqueue("id_verification", 1, iso_ts(delta_minutes=1)).expect(2),
            call_enqueue("companies_house", 1, iso_ts(delta_minutes=2)).expect(3),
            call_enqueue("companies_house", 2, iso_ts(delta_minutes=0)).expect(4),
            call_age().expect(120),
        ]
    )


def test_age_single() -> None:
    run_queue(
        [
            call_enqueue("bank_statements", 1, iso_ts(delta_minutes=0)).expect(1),
            call_age().expect(0),
        ]
    )


def test_age_none() -> None:
    run_queue(
        [
            call_age().expect(0),
        ]
    )
