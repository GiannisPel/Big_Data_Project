from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Personalization:
    student_id: int
    start_hour: int
    window_length: int
    hours: list[int]
    start_day_2024: int
    days_2024: list[int]
    top_k: int


def build_personalization(student_id: int) -> Personalization:
    start_hour = student_id % 24
    window_length = 4
    start_day_2024 = (student_id % 26) + 1
    top_k = (student_id % 11) + 10

    hours = [(start_hour + offset) % 24 for offset in range(window_length)]
    days_2024 = [start_day_2024, start_day_2024 + 1, start_day_2024 + 2]

    return Personalization(
        student_id=student_id,
        start_hour=start_hour,
        window_length=window_length,
        hours=hours,
        start_day_2024=start_day_2024,
        days_2024=days_2024,
        top_k=top_k,
    )


PERSONALIZATION = build_personalization(2121261)


if __name__ == "__main__":
    p = PERSONALIZATION
    print(f"A = {p.student_id}")
    print(f"h = {p.start_hour}")
    print(f"L = {p.window_length}")
    print(f"hours = {p.hours}")
    print(f"d = {p.start_day_2024}")
    print(f"days_2024 = {p.days_2024}")
    print(f"K = {p.top_k}")
