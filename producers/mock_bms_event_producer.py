#!/usr/bin/env python3
"""
Mock BookMyShow event producer for Amazon Kinesis Data Streams.

The producer sends two realistic event streams:
1. booking events, representing a user reserving movie tickets
2. payment events, representing the payment attempt for that booking

The data is intentionally varied so the Glue streaming job can validate:
- successful booking-payment joins
- failed payments that still join correctly
- bookings with no payment, which become unmatched joins
- payments with event timestamps outside the join window
"""

from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import boto3
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Movie:
    movie_id: str
    name: str
    genre: str
    language: str
    certificate: str


@dataclass(frozen=True)
class Venue:
    venue_id: str
    name: str
    city: str
    screens: tuple[str, ...]


MOVIES = (
    Movie("MOV1001", "Fighter", "Action", "Hindi", "UA"),
    Movie("MOV1002", "Kalki 2898 AD", "Sci-Fi", "Telugu", "UA"),
    Movie("MOV1003", "Laapataa Ladies", "Drama", "Hindi", "U"),
    Movie("MOV1004", "Manjummel Boys", "Thriller", "Malayalam", "UA"),
    Movie("MOV1005", "Maharaja", "Crime Thriller", "Tamil", "A"),
    Movie("MOV1006", "Inside Out 2", "Animation", "English", "U"),
    Movie("MOV1007", "Chandu Champion", "Biography", "Hindi", "UA"),
    Movie("MOV1008", "Aavesham", "Comedy", "Malayalam", "UA"),
)

VENUES = (
    Venue("VEN2001", "PVR Phoenix Marketcity", "Mumbai", ("Audi 1", "Audi 2", "IMAX")),
    Venue("VEN2002", "INOX Garuda Mall", "Bengaluru", ("Screen 1", "Screen 2", "Insignia")),
    Venue("VEN2003", "Cinepolis DLF Avenue", "Delhi NCR", ("Screen 1", "Screen 3", "VIP")),
    Venue("VEN2004", "PVR Nexus Vijaya Mall", "Chennai", ("Audi 1", "Audi 4")),
    Venue("VEN2005", "AMB Cinemas Gachibowli", "Hyderabad", ("Screen 2", "Screen 5", "Platinum")),
    Venue("VEN2006", "INOX Quest Mall", "Kolkata", ("Audi 2", "Audi 6")),
    Venue("VEN2007", "PVR Pavilion Mall", "Pune", ("Screen 1", "Screen 2", "Luxe")),
    Venue("VEN2008", "Rajhans Cinemas Himalaya Mall", "Ahmedabad", ("Audi 1", "Audi 3")),
)

TICKET_CATEGORIES = ("Classic", "Prime", "Recliner", "IMAX", "VIP")
CHANNELS = ("Android App", "iOS App", "Mobile Web", "Desktop Web")
DEVICE_TYPES = ("android", "ios", "mobile_web", "desktop")
PAYMENT_METHODS = ("UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet")
PAYMENT_PROVIDERS = ("Razorpay", "PayU", "Cashfree", "PhonePe PG", "BillDesk")
UPI_APPS = ("PhonePe", "Google Pay", "Paytm", "BHIM", "Amazon Pay")
BANKS = ("HDFC Bank", "ICICI Bank", "SBI", "Axis Bank", "Kotak Mahindra Bank")
FAILURE_REASONS = (
    "INSUFFICIENT_FUNDS",
    "BANK_TIMEOUT",
    "UPI_COLLECT_EXPIRED",
    "CARD_DECLINED",
    "RISK_CHECK_FAILED",
)


def now_utc() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


def isoformat(dt: datetime) -> str:
    """Format timestamps consistently for Spark timestamp parsing."""
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def money(value: Decimal) -> float:
    """Convert Decimal money values to JSON-friendly floats rounded to 2 decimals."""
    return float(value.quantize(Decimal("0.01")))


def choose_show_time(reference_time: datetime) -> datetime:
    """Pick a realistic show time between today and the next few days."""
    day_offset = random.choice((0, 0, 1, 1, 2, 3))
    hour = random.choice((9, 10, 12, 13, 15, 16, 18, 19, 21, 22, 23))
    minute = random.choice((0, 5, 10, 15, 20, 30, 45, 50))
    show_date = reference_time + timedelta(days=day_offset)
    return show_date.replace(hour=hour, minute=minute, second=0, microsecond=0)


def choose_seats(seat_count: int) -> list[str]:
    """Generate neighboring cinema seat labels such as H7, H8, H9."""
    row = random.choice(tuple("ABCDEFGHJKLMNP"))
    start_seat = random.randint(1, 18 - seat_count)
    return [f"{row}{seat_number}" for seat_number in range(start_seat, start_seat + seat_count)]


def build_booking_event() -> dict[str, Any]:
    """Build a realistic booking event with movie, venue, seat, and price details."""
    booking_time = now_utc()
    movie = random.choice(MOVIES)
    venue = random.choice(VENUES)
    seat_count = random.choices((1, 2, 3, 4, 5), weights=(18, 46, 20, 13, 3), k=1)[0]
    category = random.choices(TICKET_CATEGORIES, weights=(36, 34, 12, 10, 8), k=1)[0]

    base_price_by_category = {
        "Classic": Decimal(random.randrange(180, 280, 10)),
        "Prime": Decimal(random.randrange(260, 420, 10)),
        "Recliner": Decimal(random.randrange(550, 850, 25)),
        "IMAX": Decimal(random.randrange(450, 750, 25)),
        "VIP": Decimal(random.randrange(700, 1200, 50)),
    }

    ticket_price = base_price_by_category[category]
    gross_ticket_amount = ticket_price * seat_count
    convenience_fee = Decimal("28.00") + Decimal(seat_count * random.choice((8, 10, 12)))
    taxes = (gross_ticket_amount + convenience_fee) * Decimal("0.18")
    discount_amount = Decimal(random.choice((0, 0, 0, 25, 50, 75, 100)))
    total_amount = gross_ticket_amount + convenience_fee + taxes - discount_amount

    booking_id = f"BMS-{booking_time.strftime('%Y%m%d')}-{uuid.uuid4().hex[:10].upper()}"
    show_id = f"SHOW-{venue.venue_id}-{movie.movie_id}-{random.randint(1000, 9999)}"

    return {
        "event_type": "booking_created",
        "event_id": str(uuid.uuid4()),
        "booking_id": booking_id,
        "user_id": f"USR{random.randint(100000, 999999)}",
        "booking_ts": isoformat(booking_time),
        "show_id": show_id,
        "movie_id": movie.movie_id,
        "movie_name": movie.name,
        "genre": movie.genre,
        "language": movie.language,
        "certificate": movie.certificate,
        "city": venue.city,
        "venue_id": venue.venue_id,
        "venue_name": venue.name,
        "screen_name": random.choice(venue.screens),
        "show_ts": isoformat(choose_show_time(booking_time)),
        "seats": choose_seats(seat_count),
        "seat_count": seat_count,
        "ticket_category": category,
        "ticket_price": money(ticket_price),
        "convenience_fee": money(convenience_fee),
        "taxes": money(taxes),
        "discount_amount": money(discount_amount),
        "total_amount": money(total_amount),
        "currency": "INR",
        "channel": random.choice(CHANNELS),
        "device_type": random.choice(DEVICE_TYPES),
        "booking_status": "PENDING_PAYMENT",
    }


def build_payment_event(
    booking: dict[str, Any],
    failed_payment_rate: float,
    out_of_window_payment_rate: float,
) -> dict[str, Any]:
    """Build the payment event for a booking, sometimes failed or outside the join window."""
    booking_time = datetime.fromisoformat(booking["booking_ts"].replace("Z", "+00:00"))

    if random.random() < out_of_window_payment_rate:
        payment_time = booking_time + timedelta(minutes=random.randint(25, 60))
    else:
        payment_time = booking_time + timedelta(seconds=random.randint(5, 180))

    is_failed = random.random() < failed_payment_rate
    method = random.choice(PAYMENT_METHODS)

    payment_event = {
        "event_type": "payment_completed" if not is_failed else "payment_failed",
        "event_id": str(uuid.uuid4()),
        "booking_id": booking["booking_id"],
        "payment_id": f"PAY-{uuid.uuid4().hex[:12].upper()}",
        "payment_ts": isoformat(payment_time),
        "payment_method": method,
        "payment_provider": random.choice(PAYMENT_PROVIDERS),
        "payment_status": "SUCCESS" if not is_failed else "FAILED",
        "amount": booking["total_amount"],
        "currency": "INR",
        "bank_name": random.choice(BANKS) if method in {"Credit Card", "Debit Card", "Net Banking"} else None,
        "upi_app": random.choice(UPI_APPS) if method == "UPI" else None,
        "failure_reason": random.choice(FAILURE_REASONS) if is_failed else None,
    }
    return payment_event


def put_json_record(kinesis_client: Any, stream_name: str, partition_key: str, payload: dict[str, Any]) -> None:
    """Publish one JSON event to Kinesis."""
    kinesis_client.put_record(
        StreamName=stream_name,
        PartitionKey=partition_key,
        Data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish realistic BookMyShow mock events to Kinesis.")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"))
    parser.add_argument("--booking-stream", default=os.getenv("BOOKING_STREAM_NAME", "bms-realtime-booking-events"))
    parser.add_argument("--payment-stream", default=os.getenv("PAYMENT_STREAM_NAME", "bms-realtime-payment-events"))
    parser.add_argument("--event-count", type=int, default=0, help="0 means run forever.")
    parser.add_argument("--interval-seconds", type=float, default=float(os.getenv("EVENT_INTERVAL_SECONDS", "1.5")))
    parser.add_argument("--unmatched-payment-rate", type=float, default=float(os.getenv("UNMATCHED_PAYMENT_RATE", "0.08")))
    parser.add_argument("--out-of-window-payment-rate", type=float, default=float(os.getenv("OUT_OF_WINDOW_PAYMENT_RATE", "0.04")))
    parser.add_argument("--failed-payment-rate", type=float, default=float(os.getenv("FAILED_PAYMENT_RATE", "0.07")))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    kinesis = boto3.client("kinesis", region_name=args.region)

    produced_count = 0
    print(f"Publishing booking events to {args.booking_stream}")
    print(f"Publishing payment events to {args.payment_stream}")

    while args.event_count == 0 or produced_count < args.event_count:
        booking = build_booking_event()
        put_json_record(kinesis, args.booking_stream, booking["booking_id"], booking)

        # A small portion of bookings intentionally never receives a payment event.
        # The Glue job should emit those bookings to SQS after the watermark expires.
        has_payment_event = random.random() >= args.unmatched_payment_rate
        if has_payment_event:
            payment = build_payment_event(
                booking=booking,
                failed_payment_rate=args.failed_payment_rate,
                out_of_window_payment_rate=args.out_of_window_payment_rate,
            )
            put_json_record(kinesis, args.payment_stream, payment["booking_id"], payment)
            payment_label = payment["payment_status"]
        else:
            payment_label = "NO_PAYMENT_EVENT"

        produced_count += 1
        print(
            f"{produced_count:06d} | {booking['booking_id']} | "
            f"{booking['city']} | {booking['movie_name']} | INR {booking['total_amount']} | {payment_label}"
        )

        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
