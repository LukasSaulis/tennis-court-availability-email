from __future__ import annotations

import os
import re
import time
import smtplib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from typing import Dict, List, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


SCRAPE_CONCURRENCY = 1
SCRAPE_RETRIES = 2
REQUEST_TIMEOUT_SECONDS = 20
CHECK_INTERVAL_SECONDS = 15 * 60


@dataclass(frozen=True)
class VenueConfig:
    """Configuration for one venue."""
    id: str
    path: str
    max_courts: int
    court_prefix: str


@dataclass(frozen=True)
class Slot:
    """One parsed time/court slot from the booking page."""
    time: str
    court: str
    status: str


@dataclass(frozen=True)
class VenueWindow:
    """Target weekday and time window for a venue."""
    weekday: str
    start_time: str
    end_time: str


@dataclass(frozen=True)
class AvailabilityMatch:
    """One available court that matched the requested venue/date/time window."""
    venue_id: str
    venue_name: str
    date: str
    weekday: str
    time: str
    court: str
    booking_url: str


VENUES: Dict[str, VenueConfig] = {
    "st_johns_park": VenueConfig(
        id="st_johns_park",
        path="st-johns-park",
        max_courts=2,
        court_prefix="Court",
    ),
    "bethnal_green_gardens": VenueConfig(
        id="bethnal_green_gardens",
        path="bethnal-green-gardens",
        max_courts=4,
        court_prefix="Tennis court",
    ),
    "poplar_recreation_ground": VenueConfig(
        id="poplar_recreation_ground",
        path="poplar-rec-ground",
        max_courts=2,
        court_prefix="Court",
    ),
    "ropemakers_fields": VenueConfig(
        id="ropemakers_fields",
        path="ropemakers-field",
        max_courts=2,
        court_prefix="Court",
    ),
    "king_edward_memorial_park": VenueConfig(
        id="king_edward_memorial_park",
        path="king-edward-memorial-park",
        max_courts=2,
        court_prefix="Court",
    ),
    "wapping_gardens": VenueConfig(
        id="wapping_gardens",
        path="wapping-gardens",
        max_courts=1,
        court_prefix="Court",
    ),
    "victoria_park": VenueConfig(
        id="victoria_park",
        path="victoria-park",
        max_courts=4,
        court_prefix="Court",
    ),
}


class TimeUtils:
    """Helpers for weekday, date, and time handling."""

    WEEKDAY_MAP = {
        "monday": 0,
        "mon": 0,
        "tuesday": 1,
        "tue": 1,
        "tues": 1,
        "wednesday": 2,
        "wed": 2,
        "thursday": 3,
        "thu": 3,
        "thur": 3,
        "thurs": 3,
        "friday": 4,
        "fri": 4,
        "saturday": 5,
        "sat": 5,
        "sunday": 6,
        "sun": 6,
    }

    @staticmethod
    def normalize_weekday(weekday: str) -> int:
        """Convert a weekday string into a Python weekday integer."""
        key = weekday.strip().lower()
        if key not in TimeUtils.WEEKDAY_MAP:
            raise ValueError(f"Unsupported weekday: {weekday}")
        return TimeUtils.WEEKDAY_MAP[key]

    @staticmethod
    def next_date_for_weekday(weekday: str, from_date: Optional[date] = None) -> date:
        """Return the next date matching the requested weekday, including today."""
        base = from_date or date.today()
        target = TimeUtils.normalize_weekday(weekday)
        delta = (target - base.weekday()) % 7
        return base + timedelta(days=delta)

    @staticmethod
    def format_date_iso(d: date) -> str:
        """Return a date in YYYY-MM-DD format."""
        return d.isoformat()

    @staticmethod
    def format_weekday_label(d: date) -> str:
        """Return a readable weekday label."""
        return d.strftime("%A")

    @staticmethod
    def to_24_hour(input_value: str) -> str:
        """Convert a 12-hour time string like 9am or 9:30 pm to HH:MM."""
        s = input_value.strip().lower()
        match = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(am|pm)$", s)
        if not match:
            raise ValueError(f"Unrecognized time format: {input_value}")

        hour = int(match.group(1))
        minute = int(match.group(2) or "0")
        ampm = match.group(3)

        if ampm == "am":
            if hour == 12:
                hour = 0
        else:
            if hour != 12:
                hour += 12

        return f"{hour:02d}:{minute:02d}"

    @staticmethod
    def hhmm_in_range(value: str, start: str, end: str) -> bool:
        """Check if HH:MM lies within an inclusive time range."""
        return start <= value <= end


class TennisTowerHamletsClient:
    """HTTP client and HTML parser for Tennis Tower Hamlets pages."""

    def __init__(self, retries: int = SCRAPE_RETRIES, timeout: int = REQUEST_TIMEOUT_SECONDS):
        """Initialize the scraper client."""
        self.retries = retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

    def scrape_venue_for_date(self, venue: VenueConfig, date_iso: str) -> List[Slot]:
        """Fetch and parse all slots for a venue on a given date."""
        last_error: Optional[Exception] = None
        url = self.build_venue_url(venue, date_iso)

        for _ in range(self.retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                slots = self.parse_slots_from_html(response.text, venue.court_prefix)
                if not slots:
                    raise RuntimeError(f"No slot rows parsed for {venue.id} on {date_iso}")
                return slots
            except Exception as exc:
                last_error = exc

        raise RuntimeError(f"Failed to scrape {venue.id} for {date_iso}: {last_error}") from last_error

    def build_venue_url(self, venue: VenueConfig, date_iso: str) -> str:
        """Build the upstream page URL for a venue and date."""
        today_iso = TimeUtils.format_date_iso(date.today())
        if date_iso == today_iso:
            return f"https://tennistowerhamlets.com/book/courts/{venue.path}#book"
        return f"https://tennistowerhamlets.com/book/courts/{venue.path}/{date_iso}#book"

    def build_booking_url(self, venue: VenueConfig, date_iso: str) -> str:
        """Build the direct booking URL for a venue and date."""
        return f"https://tennistowerhamlets.com/book/courts/{venue.path}/{date_iso}#book"

    def parse_slots_from_html(self, html: str, court_prefix: str) -> List[Slot]:
        """Parse all slot rows from the venue HTML."""
        slots: List[Slot] = []
        escaped_prefix = re.escape(court_prefix)

        row_regex = re.compile(r"<tr>([\s\S]*?)</tr>", re.IGNORECASE)
        time_regex = re.compile(r'<th[^>]*class="time"[^>]*>\s*([^<]+)\s*</th>', re.IGNORECASE)
        court_regex = re.compile(
            rf'<span[^>]+class="button\s+(\w+)"[^>]*>({escaped_prefix}\s+\d+)[\s\S]*?<span[^>]*class="price"[^>]*>([\s\S]*?)</span>',
            re.IGNORECASE,
        )

        for row_match in row_regex.finditer(html):
            row = row_match.group(1)
            time_match = time_regex.search(row)
            if not time_match:
                continue

            try:
                time_24 = TimeUtils.to_24_hour(time_match.group(1).strip())
            except Exception:
                continue

            for court_match in court_regex.finditer(row):
                button_class = court_match.group(1).strip().lower()
                court = court_match.group(2).strip()

                if button_class == "available":
                    slots.append(Slot(time=time_24, court=court, status="available"))
                else:
                    slots.append(Slot(time=time_24, court=court, status="booked"))

        return sorted(slots, key=lambda x: (x.time, x.court))


class AvailabilityScanner:
    """Find matching available courts for configured venues and time windows."""

    def __init__(self, client: TennisTowerHamletsClient, venues: Dict[str, VenueConfig], concurrency: int = SCRAPE_CONCURRENCY):
        """Initialize the availability scanner."""
        self.client = client
        self.venues = venues
        self.concurrency = max(1, concurrency)

    def scan(self, target_windows: Dict[str, VenueWindow]) -> List[AvailabilityMatch]:
        """Scan all configured venues and return matching available courts."""
        tasks: List[Tuple[str, VenueConfig, VenueWindow, str, str]] = []

        for venue_name, window in target_windows.items():
            if venue_name not in self.venues:
                raise ValueError(f"Unknown venue in config: {venue_name}")

            venue = self.venues[venue_name]
            target_date = TimeUtils.next_date_for_weekday(window.weekday)
            date_iso = TimeUtils.format_date_iso(target_date)
            weekday_label = TimeUtils.format_weekday_label(target_date)
            tasks.append((venue_name, venue, window, date_iso, weekday_label))

        matches: List[AvailabilityMatch] = []

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            future_map = {
                executor.submit(self.client.scrape_venue_for_date, venue, date_iso): (venue_name, venue, window, date_iso, weekday_label)
                for venue_name, venue, window, date_iso, weekday_label in tasks
            }

            for future in as_completed(future_map):
                venue_name, venue, window, date_iso, weekday_label = future_map[future]
                try:
                    slots = future.result()
                except Exception as exc:
                    print(f"[ERROR] {venue_name} {date_iso}: {exc}")
                    continue

                for slot in slots:
                    if slot.status != "available":
                        continue
                    if not TimeUtils.hhmm_in_range(slot.time, window.start_time, window.end_time):
                        continue

                    matches.append(
                        AvailabilityMatch(
                            venue_id=venue.id,
                            venue_name=venue_name,
                            date=date_iso,
                            weekday=weekday_label,
                            time=slot.time,
                            court=slot.court,
                            booking_url=self.client.build_booking_url(venue, date_iso),
                        )
                    )

        return sorted(matches, key=lambda x: (x.date, x.time, x.venue_name, x.court))


class EmailNotifier:
    """Send availability emails via SMTP."""

    def __init__(self, smtp_host: str, smtp_port: int, username: str, password: str, sender_email: str, recipient_email: str):
        """Initialize SMTP email delivery."""
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.sender_email = sender_email
        self.recipient_email = recipient_email

    def send_matches(self, matches: List[AvailabilityMatch]) -> None:
        """Send one email containing all current availability matches."""
        if not matches:
            return

        subject = f"Tennis courts available: {len(matches)} match{'es' if len(matches) != 1 else ''}"
        body = self._build_body(matches)

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.sender_email
        msg["To"] = self.recipient_email
        msg.set_content(body)

        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.starttls()
            server.login(self.username, self.password)
            server.send_message(msg)

    def _build_body(self, matches: List[AvailabilityMatch]) -> str:
        """Build a readable email body for all matches."""
        lines = []
        lines.append(f"Generated at: {datetime.now().isoformat(timespec='seconds')}")
        lines.append("")
        lines.append("Available tennis courts found:")
        lines.append("")

        for match in matches:
            lines.append(f"Venue: {match.venue_name}")
            lines.append(f"Date: {match.date} ({match.weekday})")
            lines.append(f"Time: {match.time}")
            lines.append(f"Court: {match.court}")
            lines.append(f"Booking URL: {match.booking_url}")
            lines.append("")

        return "\n".join(lines).strip()


class AlertDeduplicator:
    """Track already-emailed matches during the current process lifetime."""

    def __init__(self):
        """Initialize the alert memory."""
        self.seen_keys: Set[str] = set()

    def get_new_matches(self, matches: List[AvailabilityMatch]) -> List[AvailabilityMatch]:
        """Return only matches that have not been emailed before."""
        fresh: List[AvailabilityMatch] = []

        for match in matches:
            key = self._make_key(match)
            if key in self.seen_keys:
                continue
            self.seen_keys.add(key)
            fresh.append(match)

        return fresh

    def _make_key(self, match: AvailabilityMatch) -> str:
        """Build a stable deduplication key."""
        return f"{match.venue_name}|{match.date}|{match.time}|{match.court}"


class CourtMonitorService:
    """Run the scanner on a loop and send emails for new matches."""

    def __init__(self, scanner: AvailabilityScanner, notifier: EmailNotifier, deduplicator: AlertDeduplicator, interval_seconds: int = CHECK_INTERVAL_SECONDS):
        """Initialize the monitoring service."""
        self.scanner = scanner
        self.notifier = notifier
        self.deduplicator = deduplicator
        self.interval_seconds = interval_seconds

    def run_forever(self, target_windows: Dict[str, VenueWindow]) -> None:
        """Run the monitor every fixed interval forever."""
        while True:
            self.run_once(target_windows)
            time.sleep(self.interval_seconds)

    def run_once(self, target_windows: Dict[str, VenueWindow]) -> None:
        """Run one scan cycle and send an email if new matches were found."""
        print(f"[INFO] Scan started at {datetime.now().isoformat(timespec='seconds')}")
        matches = self.scanner.scan(target_windows)
        new_matches = self.deduplicator.get_new_matches(matches)

        if new_matches:
            print(f"[INFO] New matches found: {len(new_matches)}")
            self.notifier.send_matches(new_matches)
        else:
            print("[INFO] No new matches found")


def load_email_notifier(recipient_email: str) -> EmailNotifier:
    """Create an email notifier from environment variables."""
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_username = os.environ["SMTP_USERNAME"]
    smtp_password = os.environ["SMTP_PASSWORD"]
    smtp_sender = os.environ.get("SMTP_SENDER_EMAIL", smtp_username)

    return EmailNotifier(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        username=smtp_username,
        password=smtp_password,
        sender_email=smtp_sender,
        recipient_email=recipient_email,
    )


def build_target_windows(raw_config: Dict[str, List[str]]) -> Dict[str, VenueWindow]:
    """Convert the raw __main__ config dictionary into typed venue windows."""
    result: Dict[str, VenueWindow] = {}

    for venue_name, values in raw_config.items():
        if not isinstance(values, list) or len(values) != 3:
            raise ValueError(
                f"Each config value must be [weekday, start_time, end_time]. Bad value for {venue_name}: {values}"
            )

        weekday, start_time, end_time = values
        if not re.match(r"^\d{2}:\d{2}$", start_time):
            raise ValueError(f"Invalid start time for {venue_name}: {start_time}")
        if not re.match(r"^\d{2}:\d{2}$", end_time):
            raise ValueError(f"Invalid end time for {venue_name}: {end_time}")
        if start_time > end_time:
            raise ValueError(f"start_time must be <= end_time for {venue_name}")

        result[venue_name] = VenueWindow(
            weekday=weekday,
            start_time=start_time,
            end_time=end_time,
        )

    return result


if __name__ == "__main__":
    venue_schedule = {
        "st_johns_park": ["Monday", "18:00", "21:00"],
        "bethnal_green_gardens": ["Sunday", "10:00", "13:00"],
        "victoria_park": ["Wednesday", "18:00", "21:00"],
    }

    target_windows = build_target_windows(venue_schedule)
    client = TennisTowerHamletsClient(retries=SCRAPE_RETRIES, timeout=REQUEST_TIMEOUT_SECONDS)
    scanner = AvailabilityScanner(client=client, venues=VENUES, concurrency=SCRAPE_CONCURRENCY)
    notifier = load_email_notifier(recipient_email="lukasd.saulis@gmail.com")
    deduplicator = AlertDeduplicator()
    service = CourtMonitorService(
        scanner=scanner,
        notifier=notifier,
        deduplicator=deduplicator,
        interval_seconds=CHECK_INTERVAL_SECONDS,
    )
    service.run_forever(target_windows)