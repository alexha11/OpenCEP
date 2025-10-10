#!/usr/bin/env python3
import sys
import csv
import argparse
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime

START_KEYS = ['start station id', 'start_station_id', 'start_station_id ']
END_KEYS = ['end station id', 'end_station_id', 'end_station_id ']
BIKE_KEYS = ['bikeid', 'bike_id']
START_TIME_KEYS = ['starttime', 'started_at']
STOP_TIME_KEYS = ['stoptime', 'ended_at']

SELECTIVITY_MODES = ['balanced', 'rare', 'common']


def detect_col(headers, candidates):
    hmap = {h.strip().lower(): i for i, h in enumerate(headers)}
    for c in candidates:
        if c in hmap:
            return c, hmap[c]
    for name, idx in hmap.items():
        if any(k in name for k in candidates):
            return name, idx
    return None, None


def parse_dt(v: str):
    if not v:
        return None
    v = v.strip().replace('"', '')
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue
    try:
        # Fallback: fromisoformat sans Z
        return datetime.fromisoformat(v.rstrip('Z'))
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(
        description='Find target station IDs for CitiBike CSV',
        epilog='Example: python3 find_targets.py data.csv --max-events 20 --mode common --top 3'
    )
    ap.add_argument('file', help='Path to CitiBike CSV')
    ap.add_argument('--top', type=int, default=3, help='Number of targets (default: 3)')
    ap.add_argument('--max-events', type=int, default=0,
                    help='Only analyze first N events (0=all, default: 0)')
    ap.add_argument('--mode', choices=SELECTIVITY_MODES, default='balanced',
                    help='Selection mode: balanced (default), rare (selective), common (high volume)')
    ap.add_argument('--verbose', action='store_true', help='Print diagnostics')
    args = ap.parse_args()

    p = Path(args.file)
    if not p.exists():
        print(f'Error: file not found: {p}', file=sys.stderr)
        sys.exit(1)

    with p.open('r', newline='') as f:
        rdr = csv.reader(f)
        try:
            headers = next(rdr)
        except StopIteration:
            print('Error: empty CSV', file=sys.stderr)
            sys.exit(1)

        if args.verbose:
            if args.max_events > 0:
                print(f'Analyzing first {args.max_events} events only', file=sys.stderr)
            else:
                print(f'Analyzing entire file', file=sys.stderr)

        start_key, start_idx = detect_col(headers, START_KEYS)
        end_key, end_idx = detect_col(headers, END_KEYS)
        bike_key, bike_idx = detect_col(headers, BIKE_KEYS)
        st_key, st_idx = detect_col(headers, START_TIME_KEYS)
        et_key, et_idx = detect_col(headers, STOP_TIME_KEYS)

        if start_idx is None or end_idx is None:
            print('Error: could not detect start/end station id columns', file=sys.stderr)
            if args.verbose:
                print('Headers:', headers, file=sys.stderr)
            sys.exit(2)

        start_counts = Counter()
        end_counts = Counter()
        # Chainable terminal counts (based on adjacent, same-bike, station continuity, within 1h)
        chain_terminal_counts = Counter()
        last_trip_by_bike = {}

        events_read = 0
        for row in rdr:
            if len(row) != len(headers):
                continue
            if args.max_events > 0 and events_read >= args.max_events:
                break
            events_read += 1

            try:
                sv = row[start_idx].strip()
                ev = row[end_idx].strip()
            except Exception:
                continue

            if sv:
                start_counts[sv] += 1
            if ev:
                end_counts[ev] += 1

            # Chain detection requires bike/time data
            if bike_idx is not None and st_idx is not None and et_idx is not None:
                try:
                    bike = row[bike_idx].strip()
                except Exception:
                    bike = None
                st = parse_dt(row[st_idx]) if st_idx is not None else None
                et = parse_dt(row[et_idx]) if et_idx is not None else None

                # Form current trip record
                trip = {
                    'bike': bike,
                    'start': sv,
                    'end': ev,
                    'starttime': st,
                    'stoptime': et,
                }
                if bike:
                    prev = last_trip_by_bike.get(bike)
                    if prev and prev.get('end') and trip['start'] and prev['end'] == trip['start']:
                        # Within 1 hour if times present
                        within = True
                        if prev.get('stoptime') and trip.get('starttime'):
                            delta = (trip['starttime'] - prev['stoptime']).total_seconds()
                            within = (delta >= 0 and delta <= 3600)
                        if within and trip['end']:
                            chain_terminal_counts[trip['end']] += 1
                    last_trip_by_bike[bike] = trip

        if args.verbose:
            print(f'Read {events_read} events', file=sys.stderr)
            print(f'Found {len(chain_terminal_counts)} chainable terminals', file=sys.stderr)

        chosen = []
        # Prefer chain-based terminals when available
        if chain_terminal_counts:
            items = list(chain_terminal_counts.items())
            # Apply selection strategy
            if args.mode == 'rare':
                items.sort(key=lambda x: x[1])
            elif args.mode == 'common':
                items.sort(key=lambda x: x[1], reverse=True)
            else:  # balanced
                items.sort(key=lambda x: x[1])
                mid_start = len(items) // 3
                mid_end = 2 * len(items) // 3
                items = items[mid_start:mid_end] if len(items) > args.top else items
            chosen = [sid for sid, _ in items[:args.top]]

        # Fallback to overlap if chain-based insufficient
        if len(chosen) < args.top:
            overlap = []
            for sid, ec in end_counts.items():
                if sid in start_counts:
                    combined = ec + start_counts[sid]
                    overlap.append((sid, combined, ec, start_counts[sid]))
            if overlap:
                if args.mode == 'rare':
                    overlap.sort(key=lambda x: x[1])
                    chosen.extend([sid for sid, _, _, _ in overlap[:max(0, args.top - len(chosen))]])
                elif args.mode == 'common':
                    overlap.sort(key=lambda x: x[1], reverse=True)
                    chosen.extend([sid for sid, _, _, _ in overlap[:max(0, args.top - len(chosen))]])
                else:
                    max_count = events_read * 0.1 if events_read > 0 else float('inf')
                    filtered = [x for x in overlap if 2 <= x[1] <= max_count]
                    if not filtered:
                        filtered = overlap
                    filtered.sort(key=lambda x: x[1])
                    mid_start = len(filtered) // 3
                    mid_end = 2 * len(filtered) // 3
                    candidates = filtered[mid_start:mid_end] if len(filtered) > args.top else filtered
                    chosen.extend([sid for sid, _, _, _ in candidates[:max(0, args.top - len(chosen))]])

        # Final fallback to end_counts if still insufficient
        chosen = list(dict.fromkeys(chosen))  # dedupe preserve order
        if len(chosen) < args.top:
            additional_needed = args.top - len(chosen)
            chosen_set = set(chosen)
            for sid, _ in end_counts.most_common():
                if sid not in chosen_set:
                    chosen.append(sid)
                    if len(chosen) >= args.top:
                        break

        # Ensure we output something even if we couldn't find enough stations
        if not chosen:
            chosen = ['1']
        chosen = chosen[:args.top]

        print(' '.join(chosen))

        if args.verbose:
            print('\nSelection mode:', args.mode, file=sys.stderr)
            print('Chainable terminals (top 10):', file=sys.stderr)
            for sid, c in chain_terminal_counts.most_common(10):
                print(f'  {sid}: {c}', file=sys.stderr)


if __name__ == '__main__':
    main()
