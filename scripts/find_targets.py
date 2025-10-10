#!/usr/bin/env python3
import sys
import csv
import argparse
from collections import Counter
from pathlib import Path

START_KEYS = ['start station id', 'start_station_id', 'start_station_id ']
END_KEYS = ['end station id', 'end_station_id', 'end_station_id ']

SELECTIVITY_MODES = ['balanced', 'rare', 'common']


def detect_col(headers, candidates):
    hmap = {h.strip().lower(): i for i, h in enumerate(headers)}
    for c in candidates:
        if c in hmap:
            return c, hmap[c]
    for name, idx in hmap.items():
        if 'station' in name and 'id' in name and ('start' in name or 'end' in name):
            return name, idx
    return None, None


def main():
    ap = argparse.ArgumentParser(
        description='Find target station IDs for CitiBike CSV',
        epilog='Example: python3 find_targets.py data.csv --max-events 20 --mode rare --top 3'
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
        if start_idx is None or end_idx is None:
            print('Error: could not detect start/end station id columns', file=sys.stderr)
            if args.verbose:
                print('Headers:', headers, file=sys.stderr)
            sys.exit(2)

        start_counts = Counter()
        end_counts = Counter()
        events_read = 0

        for row in rdr:
            if len(row) != len(headers):
                continue
            
            # Respect max_events limit
            if args.max_events > 0 and events_read >= args.max_events:
                break
            events_read += 1
            
            sv = row[start_idx].strip()
            ev = row[end_idx].strip()
            if sv:
                start_counts[sv] += 1
            if ev:
                end_counts[ev] += 1
        
        if args.verbose:
            print(f'Read {events_read} events', file=sys.stderr)

        # Build overlap list (stations appearing as both start and end)
        overlap = []
        for sid, ec in end_counts.items():
            if sid in start_counts:
                combined = ec + start_counts[sid]
                overlap.append((sid, combined, ec, start_counts[sid]))
        
        if not overlap:
            # No chainable stations, fall back to most common end stations
            chosen = [sid for sid, _ in end_counts.most_common(args.top)]
        else:
            # Apply selection strategy
            if args.mode == 'rare':
                # Rare: Prefer stations with LOW frequency (more selective patterns)
                # These will generate fewer matches, better for testing with Kleene closure
                overlap.sort(key=lambda x: x[1])  # Sort ascending (rarest first)
                chosen = [sid for sid, _, _, _ in overlap[:args.top]]
            elif args.mode == 'common':
                # Common: Prefer stations with HIGH frequency (more matches)
                overlap.sort(key=lambda x: x[1], reverse=True)  # Sort descending
                chosen = [sid for sid, _, _, _ in overlap[:args.top]]
            else:  # balanced (default)
                # Balanced: Prefer medium frequency (appears in both but not too common)
                # Filter out extremes (< 2 or > 10% of events)
                max_count = events_read * 0.1 if events_read > 0 else float('inf')
                filtered = [x for x in overlap if 2 <= x[1] <= max_count]
                if not filtered:
                    filtered = overlap  # Fall back if filter too strict
                filtered.sort(key=lambda x: x[1])  # Sort by frequency
                # Pick from middle range
                mid_start = len(filtered) // 3
                mid_end = 2 * len(filtered) // 3
                candidates = filtered[mid_start:mid_end] if len(filtered) > args.top else filtered
                chosen = [sid for sid, _, _, _ in candidates[:args.top]]

        print(' '.join(chosen))

        if args.verbose:
            print('\nDetected columns:', file=sys.stderr)
            print(f'  start: {start_key}', file=sys.stderr)
            print(f'  end:   {end_key}', file=sys.stderr)
            print(f'\nSelection mode: {args.mode}', file=sys.stderr)
            print(f'Total stations with overlap: {len(overlap)}', file=sys.stderr)
            
            if overlap:
                print('\nChosen stations (sid, combined_count, end_count, start_count):', file=sys.stderr)
                for sid in chosen:
                    matches = [x for x in overlap if x[0] == sid]
                    if matches:
                        sid_val, comb, ec, sc = matches[0]
                        selectivity = f'{(comb/events_read)*100:.1f}%' if events_read > 0 else 'N/A'
                        print(f'  {sid_val}: {comb} total ({selectivity} of events), {ec} ends, {sc} starts', file=sys.stderr)
                
                if args.mode == 'rare':
                    print('\n[RARE mode: These stations appear infrequently = more selective patterns]', file=sys.stderr)
                elif args.mode == 'common':
                    print('\n[COMMON mode: These stations appear frequently = more matches]', file=sys.stderr)
                else:
                    print('\n[BALANCED mode: Medium frequency stations]', file=sys.stderr)
            else:
                print('\nNo chainable stations found. Using top end stations:', file=sys.stderr)
                for sid, c in end_counts.most_common(10):
                    print(f'  {sid}: {c}', file=sys.stderr)


if __name__ == '__main__':
    main()
