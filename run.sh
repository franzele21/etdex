#!/bin/bash

exec python3 -u call_fly_tracker.py &
sleep 15 &
exec python3 -u get_ppr.py &
exec python3 -u get_aftn_by_id.py &
sleep 10 &
exec python3 -u etdex_ponderation.py
