#!/bin/bash

exec python3 get_airplane_flightaware.py &
exec python3 check_airplanes.py &
exec python3 get_airport_by_zone.py &
exec python3 get_ppr.py &
exec python3 get_aftn_by_id.py &
exec python3 get_ppr.py