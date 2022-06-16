#!/bin/bash

exec python3 -u get_airplane_flightaware.py &
exec python3 -u check_airplanes.py &
exec python3 -u get_airport_by_zone.py &
exec python3 -u get_ppr.py &
exec python3 -u get_aftn_by_id.py &
exec python3 -u etdex_ponderation.py