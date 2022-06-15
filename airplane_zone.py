"""
This module creates a zone, where an airplane (represented by its 
coordinate, altitude, velocity and heading) could have landed
"""

import math
from math import pi


# used to determine the radius of the small (inner) circle
sr = lambda x: max(5, x)
# used to determine the radius og the big (outer) circle
br = lambda x, y: min(40, max(7, 5*(x + 2/y)))
# used to determine the possible angle of the big circle
brd = lambda x, y: min(200, max(45, 250*x - 100/y))


def create_zone(coords_plane: tuple, altitude: float, velocity: float, heading: float) -> tuple:
    """
Creates the zone where an airplane could have landed

Parameters
----------
coords_plane : tuple
    the coordinate of the airplane, in the format (Latitude, 
    Longitude)
altitude : float
    altitude of the airplane, in meters
velocity : float
    velocity of the airplane, in meters/seconds
heading : float
    heading of the airplane, from 0° to 360°, where 0° is full North, 
    and 90° is full East (clockwise)

Returns
-------
tuple
    the coordinates of the zone where the airplane could have landed
"""

    # normalize the values 
    altitude /= 1000
    velocity *= 3.6
    velocity += 1       # so we escape division by zero
    heading = heading if heading >= 0 else heading + 360
    heading = heading if heading < 360 else heading - 360

    # calculate the radius of both circle, and the angle of the big one
    small_radius = sr(altitude)
    big_radius = br(altitude, velocity)
    big_radius_angle = brd(altitude, velocity)

    # calculate the begin angle and the end angle
    # with the heading at the middle of the big radius angle
    angle1 = heading - (big_radius_angle / 2)
    angle2 = heading + (big_radius_angle / 2)

    # get the angle range of the big circle
    all_angle_bc = create_angle_range(int(angle1), int(angle2))

    # get the points of the big circle
    # the last parameter is how much the circle is going to take on a
    # complete circle
    big_circle = points_in_circumference(big_radius, coords_plane, all_angle_bc, big_radius_angle/360)
    big_circle = [item for index, item in enumerate(big_circle) if index%30==0 or index==len(big_circle)-1]

    # get the angle range of the small circle
    # the angle range of the small circle is where the big circle isn't 
    # present, this means if the big circle has x out of y, this means
    # that the small circle has (in terms of possible angles) y-x.
    # That is why we begin from the end of the big circle (angle2), and
    # we end it at his beginning (angle1)
    all_angle_sc = create_angle_range(int(angle2)-360, int(angle1))

    # get the points of the small circle
    # as said before, the last parameters represent how much this 
    # circle covers on a complete circle. 
    # Because the small circle is cut on the angle of the big circle,
    # the last parameter is 1 - x, where x is how much the big circle
    # covers on a complete circle
    small_circle = points_in_circumference(small_radius, coords_plane, all_angle_sc, 1-(big_radius_angle/360))
    small_circle = [item for index, item in enumerate(small_circle) if index%30==0]
    small_circle = small_circle[1:]

    return (tuple(small_circle) + tuple(big_circle))

def points_in_circumference(r: float, coords: tuple, n: list, total: float) -> list:
    """
Creates the circumference points of a circle, from an origin point 
(the airplane coordinates, here coords), the distance (here r) and the
angle (here a list of angle, n)

Parameters
----------
r : float
    The distance between the airplane coordinates and the circumference
of the circle
coords : tuple
    The coordinate of the airplane, in the form (Latitude, Longitude)
n : list
    List of angle (with as maximum 360 and minimum 0)
total : float
    Percentage taken by the final circle on a complete circle. If this
paramaters would be equal to 1 (or won't exist), this function will
always create complete circle

Returns
-------
list
    List of coordinates of the circle
    """
    # get the distance by latitude and longitude
    rlat = abs(km_to_lat_long(r, True))
    rlon = abs(km_to_lat_long(r, False, rlat))

    new_coords = [(math.sin(2*pi/len(n)*x*total)*rlon*150,
                    math.cos(2*pi/len(n)*x*total)*rlat*150)
                    for x in n]
    new_coords = [(km_to_lat_long(item[1], False, 
                        km_to_lat_long(item[0], True)) + coords[0],
                    km_to_lat_long(item[0], True) + coords[1])
                    for item in new_coords]

    return new_coords

def create_angle_range(begin_angle: int, end_angle: int) -> list:
    """
Creates the range from begin_angle to end_angle, with 
360 being the maximum possible, and 0 the minimum possibe.
If the value is greater than 360, it will be decreased by 360.
If the value is lower than 0, it will increased by 360.

Parameters
----------
begin_angle : int
    angle which the range begins
end_angle : int
    angle which the range ends

Returns
-------
list
    list of the range, from begin_angle to end_angle
    """
    angle_range = [x for x in range(begin_angle, end_angle)]
    # increase if the value is lower than 0
    angle_range = [x if x >= 0 else x + 360 for x in angle_range]
    # decrease if the value is greater than 360
    angle_range = [x if x < 360 else x - 360 for x in angle_range]

    return angle_range

def km_to_lat_long(val: float, is_lat: bool, lat=None) -> float:
    """
Convert the given distance into latitude and longitude

Parameters
----------
val : float
    The initial distance (in kilometer)
is_lat : bool
    True if the conversion has to be made to latitude, else it will
be converted to longitude
lat : float, optional
    If is_lat is False, this parameters has to be the latitude 

Returns
-------
float 
    Distance by latitude/longitude
    """
    if is_lat:
        return val / 110.574
    else:
        return val / (111.32 * math.cos(math.radians(lat/ 110.574)))

